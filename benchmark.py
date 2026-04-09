#!/usr/bin/env python3
"""Unified TPC-H + TPC-DS concurrency-scale benchmark.

For each concurrency level (doubling: 1, 2, 4, 8, …), finds the maximum data
scale factor at which the system under test still matches real-world (fleet)
runtime distributions.

Usage examples:
    # Quick sanity test on DuckDB
    python benchmark.py --engine duckdb --scale-factors 1 --concurrency-start 1 --concurrency-end 1

    # Full benchmark against PostgreSQL
    python benchmark.py --engine postgresql --host 127.0.0.1 --port 5432 \
        --user myuser --password mypassword --database mydb

    # Resume from a specific point
    python benchmark.py --engine postgresql --resume-from-concurrency 8 --resume-from-sf 50
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Paths ──────────────────────────────────────────────────────────────────────
BENCH_ROOT = Path(__file__).resolve().parent
TPCH_ROOT = BENCH_ROOT / "TPC-H"
TPCDS_ROOT = BENCH_ROOT / "TPC-DS"

log = logging.getLogger("benchmark")

# ── Fleet target distribution ──────────────────────────────────────────────────
# Each entry: (bucket_label, lower_bound_sec, upper_bound_sec, pct_queries, pct_runtime)
FLEET_BUCKETS = [
    ("(0s, 10ms]",    0.0,       0.01,    13.7,   0.01),
    ("(10ms, 100ms]", 0.01,      0.1,     48.3,   0.4),
    ("(100ms, 1s]",   0.1,       1.0,     24.9,   2.3),
    ("(1s, 10s]",     1.0,       10.0,     9.9,   7.3),
    ("(10s, 1min]",   10.0,      60.0,     2.2,  13.3),
    ("(1min, 10min]", 60.0,     600.0,     0.86, 35.7),
    ("(10min, 1h]",  600.0,    3600.0,     0.08, 25.2),
    ("(1h, 10h]",   3600.0,   36000.0,    0.008, 14.3),
    (">=10h",       36000.0,   float("inf"), 9e-5, 1.6),
]

# Cumulative tail thresholds (seconds) and fleet % of queries above that threshold.
# "Bottleneck" = observed tail % > fleet tail % at any of these thresholds.
FLEET_TAIL_THRESHOLDS: list[tuple[str, float, float]] = []

def _build_tail_thresholds() -> None:
    """Pre-compute cumulative tail percentages from the fleet buckets."""
    total_pct = sum(b[3] for b in FLEET_BUCKETS)
    # We check tails starting at 1 s upward.
    for label, lower, _upper, _pct_q, _pct_r in FLEET_BUCKETS:
        if lower < 1.0:
            continue
        # Fleet % of queries with latency > lower
        tail_pct = sum(b[3] for b in FLEET_BUCKETS if b[1] >= lower)
        FLEET_TAIL_THRESHOLDS.append((label, lower, tail_pct))

_build_tail_thresholds()

# ── Bottleneck detection ───────────────────────────────────────────────────────

def bucket_latencies(latencies: list[float]) -> dict[str, Any]:
    """Categorise latencies into fleet time buckets.  Returns counts, pcts."""
    n = len(latencies)
    if n == 0:
        return {"total": 0, "buckets": {}}
    counts: dict[str, int] = {b[0]: 0 for b in FLEET_BUCKETS}
    for lat in latencies:
        for label, lo, hi, *_ in FLEET_BUCKETS:
            if lo < lat <= hi or (label == ">=10h" and lat > 36000.0):
                counts[label] += 1
                break
        else:
            # latency == 0 exactly → put in first bucket
            counts[FLEET_BUCKETS[0][0]] += 1
    buckets = {}
    for label, lo, hi, fleet_pct, fleet_rt_pct in FLEET_BUCKETS:
        cnt = counts[label]
        buckets[label] = {
            "count": cnt,
            "pct": round(100.0 * cnt / n, 4),
            "fleet_pct": fleet_pct,
        }
    return {"total": n, "buckets": buckets}


def is_bottleneck(latencies: list[float]) -> tuple[bool, dict[str, Any]]:
    """Return (True, comparison_dict) if the SUT exceeds fleet tail distribution."""
    n = len(latencies)
    if n == 0:
        return True, {}
    comparison: dict[str, Any] = {}
    bottleneck = False
    for label, threshold_sec, fleet_tail_pct in FLEET_TAIL_THRESHOLDS:
        observed_above = sum(1 for lat in latencies if lat > threshold_sec)
        observed_pct = 100.0 * observed_above / n
        passed = observed_pct <= fleet_tail_pct
        comparison[f">{threshold_sec}s"] = {
            "observed_pct": round(observed_pct, 4),
            "fleet_pct": fleet_tail_pct,
            "pass": passed,
        }
        if not passed:
            bottleneck = True
    return bottleneck, comparison


# ── Subprocess helpers ─────────────────────────────────────────────────────────

def _run(cmd: list[str], label: str, extra_env: dict[str, str] | None = None) -> None:
    """Run a shell command, streaming output to log."""
    log.info("  [%s] %s", label, " ".join(cmd))
    env = None
    if extra_env:
        env = {**os.environ, **extra_env}
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.debug("    %s", line)
    if result.returncode != 0:
        log.error("  [%s] FAILED (rc=%d)", label, result.returncode)
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log.error("    %s", line)
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")


def generate_data(sf: int) -> None:
    """Generate TPC-H and TPC-DS data at *sf* (idempotent)."""
    log.info("Generating data for SF=%d …", sf)
    _run([str(TPCH_ROOT / "run.sh"), "generate-data", "--scale", str(sf)], "tpch-datagen")
    _run([str(TPCDS_ROOT / "run.sh"), "generate-data", "--scale", str(sf)], "tpcds-datagen")


def generate_queries(engine: str, sf: int, tpch_stream: int, tpcds_stream: int) -> None:
    """Generate TPC-H and TPC-DS queries for *engine* (idempotent)."""
    log.info("Generating queries for engine=%s SF=%d …", engine, sf)
    _run(
        [str(TPCH_ROOT / "run.sh"), "generate-queries", "--engine", engine, "--stream", str(tpch_stream)],
        "tpch-querygen",
    )
    _run(
        [str(TPCDS_ROOT / "run.sh"), "generate-queries", "--engine", engine,
         "--scale", str(sf), "--stream", str(tpcds_stream)],
        "tpcds-querygen",
    )


def _isolate_customer_table(config: EngineConfig, target_schema: str) -> None:
    """Move public.customer → *target_schema*.customer to avoid TPC-H/TPC-DS collision."""
    psycopg = _require("psycopg", "pip install psycopg[binary]")
    conninfo = (
        f"host={config.host} port={config.port} "
        f"dbname={config.database} user={config.user} password={config.password}"
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
            cur.execute(f"DROP TABLE IF EXISTS {target_schema}.customer CASCADE")
            if config.engine == "cedardb":
                # CedarDB doesn't support ALTER TABLE SET SCHEMA
                cur.execute(f"CREATE TABLE {target_schema}.customer AS SELECT * FROM public.customer")
                cur.execute("DROP TABLE IF EXISTS public.customer CASCADE")
            else:
                cur.execute(f"ALTER TABLE public.customer SET SCHEMA {target_schema}")
    log.info("  Moved public.customer → %s.customer", target_schema)


def _cleanup_isolation_schemas(config: EngineConfig) -> None:
    """Drop tpch/tpcds schemas from a previous run so DDL re-runs cleanly."""
    psycopg = _require("psycopg", "pip install psycopg[binary]")
    conninfo = (
        f"host={config.host} port={config.port} "
        f"dbname={config.database} user={config.user} password={config.password}"
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            for schema in ("tpch", "tpcds"):
                # CedarDB doesn't support DROP SCHEMA CASCADE with dependent objects;
                # drop all tables in the schema explicitly first.
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                    (schema,),
                )
                for (tbl,) in cur.fetchall():
                    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{tbl}" CASCADE')
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    log.info("  Cleaned up isolation schemas (tpch, tpcds)")


def _connection_env(config: EngineConfig) -> dict[str, str]:
    """Map EngineConfig to the env vars expected by TPC-H/TPC-DS load scripts."""
    host, port = config.host, str(config.port)
    user, password, database = config.user, config.password, config.database
    if config.engine == "postgresql":
        return {
            # TPC-H
            "TPCH_PGHOST": host, "TPCH_PGPORT": port,
            "TPCH_PGUSER": user, "TPCH_PGPASSWORD": password,
            "TPCH_PGDATABASE": database,
            # TPC-DS
            "TPCDS_PGHOST": host, "TPCDS_PGPORT": port,
            "TPCDS_PGUSER": user, "TPCDS_PGPASSWORD": password,
            "TPCDS_PGDATABASE": database,
        }
    if config.engine == "cedardb":
        return {
            # TPC-H
            "TPCH_CEDAR_HOST": host, "TPCH_CEDAR_PORT": port,
            "TPCH_CEDAR_USER": user, "TPCH_CEDAR_PASSWORD": password,
            "TPCH_CEDAR_DB": database,
            # TPC-DS (uses different var names)
            "CEDAR_HOST": host, "CEDAR_PORT": port,
            "CEDAR_USER": user, "CEDAR_PASS": password,
            "CEDAR_DB": database,
        }
    if config.engine == "starrocks":
        return {
            # TPC-H
            "TPCH_STARROCKS_HOST": host, "TPCH_STARROCKS_PORT": port,
            "TPCH_STARROCKS_USER": user, "TPCH_STARROCKS_PASSWORD": password,
            "TPCH_STARROCKS_HTTP_HOST": host,
            # TPC-DS
            "TPCDS_STARROCKS_HOST": host, "TPCDS_STARROCKS_PORT": port,
            "TPCDS_STARROCKS_USER": user, "TPCDS_STARROCKS_PASSWORD": password,
            "TPCDS_STARROCKS_HTTP_HOST": host,
        }
    return {}


def load_all_data(config: EngineConfig, sf: int) -> None:
    """Load both TPC-H and TPC-DS data into the SUT."""
    engine = config.engine
    log.info("Loading data for engine=%s SF=%d …", engine, sf)
    conn_env = _connection_env(config)
    # Clean up isolation schemas from any previous run so DDL can drop/recreate tables.
    if engine in ("postgresql", "cedardb"):
        _cleanup_isolation_schemas(config)
    _run(
        [str(TPCH_ROOT / "run.sh"), "load", "--engine", engine, "--scale", str(sf)],
        "tpch-load", extra_env=conn_env,
    )
    # Move TPC-H customer to a separate schema before TPC-DS prepare
    # overwrites it (both benchmarks have a 'customer' table with different columns).
    if engine in ("postgresql", "cedardb"):
        _isolate_customer_table(config, "tpch")
    _run(
        [str(TPCDS_ROOT / "run.sh"), "prepare", "--engine", engine, "--scale", str(sf)],
        "tpcds-prepare", extra_env=conn_env,
    )
    _run(
        [str(TPCDS_ROOT / "run.sh"), "load", "--engine", engine, "--scale", str(sf)],
        "tpcds-load", extra_env=conn_env,
    )
    # Move TPC-DS customer to its own schema.
    if engine in ("postgresql", "cedardb"):
        _isolate_customer_table(config, "tpcds")

# ── Query loading ──────────────────────────────────────────────────────────────

def _split_statements(sql_text: str) -> list[str]:
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def load_tpch_queries(engine: str, stream: int) -> list[tuple[str, str]]:
    """Return [(query_id, sql_text), …] for TPC-H queries 1-22."""
    qdir = TPCH_ROOT / "queries" / engine / str(stream)
    if not qdir.exists():
        raise FileNotFoundError(f"TPC-H query dir missing: {qdir}")
    queries = []
    for i in range(1, 23):
        qpath = qdir / f"{i}.sql"
        if not qpath.exists():
            raise FileNotFoundError(f"Missing TPC-H query file: {qpath}")
        queries.append((f"tpch-q{i}", qpath.read_text(encoding="utf-8")))
    return queries


def load_tpcds_queries(engine: str, sf: int, stream: int) -> list[tuple[str, str]]:
    """Return [(query_id, sql_text), …] for TPC-DS queries 1-99."""
    qdir = TPCDS_ROOT / "queries" / engine / f"sf{sf}" / f"stream{stream}"
    if not qdir.exists():
        raise FileNotFoundError(f"TPC-DS query dir missing: {qdir}")
    queries = []
    for i in range(1, 100):
        qpath = qdir / f"query{i}.sql"
        if not qpath.exists():
            raise FileNotFoundError(f"Missing TPC-DS query file: {qpath}")
        queries.append((f"tpcds-q{i}", qpath.read_text(encoding="utf-8")))
    return queries


def load_all_queries(engine: str, sf: int, tpch_stream: int, tpcds_stream: int) -> list[tuple[str, str]]:
    """Load and combine all 121 queries (22 TPC-H + 99 TPC-DS)."""
    tpch = load_tpch_queries(engine, tpch_stream)
    tpcds = load_tpcds_queries(engine, sf, tpcds_stream)
    return tpch + tpcds

# ── Engine connection & query execution ────────────────────────────────────────
# We import DB drivers lazily to avoid hard dependencies.

def _require(module_name: str, install_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(f"Missing '{module_name}'. Install: {install_hint}") from exc


# TPC-DS normalize_sql is imported lazily to avoid path hacking at module level.
_normalize_sql = None

def _get_normalize_sql():
    global _normalize_sql
    if _normalize_sql is None:
        sys.path.insert(0, str(TPCDS_ROOT / "tools"))
        from pipeline_common import normalize_sql  # type: ignore[import-untyped]
        _normalize_sql = normalize_sql
    return _normalize_sql


@dataclass
class EngineConfig:
    engine: str
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "myuser"
    password: str = "mypassword"
    database: str = "mydb"
    duckdb_threads: int = 4
    query_timeout: int = 3600


def _execute_single_query_duckdb(
    config: EngineConfig, sf: int, query_id: str, sql_text: str,
) -> dict[str, Any]:
    """Execute one query on DuckDB (new connection per query).

    Uses an in-memory connection that attaches the TPC-H and TPC-DS database
    files as read-only.  The in-memory database itself is writable so that
    queries using CREATE TEMPORARY TABLE / WITH … AS succeed.
    """
    duckdb = _require("duckdb", "pip install duckdb")
    normalize = _get_normalize_sql()

    tpch_db = TPCH_ROOT / "duckdb" / f"tpch_sf{sf}.duckdb"
    tpcds_db = TPCDS_ROOT / "logs" / "duckdb" / f"sf{sf}" / f"tpcds_sf{sf}.duckdb"

    # In-memory main DB (writable for temp tables / CREATE AS) with data DB
    # attached read-only.  We create lightweight views in the memory catalog so
    # that unqualified table names resolve correctly while CREATE TABLE still
    # targets the writable in-memory database.
    conn = duckdb.connect(":memory:")
    if query_id.startswith("tpch-"):
        conn.execute(f"ATTACH '{tpch_db}' AS tpch_data (READ_ONLY)")
        alias = "tpch_data"
    else:
        conn.execute(f"ATTACH '{tpcds_db}' AS tpcds_data (READ_ONLY)")
        alias = "tpcds_data"
    for (tname,) in conn.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog='{alias}' AND table_schema='main'"
    ).fetchall():
        conn.execute(f'CREATE VIEW main."{tname}" AS SELECT * FROM {alias}.main."{tname}"')

    conn.execute(f"PRAGMA threads={config.duckdb_threads}")

    normalized = normalize("duckdb", sql_text) if query_id.startswith("tpcds-") else sql_text
    start = time.perf_counter()
    try:
        row_count = 0
        for stmt in _split_statements(normalized):
            result = conn.execute(stmt)
            if result.description is not None:
                row_count = len(result.fetchall())
        elapsed = time.perf_counter() - start
        return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                "status": "ok", "row_count": row_count}
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                "status": "error", "error": str(exc)}
    finally:
        conn.close()


def _execute_single_query_psycopg(
    config: EngineConfig, query_id: str, sql_text: str, engine_label: str,
) -> dict[str, Any]:
    """Execute one query via psycopg (PostgreSQL / CedarDB)."""
    psycopg = _require("psycopg", "pip install psycopg[binary]")
    normalize = _get_normalize_sql()
    conninfo = (
        f"host={config.host} port={config.port} "
        f"dbname={config.database} user={config.user} password={config.password}"
    )
    normalized = normalize(engine_label, sql_text) if query_id.startswith("tpcds-") else sql_text
    # Prevent concurrent CREATE VIEW conflicts (TPC-H Q15 revenue0).
    normalized = normalized.replace("create view revenue0", "create or replace view revenue0")

    # CedarDB may hit transient serialization conflicts under concurrency; retry.
    max_attempts = 3 if engine_label == "cedardb" else 1
    overall_start = time.perf_counter()

    for attempt in range(max_attempts):
        conn = psycopg.connect(conninfo, autocommit=True)
        if engine_label in ("postgresql", "cedardb") and config.query_timeout < 3600:
            conn.execute(f"SET statement_timeout = '{config.query_timeout * 1000}'")
        # Route to correct schema so the right `customer` table is visible.
        if engine_label in ("postgresql", "cedardb"):
            schema = "tpch" if query_id.startswith("tpch-") else "tpcds"
            conn.execute(f"SET search_path TO {schema}, public")
        start = time.perf_counter()
        try:
            row_count = 0
            with conn.cursor() as cur:
                if engine_label == "postgresql" or engine_label == "cedardb":
                    # PostgreSQL: run multi-statement as one script for TPC-DS
                    if engine_label == "postgresql" and query_id.startswith("tpcds-"):
                        cur.execute(normalized)
                        while True:
                            if cur.description is not None:
                                row_count = len(cur.fetchall())
                            if not cur.nextset():
                                break
                    else:
                        for stmt in _split_statements(normalized):
                            cur.execute(stmt)
                            if cur.description is not None:
                                row_count = len(cur.fetchall())
            elapsed = time.perf_counter() - overall_start
            return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                    "status": "ok", "row_count": row_count}
        except Exception as exc:
            elapsed = time.perf_counter() - overall_start
            # Retry transient CedarDB concurrency conflicts.
            if (attempt < max_attempts - 1
                    and "concurrent" in str(exc).lower()):
                log.debug("  CedarDB retry %d/%d for %s: %s",
                          attempt + 1, max_attempts, query_id, exc)
                continue
            return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                    "status": "error", "error": str(exc)}
        finally:
            conn.close()
    # Unreachable, but satisfy the type checker.
    elapsed = time.perf_counter() - overall_start
    return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
            "status": "error", "error": "max retries exceeded"}


def _execute_single_query_starrocks(
    config: EngineConfig, query_id: str, sql_text: str,
) -> dict[str, Any]:
    """Execute one query via pymysql (StarRocks)."""
    pymysql = _require("pymysql", "pip install pymysql")
    normalize = _get_normalize_sql()
    normalized = normalize("starrocks", sql_text) if query_id.startswith("tpcds-") else sql_text

    # Prevent concurrent CREATE VIEW conflicts (TPC-H Q15 revenue0).
    normalized = normalized.replace("create view revenue0", "create or replace view revenue0")

    conn = pymysql.connect(
        host=config.host, port=config.port,
        user=config.user, password=config.password,
        database=config.database, autocommit=True,
        read_timeout=config.query_timeout, write_timeout=config.query_timeout,
    )
    start = time.perf_counter()
    try:
        row_count = 0
        with conn.cursor() as cur:
            cur.execute(f"SET query_timeout = {config.query_timeout}")
            # Switch to the correct database for TPC-H vs TPC-DS queries.
            db = "tpcds" if query_id.startswith("tpcds-") else "tpch"
            cur.execute(f"USE {db}")
            for stmt in _split_statements(normalized):
                cur.execute(stmt)
                if cur.description:
                    row_count = len(list(cur.fetchall()))
        elapsed = time.perf_counter() - start
        return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                "status": "ok", "row_count": row_count}
    except Exception as exc:
        elapsed = time.perf_counter() - start
        return {"query_id": query_id, "elapsed_sec": round(elapsed, 6),
                "status": "error", "error": str(exc)}
    finally:
        conn.close()


def execute_query(
    config: EngineConfig, sf: int, query_id: str, sql_text: str,
) -> dict[str, Any]:
    """Dispatch a single query to the appropriate engine executor."""
    if config.engine == "duckdb":
        return _execute_single_query_duckdb(config, sf, query_id, sql_text)
    elif config.engine in ("postgresql", "cedardb"):
        return _execute_single_query_psycopg(config, query_id, sql_text, config.engine)
    elif config.engine == "starrocks":
        return _execute_single_query_starrocks(config, query_id, sql_text)
    else:
        raise ValueError(f"Unsupported engine: {config.engine}")


# ── Concurrent execution ──────────────────────────────────────────────────────

def run_concurrent(
    config: EngineConfig,
    sf: int,
    concurrency: int,
    queries: list[tuple[str, str]],
    seed: int,
    query_timeout: int,
) -> list[dict[str, Any]]:
    """Run *concurrency* users each executing all 121 queries in random order.

    Each user creates a new connection per query.  All users start together
    via a threading barrier.
    """
    barrier = threading.Barrier(concurrency, timeout=120)
    all_results: list[dict[str, Any]] = []
    lock = threading.Lock()

    def user_worker(user_id: int) -> list[dict[str, Any]]:
        rng = random.Random(seed + user_id)
        shuffled = list(queries)
        rng.shuffle(shuffled)

        # Wait for all user threads to be ready before starting.
        barrier.wait()

        results: list[dict[str, Any]] = []
        for query_id, sql_text in shuffled:
            result = execute_query(config, sf, query_id, sql_text)
            result["user_id"] = user_id
            results.append(result)
        return results

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(user_worker, uid): uid for uid in range(concurrency)}
        for future in as_completed(futures):
            uid = futures[future]
            try:
                user_results = future.result()
                with lock:
                    all_results.extend(user_results)
            except Exception as exc:
                log.error("User %d failed: %s", uid, exc)
                # Record a failure for every query for this user.
                with lock:
                    for query_id, _ in queries:
                        all_results.append({
                            "user_id": uid,
                            "query_id": query_id,
                            "elapsed_sec": 0.0,
                            "status": "error",
                            "error": str(exc),
                        })

    return all_results


# ── Main benchmark loop ───────────────────────────────────────────────────────

def build_concurrency_sequence(start: int, end: int) -> list[int]:
    """Return powers-of-2 from *start* to *end*, inclusive."""
    levels = []
    c = start
    while c <= end:
        levels.append(c)
        c *= 2
    return levels


DEFAULT_SCALE_FACTORS = [1, 5, 10, 20, 50, 100, 500, 1000, 3000, 5000]


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    config = EngineConfig(
        engine=args.engine,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        duckdb_threads=args.duckdb_threads,
        query_timeout=args.query_timeout,
    )

    scale_factors = [int(s) for s in args.scale_factors.split(",")]
    concurrency_levels = build_concurrency_sequence(args.concurrency_start, args.concurrency_end)

    # Apply resume filters.
    if args.resume_from_concurrency > 0:
        concurrency_levels = [c for c in concurrency_levels if c >= args.resume_from_concurrency]
    if not concurrency_levels:
        log.error("No concurrency levels to run after applying resume filter.")
        return {}

    output: dict[str, Any] = {
        "engine": config.engine,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "seed": args.seed,
        "config": {
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "scale_factors": scale_factors,
            "concurrency_levels": concurrency_levels,
            "query_timeout": config.query_timeout,
            "duckdb_threads": config.duckdb_threads,
        },
        "results": [],
    }

    for concurrency in concurrency_levels:
        log.info("═══ Concurrency Level: %d ═══", concurrency)
        max_sf: int | None = None
        runs: list[dict[str, Any]] = []

        effective_sfs = scale_factors
        if args.resume_from_sf > 0 and concurrency == concurrency_levels[0]:
            effective_sfs = [s for s in scale_factors if s >= args.resume_from_sf]

        for sf in effective_sfs:
            log.info("  ── SF=%d, concurrency=%d ──", sf, concurrency)

            # 1. Generate data & queries (idempotent / cached).
            try:
                generate_data(sf)
                generate_queries(config.engine, sf, args.tpch_stream, args.tpcds_stream)
            except RuntimeError as exc:
                log.error("Data/query generation failed at SF=%d: %s", sf, exc)
                runs.append({
                    "scale_factor": sf,
                    "bottleneck": True,
                    "error": f"generation failed: {exc}",
                })
                break

            # 2. Load data into SUT.
            try:
                load_all_data(config, sf)
            except RuntimeError as exc:
                log.error("Data loading failed at SF=%d: %s", sf, exc)
                runs.append({
                    "scale_factor": sf,
                    "bottleneck": True,
                    "error": f"load failed: {exc}",
                })
                break

            # 3. Load query SQL texts.
            try:
                queries = load_all_queries(config.engine, sf, args.tpch_stream, args.tpcds_stream)
            except FileNotFoundError as exc:
                log.error("Query loading failed at SF=%d: %s", sf, exc)
                runs.append({
                    "scale_factor": sf,
                    "bottleneck": True,
                    "error": f"query files missing: {exc}",
                })
                break

            log.info("  Running %d users × %d queries …", concurrency, len(queries))

            # 4. Execute concurrently.
            results = run_concurrent(config, sf, concurrency, queries, args.seed, args.query_timeout)

            # 5. Analyse.
            latencies = [r["elapsed_sec"] for r in results]
            distribution = bucket_latencies(latencies)
            bottleneck, comparison = is_bottleneck(latencies)

            error_count = sum(1 for r in results if r["status"] == "error")
            if error_count == len(results):
                bottleneck = True  # 100% error rate → treat as bottleneck

            run_record: dict[str, Any] = {
                "scale_factor": sf,
                "concurrency": concurrency,
                "total_queries": len(results),
                "error_count": error_count,
                "bottleneck": bottleneck,
                "bucket_distribution": distribution["buckets"],
                "fleet_comparison": comparison,
                "per_query": results,
            }
            runs.append(run_record)

            status = "BOTTLENECK" if bottleneck else "OK"
            log.info("  SF=%d → %s  (errors=%d, queries=%d)", sf, status, error_count, len(results))
            for thr_label, thr_data in comparison.items():
                flag = "✓" if thr_data["pass"] else "✗"
                log.info("    %s %s observed=%.2f%% fleet=%.2f%%",
                         flag, thr_label, thr_data["observed_pct"], thr_data["fleet_pct"])

            if bottleneck:
                break
            max_sf = sf

        concurrency_record = {
            "concurrency": concurrency,
            "max_scale_factor": max_sf,
            "runs": runs,
        }
        output["results"].append(concurrency_record)

        # Write intermediate results after each concurrency level.
        _write_output(args.output, output)

        if max_sf is None:
            log.info("SUT cannot meet fleet targets at SF=1 for concurrency=%d. Stopping.", concurrency)
            break
        else:
            log.info("Concurrency %d → max SF = %d", concurrency, max_sf)

    return output


def _write_output(path: str, data: dict[str, Any]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")
    log.info("Results written to %s", out)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified TPC-H/TPC-DS concurrency-scale benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--engine", required=True,
                   choices=["duckdb", "postgresql", "cedardb", "starrocks"],
                   help="Database engine to benchmark")

    # Connection.
    p.add_argument("--host", default="127.0.0.1", help="DB host (ignored for duckdb)")
    p.add_argument("--port", type=int, default=0,
                   help="DB port (default: engine-specific)")
    p.add_argument("--user", default="", help="DB user")
    p.add_argument("--password", default="", help="DB password")
    p.add_argument("--database", default="", help="DB name")

    # Scale & concurrency.
    p.add_argument("--scale-factors", default=",".join(str(s) for s in DEFAULT_SCALE_FACTORS),
                   help="Comma-separated scale factors (default: %(default)s)")
    p.add_argument("--concurrency-start", type=int, default=1,
                   help="Starting concurrency level (default: 1)")
    p.add_argument("--concurrency-end", type=int, default=512,
                   help="Max concurrency level (default: 512)")

    # Resume.
    p.add_argument("--resume-from-concurrency", type=int, default=0,
                   help="Skip concurrency levels below this value")
    p.add_argument("--resume-from-sf", type=int, default=0,
                   help="At first concurrency level, skip SFs below this value")

    # Streams & seeds.
    p.add_argument("--tpch-stream", type=int, default=1, help="TPC-H query stream (default: 1)")
    p.add_argument("--tpcds-stream", type=int, default=1, help="TPC-DS query stream (default: 1)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for query shuffle (default: 42)")

    # Engine-specific.
    p.add_argument("--duckdb-threads", type=int, default=4, help="DuckDB thread count (default: 4)")
    p.add_argument("--query-timeout", type=int, default=3600,
                   help="Per-query timeout in seconds (default: 3600)")

    # Output.
    p.add_argument("--output", default="benchmark_results.json",
                   help="Path for JSON results (default: benchmark_results.json)")

    p.add_argument("-v", "--verbose", action="store_true", help="Debug-level logging")

    args = p.parse_args(argv)

    # Apply engine-specific defaults.
    defaults: dict[str, dict[str, Any]] = {
        "duckdb":     {"port": 0,    "user": "",       "password": "",         "database": ""},
        "postgresql": {"port": 5432, "user": "myuser", "password": "mypassword", "database": "mydb"},
        "cedardb":    {"port": 5433, "user": "admin",  "password": "admin",    "database": "db"},
        "starrocks":  {"port": 9030, "user": "root",   "password": "",         "database": "tpch"},
    }
    d = defaults[args.engine]
    if args.port == 0:
        args.port = d["port"]
    if not args.user:
        args.user = d["user"]
    if not args.password:
        args.password = d["password"]
    if not args.database:
        args.database = d["database"]

    return args


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("Unified benchmark: engine=%s, SFs=%s, concurrency=%d→%d",
             args.engine, args.scale_factors,
             args.concurrency_start, args.concurrency_end)

    result = run_benchmark(args)

    # Print summary table.
    if result.get("results"):
        log.info("─── Summary ───")
        for entry in result["results"]:
            c = entry["concurrency"]
            ms = entry["max_scale_factor"]
            log.info("  concurrency=%4d  max_sf=%s", c, ms if ms is not None else "NONE (bottleneck at SF=1)")


if __name__ == "__main__":
    main()
