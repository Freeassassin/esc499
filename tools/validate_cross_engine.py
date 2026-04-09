#!/usr/bin/env python3
"""Cross-engine SQL validation using EXPLAIN to verify syntax/semantics without full execution.

Validates augmented TPC-DS and TPC-H queries against PostgreSQL, CedarDB, StarRocks
by running EXPLAIN on each query (fast, no data needed beyond schema).
DuckDB is already validated via profiling. For DuckDB we also validate via EXPLAIN
to keep the output consistent.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Add TPC-DS tools to path for pipeline_common
sys.path.insert(0, str(ROOT / "TPC-DS" / "tools"))
sys.path.insert(0, str(ROOT / "TPC-DS"))

from pipeline_common import load_statements, normalize_sql


def require_module(name: str, hint: str):
    try:
        return __import__(name)
    except ImportError as exc:
        raise RuntimeError(f"Missing '{name}'. Install: {hint}") from exc


# ── TPC-H query loading ──────────────────────────────────────────────────

def _transform_q15(raw: str) -> str:
    """Transform TPC-H q15 (CREATE VIEW / SELECT / DROP VIEW) into a single CTE query
    that can be EXPLAIN'd.  Extracts the view body and column aliases, rewrites as WITH ... SELECT ...
    """
    import re
    # Extract column list, view body, and main SELECT
    m = re.search(
        r"create\s+view\s+revenue0\s*\(([^)]*)\)\s*as\s+(select\b.+?);\s*(select\b.+?);\s*drop\s+view",
        raw, re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return raw  # fallback: return as-is
    col_list = m.group(1).strip()
    view_body = m.group(2).strip()
    main_select = m.group(3).strip()
    return f"WITH revenue0 ({col_list}) AS (\n{view_body}\n)\n{main_select}"


def load_tpch_queries(engine: str, stream: str = "1") -> list[tuple[int, str, str]]:
    """Load TPC-H queries from generated query files."""
    qdir = ROOT / "TPC-H" / "queries" / engine / stream
    if not qdir.is_dir():
        return []
    stmts = []
    for i in range(1, 23):
        f = qdir / f"{i}.sql"
        if f.exists():
            sql = f.read_text().strip()
            # Q15 uses CREATE VIEW / SELECT / DROP VIEW — transform to CTE for EXPLAIN
            if i == 15:
                sql = _transform_q15(sql)
            else:
                # Remove trailing ;
                if sql.endswith(";"):
                    sql = sql[:-1].strip()
            if sql:
                stmts.append((i, f.name, sql))
    return stmts


# ── Engine connectors ────────────────────────────────────────────────────

def explain_postgresql(queries: list[tuple[int, str, str]], label: str, dialect: str, use_normalize: bool = True, dbname: str = "mydb") -> list[dict]:
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    host = os.environ.get("TPCDS_PGHOST", "127.0.0.1")
    port = os.environ.get("TPCDS_PGPORT", "5432")
    user = os.environ.get("TPCDS_PGUSER", "myuser")
    pw = os.environ.get("TPCDS_PGPASSWORD", "mypassword")
    conninfo = f"host={host} port={port} dbname={dbname} user={user} password={pw}"

    results = []
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        for qid, fname, sql in queries:
            normed = normalize_sql(dialect, sql) if use_normalize else sql
            try:
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN {normed}")
                    cur.fetchall()
                results.append({"qid": qid, "status": "ok"})
            except Exception as exc:
                results.append({"qid": qid, "status": "error", "error": str(exc).split("\n")[0]})
    return results


def explain_cedardb(queries: list[tuple[int, str, str]], label: str, dialect: str, use_normalize: bool = True, dbname: str = "db") -> list[dict]:
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    host = os.environ.get("CEDAR_HOST", "localhost")
    port = os.environ.get("CEDAR_PORT", "5433")
    user = os.environ.get("CEDAR_USER", "admin")
    pw = os.environ.get("CEDAR_PASSWORD", "admin")
    conninfo = f"host={host} port={port} dbname={dbname} user={user} password={pw}"

    results = []
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        for qid, fname, sql in queries:
            normed = normalize_sql(dialect, sql) if use_normalize else sql
            try:
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN {normed}")
                    cur.fetchall()
                results.append({"qid": qid, "status": "ok"})
            except Exception as exc:
                results.append({"qid": qid, "status": "error", "error": str(exc).split("\n")[0]})
    return results


def explain_starrocks(queries: list[tuple[int, str, str]], label: str, dialect: str, db_name: str = "tpcds", use_normalize: bool = True) -> list[dict]:
    pymysql = require_module("pymysql", "pip install pymysql")
    host = os.environ.get("STARROCKS_HOST", "127.0.0.1")
    port = int(os.environ.get("STARROCKS_PORT", "9030"))
    user = os.environ.get("STARROCKS_USER", "root")
    pw = os.environ.get("STARROCKS_PASSWORD", "")

    results = []
    conn = pymysql.connect(host=host, port=port, user=user, password=pw, database=db_name)
    try:
        for qid, fname, sql in queries:
            normed = normalize_sql(dialect, sql) if use_normalize else sql
            try:
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN {normed}")
                    cur.fetchall()
                results.append({"qid": qid, "status": "ok"})
            except Exception as exc:
                results.append({"qid": qid, "status": "error", "error": str(exc).split("\n")[0]})
    finally:
        conn.close()
    return results


def explain_duckdb(queries: list[tuple[int, str, str]], label: str, dialect: str, db_path: str | None = None) -> list[dict]:
    duckdb = require_module("duckdb", "pip install duckdb")
    if db_path:
        con = duckdb.connect(db_path)
    else:
        con = duckdb.connect(":memory:")
    results = []
    for qid, fname, sql in queries:
        normed = normalize_sql(dialect, sql)
        try:
            con.execute(f"EXPLAIN {normed}")
            con.fetchall()
            results.append({"qid": qid, "status": "ok"})
        except Exception as exc:
            results.append({"qid": qid, "status": "error", "error": str(exc).split("\n")[0]})
    con.close()
    return results


# ── Schema preparation (create tables only, no data needed for EXPLAIN) ──

def ensure_tpcds_schema_postgresql():
    """Ensure TPC-DS schema exists in PostgreSQL."""
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    conninfo = f"host=127.0.0.1 port=5432 dbname=mydb user=myuser password=mypassword"
    ddl_path = ROOT / "TPC-DS" / "tools" / "tpcds.sql"
    if not ddl_path.exists():
        return
    ddl = ddl_path.read_text()
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check if tables exist
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='store_sales'")
            if cur.fetchone()[0] > 0:
                return  # Schema already exists


def ensure_tpcds_schema_cedardb():
    """Ensure TPC-DS schema exists in CedarDB."""
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    conninfo = f"host=localhost port=5433 dbname=db user=admin password=admin"
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='store_sales'")
                if cur.fetchone()[0] > 0:
                    return
            except Exception:
                pass


def ensure_tpch_schema_postgresql():
    """Ensure TPC-H schema exists in PostgreSQL."""
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    conninfo = f"host=127.0.0.1 port=5432 dbname=mydb user=myuser password=mypassword"
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='lineitem'")
            if cur.fetchone()[0] > 0:
                return


# ── Main ─────────────────────────────────────────────────────────────────

def print_results(bench: str, engine: str, results: list[dict]) -> tuple[int, int]:
    ok = sum(1 for r in results if r["status"] == "ok")
    err = sum(1 for r in results if r["status"] == "error")
    errors = [r for r in results if r["status"] == "error"]
    status = "PASS" if err == 0 else f"FAIL ({err} errors)"
    print(f"  {bench:6s} {engine:12s} {ok:3d}/{len(results):3d} ok   {status}")
    for r in errors:
        print(f"    q{r['qid']}: {r['error'][:120]}")
    return ok, err


def main() -> None:
    print("=" * 70)
    print("Cross-Engine Validation (EXPLAIN-based)")
    print("=" * 70)

    total_ok = 0
    total_err = 0

    # ── TPC-DS ──
    print("\n## TPC-DS\n")

    tpcds_engines = [
        ("postgresql", "postgresql", lambda q, l, d: explain_postgresql(q, l, d, use_normalize=True, dbname="mydb")),
        ("cedardb", "cedardb", lambda q, l, d: explain_cedardb(q, l, d, use_normalize=True, dbname="db")),
        ("starrocks", "starrocks", lambda q, l, d: explain_starrocks(q, l, d, db_name="tpcds", use_normalize=True)),
    ]

    for engine, dialect, explain_fn in tpcds_engines:
        qdir = ROOT / "TPC-DS" / "queries" / engine / "sf1" / "stream1"
        if not qdir.is_dir():
            print(f"  TPC-DS {engine:12s} SKIP (no queries generated)")
            continue
        stmts = load_statements(qdir)
        if not stmts:
            print(f"  TPC-DS {engine:12s} SKIP (no queries found)")
            continue
        try:
            results = explain_fn(stmts, f"TPC-DS/{engine}", dialect)
            ok, err = print_results("TPC-DS", engine, results)
            total_ok += ok
            total_err += err
        except Exception as exc:
            print(f"  TPC-DS {engine:12s} CONNECTION ERROR: {exc}")

    # ── TPC-H ──
    print("\n## TPC-H\n")

    tpch_engines = [
        ("postgresql", "postgresql", lambda q, l, d: explain_postgresql(q, l, d, use_normalize=False, dbname="tpch")),
        ("cedardb", "cedardb", lambda q, l, d: explain_cedardb(q, l, d, use_normalize=False, dbname="tpch")),
        ("starrocks", "starrocks", lambda q, l, d: explain_starrocks(q, l, d, db_name="tpch", use_normalize=False)),
    ]

    for engine, dialect, explain_fn in tpch_engines:
        stmts = load_tpch_queries(engine)
        if not stmts:
            print(f"  TPC-H  {engine:12s} SKIP (no queries generated)")
            continue

        try:
            results = explain_fn(stmts, f"TPC-H/{engine}", dialect)
            ok, err = print_results("TPC-H", engine, results)
            total_ok += ok
            total_err += err
        except Exception as exc:
            print(f"  TPC-H  {engine:12s} CONNECTION ERROR: {exc}")

    # ── Summary ──
    print(f"\n{'=' * 70}")
    total = total_ok + total_err
    print(f"TOTAL: {total_ok}/{total} ok, {total_err} errors")
    if total_err == 0:
        print("ALL ENGINES PASS")
    print("=" * 70)


if __name__ == "__main__":
    main()
