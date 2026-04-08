#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
import psycopg

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from starrocks.common import default_config, mysql_conn

from pipeline_common import load_statements, normalize_sql, write_summary


def split_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def cedar_conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.cedar_host} port={args.cedar_port} "
        f"dbname={args.cedar_dbname} user={args.cedar_user} password={args.cedar_password}"
    )


def execute_duckdb(args: argparse.Namespace, statements: list[tuple[int, str, str]]) -> list[dict[str, object]]:
    db_path = ROOT_DIR / "logs" / "duckdb" / f"sf{args.scale}" / f"tpcds_sf{args.scale}.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={args.threads}")

    summary: list[dict[str, object]] = []
    for query_id, source_file, sql_text in statements:
        start = time.perf_counter()
        try:
            row_count = 0
            normalized = normalize_sql("duckdb", sql_text)
            for stmt in split_statements(normalized):
                result = con.execute(stmt)
                if result.description is not None:
                    row_count = len(result.fetchall())
            elapsed = time.perf_counter() - start
            summary.append(
                {
                    "query_id": query_id,
                    "file": source_file,
                    "status": "ok",
                    "elapsed_sec": round(elapsed, 6),
                    "row_count": row_count,
                }
            )
            print(f"ok:q{query_id}:rows={row_count}:sec={elapsed:.4f}")
        except Exception as exc:  # noqa: BLE001
            elapsed = time.perf_counter() - start
            summary.append(
                {
                    "query_id": query_id,
                    "file": source_file,
                    "status": "error",
                    "elapsed_sec": round(elapsed, 6),
                    "error": str(exc),
                }
            )
            print(f"error:q{query_id}:sec={elapsed:.4f}:{exc}")

    con.close()
    return summary


def execute_cedardb(args: argparse.Namespace, statements: list[tuple[int, str, str]]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    with psycopg.connect(cedar_conninfo(args)) as conn:
        conn.autocommit = True
        for query_id, source_file, sql_text in statements:
            start = time.perf_counter()
            try:
                row_count = 0
                normalized = normalize_sql("cedardb", sql_text)
                with conn.cursor() as cur:
                    for stmt in split_statements(normalized):
                        cur.execute(stmt)  # type: ignore[arg-type]
                        if cur.description is not None:
                            row_count = len(cur.fetchall())
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "ok",
                        "elapsed_sec": round(elapsed, 6),
                        "row_count": row_count,
                    }
                )
                print(f"ok:q{query_id}:rows={row_count}:sec={elapsed:.4f}")
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "error",
                        "elapsed_sec": round(elapsed, 6),
                        "error": str(exc),
                    }
                )
                print(f"error:q{query_id}:sec={elapsed:.4f}:{exc}")

    return summary


def pg_conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.pg_host} port={args.pg_port} "
        f"dbname={args.pg_dbname} user={args.pg_user} password={args.pg_password}"
    )


def execute_postgresql(args: argparse.Namespace, statements: list[tuple[int, str, str]]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    with psycopg.connect(pg_conninfo(args)) as conn:
        conn.autocommit = True
        for query_id, source_file, sql_text in statements:
            start = time.perf_counter()
            try:
                row_count = 0
                normalized = normalize_sql("postgresql", sql_text)
                with conn.cursor() as cur:
                    # Keep PostgreSQL execution as a single script per query file.
                    cur.execute(normalized)  # type: ignore[arg-type]
                    while True:
                        if cur.description is not None:
                            row_count = len(cur.fetchall())
                        if not cur.nextset():
                            break
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "ok",
                        "elapsed_sec": round(elapsed, 6),
                        "row_count": row_count,
                    }
                )
                print(f"ok:q{query_id}:rows={row_count}:sec={elapsed:.4f}")
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "error",
                        "elapsed_sec": round(elapsed, 6),
                        "error": str(exc),
                    }
                )
                print(f"error:q{query_id}:sec={elapsed:.4f}:{exc}")

    return summary


def execute_starrocks(args: argparse.Namespace, statements: list[tuple[int, str, str]]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    with mysql_conn(
        args.starrocks_host,
        args.starrocks_port,
        args.starrocks_user,
        args.starrocks_password,
        database=args.starrocks_dbname,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SET query_timeout = 3600")

        for query_id, source_file, sql_text in statements:
            start = time.perf_counter()
            try:
                row_count = 0
                normalized = normalize_sql("starrocks", sql_text)
                with conn.cursor() as cur:
                    for stmt in split_statements(normalized):
                        cur.execute(stmt)
                        if cur.description:
                            row_count = len(list(cur.fetchall()))
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "ok",
                        "elapsed_sec": round(elapsed, 6),
                        "row_count": row_count,
                    }
                )
                print(f"ok:q{query_id}:rows={row_count}:sec={elapsed:.4f}")
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "error",
                        "elapsed_sec": round(elapsed, 6),
                        "error": str(exc),
                    }
                )
                print(f"error:q{query_id}:sec={elapsed:.4f}:{exc}")

    return summary


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Run generated TPC-DS queries for one engine")
    parser.add_argument("--engine", required=True, choices=["duckdb", "cedardb", "starrocks", "postgresql"])
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--stream", type=int, default=1)
    parser.add_argument("--threads", type=int, default=4)

    parser.add_argument("--cedar-host", default=os.environ.get("CEDAR_HOST", "localhost"))
    parser.add_argument("--cedar-port", type=int, default=int(os.environ.get("CEDAR_PORT", "5433")))
    parser.add_argument("--cedar-dbname", default=os.environ.get("CEDAR_DB", "db"))
    parser.add_argument("--cedar-user", default=os.environ.get("CEDAR_USER", "admin"))
    parser.add_argument("--cedar-password", default=os.environ.get("CEDAR_PASS", "admin"))

    parser.add_argument("--pg-host", default=os.environ.get("TPCDS_PGHOST", "127.0.0.1"))
    parser.add_argument("--pg-port", type=int, default=int(os.environ.get("TPCDS_PGPORT", "5432")))
    parser.add_argument("--pg-dbname", default=os.environ.get("TPCDS_PGDATABASE", "mydb"))
    parser.add_argument("--pg-user", default=os.environ.get("TPCDS_PGUSER", "myuser"))
    parser.add_argument("--pg-password", default=os.environ.get("TPCDS_PGPASSWORD", "mypassword"))

    parser.add_argument("--starrocks-host", default=defaults["mysql_host"])
    parser.add_argument("--starrocks-port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--starrocks-user", default=defaults["user"])
    parser.add_argument("--starrocks-password", default=defaults["password"])
    parser.add_argument("--starrocks-dbname", default=defaults["database"])

    args = parser.parse_args()

    queries_dir = ROOT_DIR / "queries" / args.engine / f"sf{args.scale}" / f"stream{args.stream}"
    if not queries_dir.exists():
        raise FileNotFoundError(
            f"Queries directory does not exist: {queries_dir}. Run generate-queries first."
        )
    statements = load_statements(queries_dir)

    if args.engine == "duckdb":
        summary = execute_duckdb(args, statements)
    elif args.engine == "cedardb":
        summary = execute_cedardb(args, statements)
    elif args.engine == "postgresql":
        summary = execute_postgresql(args, statements)
    else:
        summary = execute_starrocks(args, statements)

    summary_path = ROOT_DIR / "logs" / args.engine / f"sf{args.scale}" / f"stream{args.stream}" / "query_summary.json"
    write_summary(summary_path, summary)

    failures = [item for item in summary if item["status"] != "ok"]
    if failures:
        raise SystemExit(f"{len(failures)} query failures; see {summary_path}")

    print(f"all_queries_ok:99:summary={summary_path}")


if __name__ == "__main__":
    main()
