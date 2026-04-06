#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import psycopg

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from starrocks.common import (
    create_table_sql,
    default_config,
    ensure_backend,
    mysql_conn,
    parse_tpcds_schema,
    wait_for_frontend,
)

DDL_PATH = ROOT_DIR / "tools" / "tpcds.sql"


def prepare_duckdb(scale: int, threads: int) -> None:
    db_path = ROOT_DIR / "logs" / "duckdb" / f"sf{scale}" / f"tpcds_sf{scale}.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    ddl_sql = DDL_PATH.read_text(encoding="utf-8")
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={threads}")
    con.execute(ddl_sql)
    con.close()
    print(f"schema_created:{db_path}")


def cedar_conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.cedar_host} port={args.cedar_port} "
        f"dbname={args.cedar_dbname} user={args.cedar_user} password={args.cedar_password}"
    )


def prepare_cedardb(args: argparse.Namespace) -> None:
    tpcds_tables = [
        "store_sales", "catalog_sales", "web_sales",
        "store_returns", "catalog_returns", "web_returns",
        "inventory", "catalog_page", "promotion", "web_page",
        "household_demographics", "web_site", "customer", "call_center", "store",
        "item", "income_band", "reason", "time_dim", "ship_mode", "warehouse",
        "date_dim", "customer_demographics", "customer_address", "dbgen_version",
    ]
    ddl_sql = DDL_PATH.read_text(encoding="utf-8")
    with psycopg.connect(cedar_conninfo(args), autocommit=True) as conn:
        with conn.cursor() as cur:
            for table in tpcds_tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')  # type: ignore[arg-type]
            cur.execute(ddl_sql)  # type: ignore[arg-type]
    print(f"schema_created:cedardb://{args.cedar_host}:{args.cedar_port}/{args.cedar_dbname}")


def prepare_starrocks(args: argparse.Namespace) -> None:
    wait_for_frontend(args.starrocks_host, args.starrocks_port, args.starrocks_user, args.starrocks_password)
    ensure_backend(
        args.starrocks_host,
        args.starrocks_port,
        args.starrocks_user,
        args.starrocks_password,
        args.starrocks_backend,
    )
    tables = parse_tpcds_schema(DDL_PATH)

    with mysql_conn(args.starrocks_host, args.starrocks_port, args.starrocks_user, args.starrocks_password, database=None) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {args.starrocks_dbname}")
            cur.execute(f"CREATE DATABASE {args.starrocks_dbname}")

    with mysql_conn(
        args.starrocks_host,
        args.starrocks_port,
        args.starrocks_user,
        args.starrocks_password,
        database=args.starrocks_dbname,
    ) as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(create_table_sql(table))

    print(f"schema_created:starrocks://{args.starrocks_host}:{args.starrocks_port}/{args.starrocks_dbname}")


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Prepare TPC-DS schema for one engine")
    parser.add_argument("--engine", required=True, choices=["duckdb", "cedardb", "starrocks"])
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--threads", type=int, default=4)

    parser.add_argument("--cedar-host", default=os.environ.get("CEDAR_HOST", "localhost"))
    parser.add_argument("--cedar-port", type=int, default=int(os.environ.get("CEDAR_PORT", "5433")))
    parser.add_argument("--cedar-dbname", default=os.environ.get("CEDAR_DB", "db"))
    parser.add_argument("--cedar-user", default=os.environ.get("CEDAR_USER", "admin"))
    parser.add_argument("--cedar-password", default=os.environ.get("CEDAR_PASS", "admin"))

    parser.add_argument("--starrocks-host", default=defaults["mysql_host"])
    parser.add_argument("--starrocks-port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--starrocks-user", default=defaults["user"])
    parser.add_argument("--starrocks-password", default=defaults["password"])
    parser.add_argument("--starrocks-dbname", default=defaults["database"])
    parser.add_argument("--starrocks-backend", default=defaults["backend_addr"])

    args = parser.parse_args()

    if args.engine == "duckdb":
        prepare_duckdb(args.scale, args.threads)
    elif args.engine == "cedardb":
        prepare_cedardb(args)
    else:
        prepare_starrocks(args)


if __name__ == "__main__":
    main()
