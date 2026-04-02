#!/usr/bin/env python3
"""Create TPC-DS schema in CedarDB (PostgreSQL-compatible)."""
import argparse
import re
from pathlib import Path

import psycopg

# All TPC-DS tables in reverse dependency order for clean DROP.
TPCDS_TABLES = [
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns",
    "inventory",
    "catalog_page", "promotion", "web_page",
    "household_demographics", "web_site",
    "customer", "call_center", "store",
    "item", "income_band", "reason",
    "time_dim", "ship_mode", "warehouse",
    "date_dim", "customer_demographics", "customer_address",
    "dbgen_version",
]


def conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.host} port={args.port} "
        f"dbname={args.dbname} user={args.user} password={args.password}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create TPC-DS schema in CedarDB")
    parser.add_argument("--ddl", required=True, help="TPC-DS DDL SQL file path")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="db")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="admin")
    args = parser.parse_args()

    ddl_path = Path(args.ddl)
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8")

    with psycopg.connect(conninfo(args), autocommit=True) as conn:
        with conn.cursor() as cur:
            # Drop all tables to ensure a clean slate.
            for table in TPCDS_TABLES:
                cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

            # Execute the entire DDL in one shot.
            cur.execute(ddl_sql)

    print(f"schema_created:cedardb://{args.host}:{args.port}/{args.dbname}")


if __name__ == "__main__":
    main()
