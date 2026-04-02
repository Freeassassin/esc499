#!/usr/bin/env python3
from __future__ import annotations

import argparse

from common import create_table_sql, default_config, ensure_backend, mysql_conn, parse_tpcds_schema, wait_for_frontend


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Create TPC-DS schema in StarRocks")
    parser.add_argument("--ddl", required=True, help="TPC-DS DDL SQL file path")
    parser.add_argument("--host", default=defaults["mysql_host"])
    parser.add_argument("--port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--user", default=defaults["user"])
    parser.add_argument("--password", default=defaults["password"])
    parser.add_argument("--dbname", default=defaults["database"])
    parser.add_argument("--backend-addr", default=defaults["backend_addr"])
    args = parser.parse_args()

    wait_for_frontend(args.host, args.port, args.user, args.password)
    ensure_backend(args.host, args.port, args.user, args.password, args.backend_addr)
    tables = parse_tpcds_schema(args.ddl)

    with mysql_conn(args.host, args.port, args.user, args.password, database=None) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {args.dbname}")
            cur.execute(f"CREATE DATABASE {args.dbname}")

    with mysql_conn(args.host, args.port, args.user, args.password, database=args.dbname) as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(create_table_sql(table))

    print(f"schema_created:starrocks://{args.host}:{args.port}/{args.dbname}")


if __name__ == "__main__":
    main()