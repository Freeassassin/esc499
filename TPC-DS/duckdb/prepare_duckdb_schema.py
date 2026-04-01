#!/usr/bin/env python3
import argparse
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Create TPC-DS schema in DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database file path")
    parser.add_argument("--ddl", required=True, help="TPC-DS DDL SQL file path")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB worker threads")
    args = parser.parse_args()

    db_path = Path(args.db)
    ddl_path = Path(args.ddl)

    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8")

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute(ddl_sql)
    con.close()

    print(f"schema_created:{db_path}")


if __name__ == "__main__":
    main()
