#!/usr/bin/env python3
import argparse
from pathlib import Path

import duckdb


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-load TPC-DS flat files into DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database file path")
    parser.add_argument("--data-dir", required=True, help="Directory containing *.dat files from dsdgen")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB worker threads")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    dat_files = sorted(data_dir.glob("*.dat"))
    if not dat_files:
        raise RuntimeError(f"No .dat files found in {data_dir}")

    con = duckdb.connect(args.db)
    con.execute(f"PRAGMA threads={args.threads}")

    loaded = 0
    for dat_file in dat_files:
        table_name = dat_file.stem
        copy_sql = (
            f"COPY {quote_ident(table_name)} FROM '{dat_file.as_posix()}' "
            "(DELIMITER '|', HEADER FALSE, NULLSTR '', AUTO_DETECT FALSE)"
        )
        con.execute(copy_sql)
        loaded += 1
        print(f"loaded:{table_name}")

    con.close()
    print(f"tables_loaded:{loaded}")


if __name__ == "__main__":
    main()
