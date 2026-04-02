#!/usr/bin/env python3
"""Bulk-load TPC-DS flat files into CedarDB using COPY FROM STDIN.

dsdgen produces pipe-delimited files with a trailing '|' on every row.
PostgreSQL's COPY parser would treat that as an extra empty column and fail,
so we strip the trailing delimiter while streaming each file.
"""
import argparse
from pathlib import Path

import psycopg


def conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.host} port={args.port} "
        f"dbname={args.dbname} user={args.user} password={args.password}"
    )


def load_table(conn: psycopg.Connection, table: str, dat_file: Path) -> None:
    copy_sql = (
        f'COPY "{table}" FROM STDIN '
        "WITH (FORMAT CSV, DELIMITER '|', HEADER FALSE, NULL '')"
    )
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with dat_file.open("rb") as fh:
                for raw_line in fh:
                    # Strip newline then trailing pipe added by dsdgen.
                    line = raw_line.rstrip(b"\r\n")
                    if line.endswith(b"|"):
                        line = line[:-1]
                    copy.write(line + b"\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-load TPC-DS data into CedarDB")
    parser.add_argument("--data-dir", required=True, help="Directory with *.dat files from dsdgen")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="db")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="admin")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    dat_files = sorted(data_dir.glob("*.dat"))
    if not dat_files:
        raise RuntimeError(f"No .dat files found in {data_dir}")

    with psycopg.connect(conninfo(args), autocommit=True) as conn:
        loaded = 0
        for dat_file in dat_files:
            table = dat_file.stem
            load_table(conn, table, dat_file)
            loaded += 1
            print(f"loaded:{table}")

    print(f"tables_loaded:{loaded}")


if __name__ == "__main__":
    main()
