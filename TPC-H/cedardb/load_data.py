from __future__ import annotations

import concurrent.futures
import os
from pathlib import Path
from typing import Iterable

import psycopg

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DBGEN_DIR = REPO_ROOT / "TPC-H" / "dbgen"
DDL_FILE = SCRIPT_DIR / "ddl.sql"

HOST = os.environ.get("TPCH_CEDAR_HOST", "127.0.0.1")
PORT = int(os.environ.get("TPCH_CEDAR_PORT", "5432"))
USER = os.environ.get("TPCH_CEDAR_USER", "admin")
PASSWORD = os.environ.get("TPCH_CEDAR_PASSWORD", "admin")
DBNAME = os.environ.get("TPCH_CEDAR_DB", "db")
PARALLEL_JOBS = int(os.environ.get("TPCH_CEDAR_LOAD_JOBS", "3"))

TABLES_PHASED: list[list[str]] = [
    ["region"],
    ["nation"],
    ["supplier", "customer", "part"],
    ["partsupp"],
    ["orders"],
    ["lineitem"],
]


def dsn() -> str:
    return f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASSWORD}"


def copy_table(table: str) -> None:
    file_path = DBGEN_DIR / f"{table}.tbl"
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    with psycopg.connect(dsn()) as conn:
        with conn.cursor() as cur:
            with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT text, DELIMITER '|')") as copy:
                with file_path.open("r", encoding="utf-8") as infile:
                    for line in infile:
                        if line.endswith("|\n"):
                            copy.write(line[:-2] + "\n")
                        elif line.endswith("|"):
                            copy.write(line[:-1])
                        else:
                            copy.write(line)
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cur.fetchone()[0]
            print(f"Loaded {table}: {row_count} rows")


def run_phase(tables: Iterable[str]) -> None:
    table_list = list(tables)
    if len(table_list) == 1:
        copy_table(table_list[0])
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(PARALLEL_JOBS, len(table_list))) as executor:
        futures = [executor.submit(copy_table, table) for table in table_list]
        for future in concurrent.futures.as_completed(futures):
            future.result()


def main() -> None:
    with psycopg.connect(dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_FILE.read_text(encoding="utf-8"))

    for phase in TABLES_PHASED:
        run_phase(phase)

    print("CedarDB data load complete.")


if __name__ == "__main__":
    main()
