#!/usr/bin/env python3
"""Run all 99 TPC-DS queries individually with DuckDB JSON profiling enabled."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "tools"))

from pipeline_common import load_statements, normalize_sql


def require_module(module_name: str, install_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(f"Missing Python dependency '{module_name}'. Install with: {install_hint}") from exc


def split_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile TPC-DS queries with DuckDB JSON profiling")
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--stream", type=int, default=1)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    duckdb = require_module("duckdb", "pip install duckdb")

    db_path = ROOT_DIR / "logs" / "duckdb" / f"sf{args.scale}" / f"tpcds_sf{args.scale}.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DuckDB file {db_path}. Run the pipeline first.")

    queries_dir = ROOT_DIR / "queries" / "duckdb" / f"sf{args.scale}" / f"stream{args.stream}"
    if not queries_dir.exists():
        raise FileNotFoundError(f"Queries directory not found: {queries_dir}. Run generate-queries first.")

    statements = load_statements(queries_dir)

    output_dir = ROOT_DIR / "logs" / "duckdb" / f"sf{args.scale}" / "profile"
    output_dir.mkdir(parents=True, exist_ok=True)

    for old in output_dir.glob("q*.json"):
        old.unlink()

    errors: list[tuple[int, str]] = []

    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(f"PRAGMA threads={args.threads}")
        for query_id, source_file, sql_text in statements:
            profile_path = output_dir / f"q{query_id}.json"
            try:
                con.execute("PRAGMA enable_profiling='json'")
                con.execute(f"PRAGMA profiling_output='{profile_path}'")

                for stmt in split_statements(normalize_sql("duckdb", sql_text)):
                    result = con.execute(stmt)
                    if result.description is not None:
                        result.fetchall()

                con.execute("PRAGMA disable_profiling")
                print(f"Profiled query {query_id}")
            except Exception as exc:  # noqa: BLE001
                errors.append((query_id, str(exc)))
                print(f"Error profiling query {query_id}: {exc}")
                try:
                    con.execute("PRAGMA disable_profiling")
                except Exception:  # noqa: BLE001
                    pass
    finally:
        con.close()

    print(f"\nDone. JSON profiles written to {output_dir}")
    if errors:
        print(f"{len(errors)} queries failed:")
        for qid, err in errors:
            print(f"  q{qid}: {err}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
