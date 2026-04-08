#!/usr/bin/env python3
"""Run all 22 TPC-H queries with DuckDB JSON profiling enabled."""
from __future__ import annotations

import argparse
from pathlib import Path

TPCH_ROOT = Path(__file__).resolve().parent.parent


def require_module(module_name: str, install_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(f"Missing Python dependency '{module_name}'. Install with: {install_hint}") from exc


def split_statements(sql_text: str) -> list[str]:
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile TPC-H queries with DuckDB JSON profiling")
    parser.add_argument("--scale", default="1")
    parser.add_argument("--stream", default="1")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    duckdb = require_module("duckdb", "pip install duckdb")

    db_file = TPCH_ROOT / "duckdb" / f"tpch_sf{args.scale}.duckdb"
    if not db_file.exists():
        raise FileNotFoundError(f"Missing DuckDB file {db_file}. Run load first.")

    qdir = TPCH_ROOT / "queries" / "duckdb" / args.stream
    if not qdir.exists():
        raise FileNotFoundError(f"Query directory not found: {qdir}. Run generate-queries first.")

    output_dir = TPCH_ROOT / "logs" / "duckdb" / f"sf{args.scale}" / "profile"
    output_dir.mkdir(parents=True, exist_ok=True)

    for old in output_dir.glob("q*.json"):
        old.unlink()

    conn = duckdb.connect(str(db_file), read_only=False)
    try:
        conn.execute(f"PRAGMA threads={args.threads}")
        for i in range(1, 23):
            qpath = qdir / f"{i}.sql"
            if not qpath.exists():
                raise FileNotFoundError(qpath)

            profile_path = output_dir / f"q{i}.json"
            conn.execute("PRAGMA enable_profiling='json'")
            conn.execute(f"PRAGMA profiling_output='{profile_path}'")

            stmts = split_statements(qpath.read_text(encoding="utf-8"))
            for stmt in stmts:
                result = conn.execute(stmt)
                if result.description is not None:
                    result.fetchall()

            conn.execute("PRAGMA disable_profiling")
            print(f"Profiled query {i}")
    finally:
        conn.close()

    print(f"Done. JSON profiles written to {output_dir}")


if __name__ == "__main__":
    main()
