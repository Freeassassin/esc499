from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import re
import subprocess
import time
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[2]
TPCDS_ROOT = ROOT / "TPC-DS"
TOOLS_DIR = TPCDS_ROOT / "tools"
TEMPLATE_DIR = TPCDS_ROOT / "query_templates"
POSTGRES_DIR = TPCDS_ROOT / "postgresql"
RESET_SCHEMA_SQL = POSTGRES_DIR / "reset_schema.sql"
BASE_SCHEMA_SQL = TOOLS_DIR / "tpcds.sql"

DB_CONFIG = {
    "host": os.environ.get("TPCDS_PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("TPCDS_PGPORT", "5432")),
    "dbname": os.environ.get("TPCDS_PGDATABASE", "mydb"),
    "user": os.environ.get("TPCDS_PGUSER", "myuser"),
    "password": os.environ.get("TPCDS_PGPASSWORD", "mypassword"),
}

QUERY_DATE_INTERVAL_RE = re.compile(r"([+-])\s*(\d+)\s+days?\b", re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r"^create table\s+([a-z_]+)", re.IGNORECASE | re.MULTILINE)
TABLE_BLOCK_RE = re.compile(r"create table\s+([a-z_]+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
STATEMENT_SPLIT_RE = re.compile(r";\s*(?:\n|$)")


def run_command(command: list[str], cwd: Path | None = None, stdout_path: Path | None = None) -> None:
    stdout = subprocess.PIPE if stdout_path is None else stdout_path.open("w", encoding="utf-8")
    try:
        subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            check=True,
            text=True,
            stdout=stdout,
            stderr=None if stdout_path is not None else subprocess.STDOUT,
        )
    finally:
        if stdout_path is not None:
            stdout.close()


def ensure_compose_db() -> None:
    run_command(["docker", "compose", "up", "-d", "db"], cwd=ROOT)


def wait_for_database(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while True:
        try:
            with psycopg.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1")
                return
        except psycopg.Error:
            if time.time() >= deadline:
                raise
            time.sleep(1)


def build_tools() -> None:
    run_command(["make", "OS=LINUX", "-j1", "all"], cwd=TOOLS_DIR)


def generate_data(scale: int, data_dir: Path, regenerate: bool) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    sentinel = data_dir / "store_sales.dat"
    if sentinel.exists() and not regenerate:
        return
    run_command(
        [
            str(TOOLS_DIR / "dsdgen"),
            "-scale",
            str(scale),
            "-dir",
            str(data_dir),
            "-force",
            "y",
            "-rngseed",
            "19620718",
            "-quiet",
            "y",
        ],
        cwd=TOOLS_DIR,
    )


def normalize_query_sql(sql_text: str) -> str:
    sql_text = QUERY_DATE_INTERVAL_RE.sub(lambda match: f"{match.group(1)} interval '{match.group(2)} days'", sql_text)
    return sql_text


def generate_queries(scale: int, raw_dir: Path, normalized_dir: Path) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_dir.mkdir(parents=True, exist_ok=True)
    input_dir = raw_dir / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for query_number in range(1, 100):
        template_name = f"query{query_number}.tpl"
        raw_path = raw_dir / f"query{query_number:02d}.sql"
        normalized_path = normalized_dir / f"query{query_number:02d}.sql"
        dsqgen_output_dir = raw_dir / f"out_{query_number:02d}"
        dsqgen_output_dir.mkdir(parents=True, exist_ok=True)
        input_path = input_dir / f"query{query_number:02d}.lst"
        input_path.write_text(f"{template_name}\n", encoding="utf-8")
        run_command(
            [
                str(TOOLS_DIR / "dsqgen"),
                "-quiet",
                "y",
                "-scale",
                str(scale),
                "-input",
                str(input_path),
                "-output_dir",
                str(dsqgen_output_dir),
                "-streams",
                "1",
                "-directory",
                str(TEMPLATE_DIR),
                "-dialect",
                "postgresql",
            ],
            cwd=TOOLS_DIR,
        )
        generated_sql = dsqgen_output_dir / "query_0.sql"
        raw_path.write_text(generated_sql.read_text(encoding="utf-8"), encoding="utf-8")
        normalized_path.write_text(normalize_query_sql(raw_path.read_text(encoding="utf-8")), encoding="utf-8")
        generated.append(normalized_path)
    return generated


def ddl_statements() -> list[str]:
    ddl = BASE_SCHEMA_SQL.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in STATEMENT_SPLIT_RE.split(ddl) if statement.strip()]
    return statements


def table_names() -> list[str]:
    return CREATE_TABLE_RE.findall(BASE_SCHEMA_SQL.read_text(encoding="utf-8"))


def sk_indexes() -> list[tuple[str, str]]:
    indexes: list[tuple[str, str]] = []
    ddl = BASE_SCHEMA_SQL.read_text(encoding="utf-8")
    for table_name, block in TABLE_BLOCK_RE.findall(ddl):
        primary_keys = set()
        pk_match = re.search(r"primary key\s*\(([^\)]+)\)", block, re.IGNORECASE)
        if pk_match:
            primary_keys = {column.strip() for column in pk_match.group(1).split(",")}
        for raw_line in block.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.lower().startswith("primary key"):
                continue
            parts = line.split()
            column_name = parts[0]
            if column_name.endswith("_sk") and column_name not in primary_keys:
                indexes.append((table_name, column_name))
    return indexes


def reset_schema() -> None:
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(RESET_SCHEMA_SQL.read_text(encoding="utf-8"))
            for statement in ddl_statements():
                cur.execute(statement)


def strip_trailing_pipe(line: str) -> str:
    if line.endswith("|\n"):
        return line[:-2] + "\n"
    if line.endswith("|"):
        return line[:-1]
    return line


def copy_table(table: str, data_path: Path) -> dict[str, object]:
    started = time.time()
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("set search_path to tpcds, public")
            with cur.copy(
                f"COPY {table} FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"
            ) as copy:
                with data_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        copy.write(strip_trailing_pipe(line))
    return {"table": table, "seconds": round(time.time() - started, 3), "rows_file": data_path.name}


def load_data(data_dir: Path, workers: int) -> list[dict[str, object]]:
    tables = table_names()
    missing = [table for table in tables if not (data_dir / f"{table}.dat").exists()]
    if missing:
        raise FileNotFoundError(f"Missing data files for tables: {', '.join(missing)}")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(copy_table, table, data_dir / f"{table}.dat"): table
            for table in tables
        }
        for future in concurrent.futures.as_completed(future_map):
            results.append(future.result())
    return sorted(results, key=lambda item: item["table"])


def analyze_tables() -> None:
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("set search_path to tpcds, public")
            for table in table_names():
                cur.execute(f"ANALYZE {table}")


def create_indexes() -> list[str]:
    created = []
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("set search_path to tpcds, public")
            for table_name, column_name in sk_indexes():
                index_name = f"idx_{table_name}_{column_name}"
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
                )
                created.append(index_name)
    return created


def write_result_tsv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(rows)


def execute_queries(query_paths: list[Path], results_dir: Path) -> list[dict[str, object]]:
    results_dir.mkdir(parents=True, exist_ok=True)
    summaries = []
    with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("set search_path to tpcds, public")
            cur.execute("set statement_timeout to 0")
            for query_path in query_paths:
                sql_text = query_path.read_text(encoding="utf-8")
                print(f"Running {query_path.stem}...", flush=True)
                started = time.time()
                cur.execute(sql_text)
                rows = cur.fetchall() if cur.description else []
                headers = [desc.name for desc in cur.description] if cur.description else []
                elapsed = round(time.time() - started, 3)
                print(f"Completed {query_path.stem} in {elapsed}s ({len(rows)} rows)", flush=True)
                result_path = results_dir / f"{query_path.stem}.tsv"
                write_result_tsv(result_path, headers, rows)
                summaries.append(
                    {
                        "query": query_path.stem,
                        "seconds": elapsed,
                        "rows": len(rows),
                        "result_file": str(result_path.relative_to(TPCDS_ROOT)),
                    }
                )
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description="Build, load, and run TPC-DS on PostgreSQL")
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--workers", type=int, default=max(1, min(4, (os.cpu_count() or 1))))
    parser.add_argument("--regenerate", action="store_true")
    args = parser.parse_args()

    run_root = POSTGRES_DIR / "generated" / f"sf{args.scale}"
    data_dir = run_root / "data"
    raw_query_dir = run_root / "raw_queries"
    normalized_query_dir = run_root / "queries"
    results_dir = run_root / "results"
    logs_dir = run_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ensure_compose_db()
    wait_for_database()
    build_tools()
    generate_data(args.scale, data_dir, regenerate=args.regenerate)
    query_paths = generate_queries(args.scale, raw_query_dir, normalized_query_dir)
    reset_schema()
    load_summary = load_data(data_dir, args.workers)
    index_summary = create_indexes()
    analyze_tables()
    query_summary = execute_queries(query_paths, results_dir)

    summary = {
        "scale": args.scale,
        "workers": args.workers,
        "database": DB_CONFIG["dbname"],
        "schema": "tpcds",
        "load": load_summary,
        "indexes": index_summary,
        "queries": query_summary,
    }
    (logs_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"status": "ok", "summary": str((logs_dir / 'summary.json').relative_to(TPCDS_ROOT))}, indent=2))


if __name__ == "__main__":
    main()