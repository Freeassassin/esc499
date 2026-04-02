from __future__ import annotations

import os
from pathlib import Path

import psycopg

SCRIPT_DIR = Path(__file__).resolve().parent
QUERY_DIR = SCRIPT_DIR / "queries"
LOG_DIR = SCRIPT_DIR / "logs"

HOST = os.environ.get("TPCH_CEDAR_HOST", "127.0.0.1")
PORT = int(os.environ.get("TPCH_CEDAR_PORT", "5432"))
USER = os.environ.get("TPCH_CEDAR_USER", "admin")
PASSWORD = os.environ.get("TPCH_CEDAR_PASSWORD", "admin")
DBNAME = os.environ.get("TPCH_CEDAR_DB", "db")


def dsn() -> str:
    return f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASSWORD}"


def split_statements(sql_text: str) -> list[str]:
    statements = [statement.strip() for statement in sql_text.split(";")]
    return [statement for statement in statements if statement]


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for old_log in LOG_DIR.glob("q*.log"):
        old_log.unlink()

    with psycopg.connect(dsn()) as conn:
        for query_number in range(1, 23):
            query_path = QUERY_DIR / f"{query_number}.sql"
            if not query_path.exists():
                raise FileNotFoundError(query_path)

            output_lines: list[str] = []
            with conn.cursor() as cur:
                for statement_index, statement in enumerate(split_statements(query_path.read_text(encoding="utf-8")), start=1):
                    cur.execute(statement)
                    if cur.description is None:
                        continue
                    columns = [column.name for column in cur.description]
                    rows = cur.fetchall()
                    output_lines.append(f"-- statement {statement_index}")
                    output_lines.append("\t".join(columns))
                    output_lines.extend("\t".join(str(value) for value in row) for row in rows)
                    output_lines.append("")

            (LOG_DIR / f"q{query_number}.log").write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")
            print(f"Executed query {query_number}")

    print(f"All 22 CedarDB queries executed successfully. Logs are in {LOG_DIR}")


if __name__ == "__main__":
    main()
