from __future__ import annotations

from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
DB_FILE = SCRIPT_DIR / "tpch_sf1.duckdb"
QUERY_DIR = SCRIPT_DIR / "queries"
LOG_DIR = SCRIPT_DIR / "logs"


def split_statements(sql_text: str) -> list[str]:
    statements = [statement.strip() for statement in sql_text.split(";")]
    return [statement for statement in statements if statement]


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for log_file in LOG_DIR.glob("q*.log"):
        log_file.unlink()

    conn = duckdb.connect(str(DB_FILE), read_only=False)

    for query_number in range(1, 23):
        query_path = QUERY_DIR / f"{query_number}.sql"
        if not query_path.exists():
            raise FileNotFoundError(query_path)

        log_path = LOG_DIR / f"q{query_number}.log"
        lines: list[str] = []
        for statement_index, statement in enumerate(split_statements(query_path.read_text()), start=1):
            result = conn.execute(statement)
            if result.description is None:
                continue

            columns = [column[0] for column in result.description]
            rows = result.fetchall()
            lines.append(f"-- statement {statement_index}")
            lines.append("\t".join(columns))
            lines.extend("\t".join(str(value) for value in row) for row in rows)
            lines.append("")

        log_path.write_text("\n".join(lines).rstrip() + "\n")
        print(f"Executed query {query_number}")

    conn.close()
    print(f"All 22 DuckDB queries executed successfully. Logs are in {LOG_DIR}")


if __name__ == "__main__":
    main()
