from __future__ import annotations

import argparse
import os
from pathlib import Path

TPCH_ROOT = Path(__file__).resolve().parent.parent


def require_module(module_name: str, install_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(f"Missing Python dependency '{module_name}'. Install with: {install_hint}") from exc


def split_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def query_dir(engine: str, stream: str) -> Path:
    return TPCH_ROOT / "queries" / engine / stream


def log_dir(engine: str, scale: str, stream: str) -> Path:
    return TPCH_ROOT / "logs" / engine / f"sf{scale}" / f"stream{stream}"


def execute_duckdb(scale: str, stream: str) -> None:
    duckdb = require_module("duckdb", "pip install duckdb")
    db_file = TPCH_ROOT / "duckdb" / f"tpch_sf{scale}.duckdb"
    if not db_file.exists():
        raise FileNotFoundError(f"Missing DuckDB file {db_file}. Run load first.")

    qdir = query_dir("duckdb", stream)
    ldir = log_dir("duckdb", scale, stream)
    ldir.mkdir(parents=True, exist_ok=True)

    for old_log in ldir.glob("q*.log"):
        old_log.unlink()

    conn = duckdb.connect(str(db_file), read_only=False)
    try:
        for i in range(1, 23):
            qpath = qdir / f"{i}.sql"
            if not qpath.exists():
                raise FileNotFoundError(qpath)
            lines: list[str] = []
            for idx, statement in enumerate(split_statements(qpath.read_text(encoding="utf-8")), start=1):
                result = conn.execute(statement)
                if result.description is None:
                    continue
                columns = [c[0] for c in result.description]
                rows = result.fetchall()
                lines.append(f"-- statement {idx}")
                lines.append("\t".join(columns))
                lines.extend("\t".join(str(v) for v in row) for row in rows)
                lines.append("")
            (ldir / f"q{i}.log").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
            print(f"Executed query {i}")
    finally:
        conn.close()


def execute_psycopg(engine: str, scale: str, stream: str, dsn: str) -> None:
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    qdir = query_dir(engine, stream)
    ldir = log_dir(engine, scale, stream)
    ldir.mkdir(parents=True, exist_ok=True)

    for old_log in ldir.glob("q*.log"):
        old_log.unlink()

    with psycopg.connect(dsn) as conn:
        for i in range(1, 23):
            qpath = qdir / f"{i}.sql"
            if not qpath.exists():
                raise FileNotFoundError(qpath)
            lines: list[str] = []
            with conn.cursor() as cur:
                for idx, statement in enumerate(split_statements(qpath.read_text(encoding="utf-8")), start=1):
                    cur.execute(statement)
                    if cur.description is None:
                        continue
                    columns = [col.name for col in cur.description]
                    rows = cur.fetchall()
                    lines.append(f"-- statement {idx}")
                    lines.append("\t".join(columns))
                    lines.extend("\t".join(str(v) for v in row) for row in rows)
                    lines.append("")
            (ldir / f"q{i}.log").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
            print(f"Executed query {i}")


def execute_starrocks(scale: str, stream: str) -> None:
    pymysql = require_module("pymysql", "pip install pymysql")
    host = os.environ.get("TPCH_STARROCKS_HOST", "127.0.0.1")
    port = int(os.environ.get("TPCH_STARROCKS_PORT", "9030"))
    user = os.environ.get("TPCH_STARROCKS_USER", "root")
    password = os.environ.get("TPCH_STARROCKS_PASSWORD", "")
    database = os.environ.get("TPCH_STARROCKS_DB", "tpch")

    qdir = query_dir("starrocks", stream)
    ldir = log_dir("starrocks", scale, stream)
    ldir.mkdir(parents=True, exist_ok=True)

    for old_log in ldir.glob("q*.log"):
        old_log.unlink()

    with pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        read_timeout=3600,
        write_timeout=3600,
    ) as conn:
        for i in range(1, 23):
            qpath = qdir / f"{i}.sql"
            if not qpath.exists():
                raise FileNotFoundError(qpath)
            lines: list[str] = []
            with conn.cursor() as cur:
                for idx, statement in enumerate(split_statements(qpath.read_text(encoding="utf-8")), start=1):
                    cur.execute(statement)
                    if cur.description is None:
                        continue
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    lines.append(f"-- statement {idx}")
                    lines.append("\t".join(columns))
                    lines.extend("\t".join(str(v) for v in row) for row in rows)
                    lines.append("")
            (ldir / f"q{i}.log").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
            print(f"Executed query {i}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run generated TPC-H query files.")
    parser.add_argument("--engine", required=True, choices=["duckdb", "postgresql", "cedardb", "starrocks"])
    parser.add_argument("--scale", required=True)
    parser.add_argument("--stream", required=True)
    args = parser.parse_args()

    if args.engine == "duckdb":
        execute_duckdb(args.scale, args.stream)
        return

    if args.engine == "postgresql":
        dsn = (
            f"host={os.environ.get('TPCH_PGHOST', '127.0.0.1')} "
            f"port={int(os.environ.get('TPCH_PGPORT', '5432'))} "
            f"dbname={os.environ.get('TPCH_PGDATABASE', 'mydb')} "
            f"user={os.environ.get('TPCH_PGUSER', 'myuser')} "
            f"password={os.environ.get('TPCH_PGPASSWORD', 'mypassword')}"
        )
        execute_psycopg("postgresql", args.scale, args.stream, dsn)
        return

    if args.engine == "cedardb":
        dsn = (
            f"host={os.environ.get('TPCH_CEDAR_HOST', '127.0.0.1')} "
            f"port={int(os.environ.get('TPCH_CEDAR_PORT', '5432'))} "
            f"dbname={os.environ.get('TPCH_CEDAR_DB', 'db')} "
            f"user={os.environ.get('TPCH_CEDAR_USER', 'admin')} "
            f"password={os.environ.get('TPCH_CEDAR_PASSWORD', 'admin')}"
        )
        execute_psycopg("cedardb", args.scale, args.stream, dsn)
        return

    execute_starrocks(args.scale, args.stream)


if __name__ == "__main__":
    main()
