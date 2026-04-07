#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile

import duckdb
import psycopg
import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from starrocks.common import (
    authorization_header,
    default_config,
    ensure_backend,
    mysql_conn,
    parse_tpcds_schema,
    wait_for_frontend,
)

DDL_PATH = ROOT_DIR / "tools" / "tpcds.sql"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def cedar_conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.cedar_host} port={args.cedar_port} "
        f"dbname={args.cedar_dbname} user={args.cedar_user} password={args.cedar_password}"
    )


def clean_data_file(dat_file: Path) -> Path:
    tmp = NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=f"_{dat_file.name}")
    with dat_file.open("r", encoding="utf-8") as infile, tmp:
        for line in infile:
            fields = line.rstrip("\r\n").split("|")
            if fields and fields[-1] == "":
                fields = fields[:-1]
            normalized = [field if field != "" else r"\N" for field in fields]
            tmp.write("|".join(normalized) + "\n")
    return Path(tmp.name)


def load_duckdb(args: argparse.Namespace, data_dir: Path) -> None:
    db_path = ROOT_DIR / "logs" / "duckdb" / f"sf{args.scale}" / f"tpcds_sf{args.scale}.duckdb"
    dat_files = sorted(data_dir.glob("*.dat"))
    if not dat_files:
        raise RuntimeError(f"No .dat files found in {data_dir}")

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={args.threads}")
    loaded = 0
    for dat_file in dat_files:
        table_name = dat_file.stem
        con.execute(
            f"COPY {quote_ident(table_name)} FROM '{dat_file.as_posix()}' "
            "(DELIMITER '|', HEADER FALSE, NULLSTR '', AUTO_DETECT FALSE)"
        )
        loaded += 1
        print(f"loaded:{table_name}")
    con.close()
    print(f"tables_loaded:{loaded}")


def load_cedardb(args: argparse.Namespace, data_dir: Path) -> None:
    dat_files = sorted(data_dir.glob("*.dat"))
    if not dat_files:
        raise RuntimeError(f"No .dat files found in {data_dir}")

    with psycopg.connect(cedar_conninfo(args), autocommit=True) as conn:
        loaded = 0
        for dat_file in dat_files:
            table = dat_file.stem
            copy_sql = (
                f'COPY "{table}" FROM STDIN '
                "WITH (FORMAT CSV, DELIMITER '|', HEADER FALSE, NULL '')"
            )
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:  # type: ignore[arg-type]
                    with dat_file.open("rb") as fh:
                        for raw_line in fh:
                            line = raw_line.rstrip(b"\r\n")
                            if line.endswith(b"|"):
                                line = line[:-1]
                            copy.write(line + b"\n")
            loaded += 1
            print(f"loaded:{table}")

    print(f"tables_loaded:{loaded}")


def pg_conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.pg_host} port={args.pg_port} "
        f"dbname={args.pg_dbname} user={args.pg_user} password={args.pg_password}"
    )


def load_postgresql(args: argparse.Namespace, data_dir: Path) -> None:
    dat_files = sorted(data_dir.glob("*.dat"))
    if not dat_files:
        raise RuntimeError(f"No .dat files found in {data_dir}")

    with psycopg.connect(pg_conninfo(args), autocommit=True) as conn:
        loaded = 0
        for dat_file in dat_files:
            table = dat_file.stem
            copy_sql = (
                f'COPY "{table}" FROM STDIN '
                "WITH (FORMAT CSV, DELIMITER '|', HEADER FALSE, NULL '')"
            )
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:  # type: ignore[arg-type]
                    with dat_file.open("rb") as fh:
                        for raw_line in fh:
                            line = raw_line.rstrip(b"\r\n")
                            if line.endswith(b"|"):
                                line = line[:-1]
                            copy.write(line + b"\n")
            loaded += 1
            print(f"loaded:{table}")

    print(f"tables_loaded:{loaded}")


def put_stream_load(url: str, headers: dict[str, str], data_path: Path) -> requests.Response:
    with data_path.open("rb") as data_file:
        return requests.put(
            url,
            headers=headers,
            data=data_file,
            allow_redirects=False,
            timeout=7200,
        )


def load_starrocks(args: argparse.Namespace, data_dir: Path) -> None:
    wait_for_frontend(args.starrocks_host, args.starrocks_port, args.starrocks_user, args.starrocks_password)
    ensure_backend(
        args.starrocks_host,
        args.starrocks_port,
        args.starrocks_user,
        args.starrocks_password,
        args.starrocks_backend,
    )
    tables = parse_tpcds_schema(DDL_PATH)

    loaded = 0
    for table in tables:
        dat_file = data_dir / f"{table.name}.dat"
        if not dat_file.exists():
            raise FileNotFoundError(dat_file)

        temp_path = clean_data_file(dat_file)
        label = f"tpcds_{table.name}_{uuid.uuid4().hex}"
        url = (
            f"http://{args.starrocks_http_host}:{args.starrocks_http_port}"
            f"/api/{args.starrocks_dbname}/{table.name}/_stream_load"
        )
        headers = {
            "Authorization": authorization_header(args.starrocks_user, args.starrocks_password),
            "Expect": "100-continue",
            "column_separator": "|",
            "columns": ",".join([column.name for column in table.columns]),
            "label": label,
            "null_format": r"\N",
            "strict_mode": "false",
            "timeout": "7200",
        }
        try:
            response = put_stream_load(url, headers, temp_path)
            if response.status_code in {307, 308}:
                redirect_url = response.headers.get("Location")
                if not redirect_url:
                    raise RuntimeError(f"Stream load redirect missing location for {table.name}")
                response = put_stream_load(redirect_url, headers, temp_path)
            response.raise_for_status()
            payload = response.json()
            if payload.get("Status") not in {"Success", "Publish Timeout"}:
                raise RuntimeError(f"Stream load failed for {table.name}: {payload}")
            loaded += 1
            print(f"loaded:{table.name}")
        finally:
            temp_path.unlink(missing_ok=True)

    print(f"tables_loaded:{loaded}")


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Load shared generated TPC-DS data into one engine")
    parser.add_argument("--engine", required=True, choices=["duckdb", "cedardb", "starrocks", "postgresql"])
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--threads", type=int, default=4)

    parser.add_argument("--cedar-host", default=os.environ.get("CEDAR_HOST", "localhost"))
    parser.add_argument("--cedar-port", type=int, default=int(os.environ.get("CEDAR_PORT", "5433")))
    parser.add_argument("--cedar-dbname", default=os.environ.get("CEDAR_DB", "db"))
    parser.add_argument("--cedar-user", default=os.environ.get("CEDAR_USER", "admin"))
    parser.add_argument("--cedar-password", default=os.environ.get("CEDAR_PASS", "admin"))

    parser.add_argument("--pg-host", default=os.environ.get("TPCDS_PGHOST", "127.0.0.1"))
    parser.add_argument("--pg-port", type=int, default=int(os.environ.get("TPCDS_PGPORT", "5432")))
    parser.add_argument("--pg-dbname", default=os.environ.get("TPCDS_PGDATABASE", "mydb"))
    parser.add_argument("--pg-user", default=os.environ.get("TPCDS_PGUSER", "myuser"))
    parser.add_argument("--pg-password", default=os.environ.get("TPCDS_PGPASSWORD", "mypassword"))

    parser.add_argument("--starrocks-host", default=defaults["mysql_host"])
    parser.add_argument("--starrocks-port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--starrocks-http-host", default=defaults["http_host"])
    parser.add_argument("--starrocks-http-port", type=int, default=defaults["http_port"])
    parser.add_argument("--starrocks-user", default=defaults["user"])
    parser.add_argument("--starrocks-password", default=defaults["password"])
    parser.add_argument("--starrocks-dbname", default=defaults["database"])
    parser.add_argument("--starrocks-backend", default=defaults["backend_addr"])

    args = parser.parse_args()

    data_dir = ROOT_DIR / "data" / f"sf{args.scale}"
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Data directory does not exist: {data_dir}. Run generate-data first."
        )

    if args.engine == "duckdb":
        load_duckdb(args, data_dir)
    elif args.engine == "cedardb":
        load_cedardb(args, data_dir)
    elif args.engine == "postgresql":
        load_postgresql(args, data_dir)
    else:
        load_starrocks(args, data_dir)


if __name__ == "__main__":
    main()
