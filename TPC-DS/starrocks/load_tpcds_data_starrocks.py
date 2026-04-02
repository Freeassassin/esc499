#!/usr/bin/env python3
from __future__ import annotations

import argparse
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests

from common import (
    authorization_header,
    default_config,
    ensure_backend,
    mysql_conn,
    parse_tpcds_schema,
    wait_for_frontend,
)


def cleaned_file_path(dat_file: Path) -> Path:
    tmp = NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=f"_{dat_file.name}")
    with dat_file.open("r", encoding="utf-8") as infile, tmp:
        for line in infile:
            fields = line.rstrip("\r\n").split("|")
            if fields and fields[-1] == "":
                fields = fields[:-1]
            normalized = [field if field != "" else r"\N" for field in fields]
            tmp.write("|".join(normalized) + "\n")
    return Path(tmp.name)


def put_stream_load(url: str, headers: dict[str, str], data_path: Path) -> requests.Response:
    with data_path.open("rb") as data_file:
        return requests.put(
            url,
            headers=headers,
            data=data_file,
            allow_redirects=False,
            timeout=7200,
        )


def stream_load(
    table_name: str,
    column_names: list[str],
    dat_file: Path,
    http_host: str,
    http_port: int,
    user: str,
    password: str,
    database: str,
    mysql_host: str,
    mysql_port: int,
) -> None:
    temp_path = cleaned_file_path(dat_file)
    label = f"tpcds_{table_name}_{uuid.uuid4().hex}"
    url = f"http://{http_host}:{http_port}/api/{database}/{table_name}/_stream_load"
    headers = {
        "Authorization": authorization_header(user, password),
        "Expect": "100-continue",
        "column_separator": "|",
        "columns": ",".join(column_names),
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
                raise RuntimeError(f"Stream load redirect missing location for {table_name}")
            response = put_stream_load(redirect_url, headers, temp_path)
        response.raise_for_status()
        payload = response.json()
        if payload.get("Status") not in {"Success", "Publish Timeout"}:
            raise RuntimeError(f"Stream load failed for {table_name}: {payload}")
        with mysql_conn(mysql_host, mysql_port, user, password, database=database) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}")
                row_count = cur.fetchone()["row_count"]
                print(f"loaded:{table_name}:rows={row_count}")
    finally:
        temp_path.unlink(missing_ok=True)


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Bulk-load TPC-DS data into StarRocks")
    parser.add_argument("--ddl", required=True, help="TPC-DS DDL SQL file path")
    parser.add_argument("--data-dir", required=True, help="Directory with *.dat files from dsdgen")
    parser.add_argument("--host", default=defaults["mysql_host"])
    parser.add_argument("--port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--http-host", default=defaults["http_host"])
    parser.add_argument("--http-port", type=int, default=defaults["http_port"])
    parser.add_argument("--user", default=defaults["user"])
    parser.add_argument("--password", default=defaults["password"])
    parser.add_argument("--dbname", default=defaults["database"])
    parser.add_argument("--backend-addr", default=defaults["backend_addr"])
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    wait_for_frontend(args.host, args.port, args.user, args.password)
    ensure_backend(args.host, args.port, args.user, args.password, args.backend_addr)
    tables = parse_tpcds_schema(args.ddl)

    loaded = 0
    for table in tables:
        dat_file = data_dir / f"{table.name}.dat"
        if not dat_file.exists():
            raise FileNotFoundError(dat_file)
        stream_load(
            table.name,
            [column.name for column in table.columns],
            dat_file,
            str(args.http_host),
            int(args.http_port),
            str(args.user),
            str(args.password),
            str(args.dbname),
            str(args.host),
            int(args.port),
        )
        loaded += 1

    print(f"tables_loaded:{loaded}")


if __name__ == "__main__":
    main()