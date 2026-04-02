#!/usr/bin/env python3
from __future__ import annotations

import base64
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path

import pymysql
from pymysql.err import ProgrammingError

FACT_TABLES = {
    "catalog_returns",
    "catalog_sales",
    "inventory",
    "store_returns",
    "store_sales",
    "web_returns",
    "web_sales",
}

CREATE_TABLE_RE = re.compile(
    r"create table\s+([a-z_]+)\s*\((.*?)\);",
    re.IGNORECASE | re.DOTALL,
)
PRIMARY_KEY_RE = re.compile(r"primary key\s*\(([^\)]+)\)", re.IGNORECASE)


@dataclass(frozen=True)
class ColumnDef:
    name: str
    type_name: str
    not_null: bool


@dataclass(frozen=True)
class TableDef:
    name: str
    columns: list[ColumnDef]
    primary_key: list[str]


def default_config() -> dict[str, object]:
    return {
        "mysql_host": os.environ.get("TPCDS_STARROCKS_HOST", "127.0.0.1"),
        "mysql_port": int(os.environ.get("TPCDS_STARROCKS_PORT", "9030")),
        "http_host": os.environ.get("TPCDS_STARROCKS_HTTP_HOST", "127.0.0.1"),
        "http_port": int(os.environ.get("TPCDS_STARROCKS_HTTP_PORT", "8030")),
        "user": os.environ.get("TPCDS_STARROCKS_USER", "root"),
        "password": os.environ.get("TPCDS_STARROCKS_PASSWORD", ""),
        "database": os.environ.get("TPCDS_STARROCKS_DB", "tpcds"),
        "backend_addr": os.environ.get("TPCDS_STARROCKS_BACKEND", "be:9050"),
    }


def mysql_conn(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str | None = None,
    autocommit: bool = True,
) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=autocommit,
        read_timeout=3600,
        write_timeout=3600,
        cursorclass=pymysql.cursors.DictCursor,
    )


def wait_for_frontend(host: str, port: int, user: str, password: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with mysql_conn(host, port, user, password, database=None) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"StarRocks frontend did not become ready: {last_error}")


def ensure_backend(host: str, port: int, user: str, password: str, backend_addr: str) -> None:
    backend_host, backend_port = backend_addr.split(":", 1)
    with mysql_conn(host, port, user, password, database=None) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW BACKENDS")
            rows = cur.fetchall()
            matched = [
                row for row in rows
                if str(row.get("IP", "")) == backend_host
                and str(row.get("HeartbeatPort", row.get("BePort", ""))) == backend_port
            ]
            if not matched:
                try:
                    cur.execute(f"ALTER SYSTEM ADD BACKEND '{backend_addr}'")
                except ProgrammingError as exc:
                    if "Backend already exists" not in str(exc):
                        raise

            for _ in range(90):
                cur.execute("SHOW BACKENDS")
                rows = cur.fetchall()
                for row in rows:
                    same_host = str(row.get("IP", "")) == backend_host
                    same_port = str(row.get("HeartbeatPort", row.get("BePort", ""))) == backend_port
                    if not same_port and not same_host:
                        continue
                    alive = str(row.get("Alive", "")).lower() == "true"
                    status = str(row.get("StatusCode", row.get("Status", "")))
                    if alive and status == "OK":
                        return
                time.sleep(2)
    raise RuntimeError(f"StarRocks backend {backend_addr} did not become healthy")


def authorization_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def parse_tpcds_schema(ddl_path: str | Path) -> list[TableDef]:
    ddl_text = Path(ddl_path).read_text(encoding="utf-8")
    tables: list[TableDef] = []
    for table_name, block in CREATE_TABLE_RE.findall(ddl_text):
        columns: list[ColumnDef] = []
        primary_key: list[str] = []
        for raw_line in block.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            pk_match = PRIMARY_KEY_RE.fullmatch(line)
            if pk_match:
                primary_key = [column.strip() for column in pk_match.group(1).split(",") if column.strip()]
                continue
            column_sql = re.sub(r"\bnot null\b", "", line, flags=re.IGNORECASE).strip()
            column_name, type_name = column_sql.split(None, 1)
            columns.append(
                ColumnDef(
                    name=column_name,
                    type_name=type_name,
                    not_null=bool(re.search(r"\bnot null\b", line, flags=re.IGNORECASE)),
                )
            )
        if not columns:
            continue
        tables.append(TableDef(name=table_name, columns=columns, primary_key=primary_key))
    return tables


def starrocks_type(type_name: str, column_name: str) -> str:
    normalized = type_name.strip().lower()
    if normalized == "integer":
        return "INT"
    if normalized == "bigint":
        return "BIGINT"
    if normalized == "date":
        return "DATE"
    if normalized == "time":
        return "VARCHAR(16)"
    if normalized.startswith("decimal"):
        return normalized.upper()
    if normalized.startswith("varchar"):
        return normalized.upper()
    if normalized.startswith("char"):
        return normalized.upper()
    raise ValueError(f"Unsupported TPC-DS type for {column_name}: {type_name}")


def bucket_count(table_name: str) -> int:
    return 8 if table_name in FACT_TABLES else 1


def create_table_sql(table: TableDef) -> str:
    key_columns = [table.columns[0].name]
    key_sql = ", ".join(key_columns)
    column_lines = []
    for column in table.columns:
        nullable = " NOT NULL" if column.not_null else ""
        column_lines.append(f"    {column.name} {starrocks_type(column.type_name, column.name)}{nullable}")
    return (
        f"CREATE TABLE {table.name} (\n"
        + ",\n".join(column_lines)
        + "\n)\n"
        + "ENGINE=OLAP\n"
        + f"DUPLICATE KEY({key_sql})\n"
        + f"DISTRIBUTED BY HASH({key_sql}) BUCKETS {bucket_count(table.name)}\n"
        + 'PROPERTIES ("replication_num" = "1")'
    )