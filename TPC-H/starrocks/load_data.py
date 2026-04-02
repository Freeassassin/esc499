from __future__ import annotations

import base64
import os
import time
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile

import pymysql
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DBGEN_DIR = REPO_ROOT / "TPC-H" / "dbgen"
DDL_FILE = SCRIPT_DIR / "ddl.sql"

MYSQL_HOST = os.environ.get("TPCH_STARROCKS_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("TPCH_STARROCKS_PORT", "9030"))
HTTP_HOST = os.environ.get("TPCH_STARROCKS_HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.environ.get("TPCH_STARROCKS_HTTP_PORT", "8030"))
USER = os.environ.get("TPCH_STARROCKS_USER", "root")
PASSWORD = os.environ.get("TPCH_STARROCKS_PASSWORD", "")
DATABASE = os.environ.get("TPCH_STARROCKS_DB", "tpch")
BACKEND_ADDR = os.environ.get("TPCH_STARROCKS_BACKEND", "be:9050")

TABLE_COLUMNS: dict[str, list[str]] = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
    "customer": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
    "part": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
}

TABLE_ORDER = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def mysql_conn(database: str | None = None, autocommit: bool = True) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=USER,
        password=PASSWORD,
        database=database,
        autocommit=autocommit,
        read_timeout=3600,
        write_timeout=3600,
    )


def ensure_backend() -> None:
    with mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW BACKENDS")
            rows = cur.fetchall()
            if not rows:
                cur.execute(f"ALTER SYSTEM ADD BACKEND '{BACKEND_ADDR}'")
            for _ in range(60):
                cur.execute("SHOW BACKENDS")
                rows = cur.fetchall()
                if rows and rows[0][8] == 'true' and rows[0][-1] == 'OK':
                    return
                time.sleep(2)
    raise RuntimeError("StarRocks backend did not become healthy")


def execute_ddl() -> None:
    sql = DDL_FILE.read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql.split(';') if statement.strip()]
    with mysql_conn() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def cleaned_file_path(table: str) -> Path:
    source = DBGEN_DIR / f"{table}.tbl"
    if not source.exists():
        raise FileNotFoundError(source)
    tmp = NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=f"_{table}.tbl")
    with source.open("r", encoding="utf-8") as infile, tmp:
        for line in infile:
            if line.endswith("|\n"):
                tmp.write(line[:-2] + "\n")
            elif line.endswith("|"):
                tmp.write(line[:-1])
            else:
                tmp.write(line)
    return Path(tmp.name)


def authorization_header() -> str:
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def put_stream_load(url: str, headers: dict[str, str], data_path: Path) -> requests.Response:
    with data_path.open("rb") as data_file:
        return requests.put(
            url,
            headers=headers,
            data=data_file,
            allow_redirects=False,
            timeout=7200,
        )


def stream_load(table: str) -> None:
    tmp_path = cleaned_file_path(table)
    label = f"tpch_{table}_{uuid.uuid4().hex}"
    url = f"http://{HTTP_HOST}:{HTTP_PORT}/api/{DATABASE}/{table}/_stream_load"
    headers = {
        "label": label,
        "column_separator": "|",
        "columns": ",".join(TABLE_COLUMNS[table]),
        "Authorization": authorization_header(),
        "Expect": "100-continue",
        "strict_mode": "false",
        "timeout": "7200",
    }

    try:
        response = put_stream_load(url, headers, tmp_path)
        if response.status_code in {307, 308}:
            redirect_url = response.headers.get("Location")
            if not redirect_url:
                raise RuntimeError(f"Stream load redirect missing location for {table}")
            response = put_stream_load(redirect_url, headers, tmp_path)
        response.raise_for_status()
        payload = response.json()
        if payload.get("Status") not in {"Success", "Publish Timeout"}:
            raise RuntimeError(f"Stream load failed for {table}: {payload}")

        with mysql_conn(DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"Loaded {table}: {count} rows")
    finally:
        tmp_path.unlink(missing_ok=True)


def main() -> None:
    ensure_backend()
    execute_ddl()
    for table in TABLE_ORDER:
        stream_load(table)
    print("StarRocks data load complete.")


if __name__ == "__main__":
    main()
