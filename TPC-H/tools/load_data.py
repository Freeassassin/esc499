from __future__ import annotations

import argparse
import base64
import concurrent.futures
import os
import time
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable

TPCH_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = TPCH_ROOT / "data"

TABLE_ORDER = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

TABLE_PHASES = [
    ["region"],
    ["nation"],
    ["supplier", "customer", "part"],
    ["partsupp"],
    ["orders"],
    ["lineitem"],
]

DUCKDB_TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "region": [
        ("r_regionkey", "INTEGER"),
        ("r_name", "VARCHAR"),
        ("r_comment", "VARCHAR"),
    ],
    "nation": [
        ("n_nationkey", "INTEGER"),
        ("n_name", "VARCHAR"),
        ("n_regionkey", "INTEGER"),
        ("n_comment", "VARCHAR"),
    ],
    "supplier": [
        ("s_suppkey", "INTEGER"),
        ("s_name", "VARCHAR"),
        ("s_address", "VARCHAR"),
        ("s_nationkey", "INTEGER"),
        ("s_phone", "VARCHAR"),
        ("s_acctbal", "DECIMAL(15,2)"),
        ("s_comment", "VARCHAR"),
    ],
    "customer": [
        ("c_custkey", "INTEGER"),
        ("c_name", "VARCHAR"),
        ("c_address", "VARCHAR"),
        ("c_nationkey", "INTEGER"),
        ("c_phone", "VARCHAR"),
        ("c_acctbal", "DECIMAL(15,2)"),
        ("c_mktsegment", "VARCHAR"),
        ("c_comment", "VARCHAR"),
    ],
    "part": [
        ("p_partkey", "INTEGER"),
        ("p_name", "VARCHAR"),
        ("p_mfgr", "VARCHAR"),
        ("p_brand", "VARCHAR"),
        ("p_type", "VARCHAR"),
        ("p_size", "INTEGER"),
        ("p_container", "VARCHAR"),
        ("p_retailprice", "DECIMAL(15,2)"),
        ("p_comment", "VARCHAR"),
    ],
    "partsupp": [
        ("ps_partkey", "INTEGER"),
        ("ps_suppkey", "INTEGER"),
        ("ps_availqty", "INTEGER"),
        ("ps_supplycost", "DECIMAL(15,2)"),
        ("ps_comment", "VARCHAR"),
    ],
    "orders": [
        ("o_orderkey", "BIGINT"),
        ("o_custkey", "INTEGER"),
        ("o_orderstatus", "VARCHAR"),
        ("o_totalprice", "DECIMAL(15,2)"),
        ("o_orderdate", "DATE"),
        ("o_orderpriority", "VARCHAR"),
        ("o_clerk", "VARCHAR"),
        ("o_shippriority", "INTEGER"),
        ("o_comment", "VARCHAR"),
    ],
    "lineitem": [
        ("l_orderkey", "BIGINT"),
        ("l_partkey", "INTEGER"),
        ("l_suppkey", "INTEGER"),
        ("l_linenumber", "INTEGER"),
        ("l_quantity", "DECIMAL(15,2)"),
        ("l_extendedprice", "DECIMAL(15,2)"),
        ("l_discount", "DECIMAL(15,2)"),
        ("l_tax", "DECIMAL(15,2)"),
        ("l_returnflag", "VARCHAR"),
        ("l_linestatus", "VARCHAR"),
        ("l_shipdate", "DATE"),
        ("l_commitdate", "DATE"),
        ("l_receiptdate", "DATE"),
        ("l_shipinstruct", "VARCHAR"),
        ("l_shipmode", "VARCHAR"),
        ("l_comment", "VARCHAR"),
    ],
}

STARROCKS_COLUMNS: dict[str, list[str]] = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
    "customer": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
    "part": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
}


def require_module(module_name: str, install_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(f"Missing Python dependency '{module_name}'. Install with: {install_hint}") from exc


def table_file(data_dir: Path, table: str) -> Path:
    path = data_dir / f"{table}.tbl"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def columns_sql(columns: list[tuple[str, str]]) -> str:
    items = columns + [("_trailing", "VARCHAR")]
    return "{" + ", ".join(f"'{name}': '{dtype}'" for name, dtype in items) + "}"


def apply_ddl_psycopg(ddf_file: Path, dsn: str) -> None:
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(ddf_file.read_text(encoding="utf-8"))


def load_with_psycopg(data_dir: Path, dsn: str, table: str) -> None:
    psycopg = require_module("psycopg", "pip install psycopg[binary]")
    file_path = table_file(data_dir, table)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT text, DELIMITER '|')") as copy:
                with file_path.open("r", encoding="utf-8") as infile:
                    for line in infile:
                        if line.endswith("|\n"):
                            copy.write(line[:-2] + "\n")
                        elif line.endswith("|"):
                            copy.write(line[:-1])
                        else:
                            copy.write(line)
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"Loaded {table}: {cur.fetchone()[0]} rows")


def load_postgresql(data_dir: Path) -> None:
    dsn = (
        f"host={os.environ.get('TPCH_PGHOST', '127.0.0.1')} "
        f"port={int(os.environ.get('TPCH_PGPORT', '5432'))} "
        f"dbname={os.environ.get('TPCH_PGDATABASE', 'mydb')} "
        f"user={os.environ.get('TPCH_PGUSER', 'myuser')} "
        f"password={os.environ.get('TPCH_PGPASSWORD', 'mypassword')}"
    )
    apply_ddl_psycopg(TPCH_ROOT / "postgresql" / "ddl.sql", dsn)
    for phase in TABLE_PHASES:
        if len(phase) == 1:
            load_with_psycopg(data_dir, dsn, phase[0])
            continue
        max_workers = min(int(os.environ.get("TPCH_LOAD_JOBS", "4")), len(phase))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(load_with_psycopg, data_dir, dsn, table) for table in phase]
            for future in concurrent.futures.as_completed(futures):
                future.result()


def load_cedardb(data_dir: Path) -> None:
    dsn = (
        f"host={os.environ.get('TPCH_CEDAR_HOST', '127.0.0.1')} "
        f"port={int(os.environ.get('TPCH_CEDAR_PORT', '5432'))} "
        f"dbname={os.environ.get('TPCH_CEDAR_DB', 'db')} "
        f"user={os.environ.get('TPCH_CEDAR_USER', 'admin')} "
        f"password={os.environ.get('TPCH_CEDAR_PASSWORD', 'admin')}"
    )
    apply_ddl_psycopg(TPCH_ROOT / "cedardb" / "ddl.sql", dsn)
    jobs = int(os.environ.get("TPCH_CEDAR_LOAD_JOBS", "3"))
    for phase in TABLE_PHASES:
        if len(phase) == 1:
            load_with_psycopg(data_dir, dsn, phase[0])
            continue
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(phase))) as pool:
            futures = [pool.submit(load_with_psycopg, data_dir, dsn, table) for table in phase]
            for future in concurrent.futures.as_completed(futures):
                future.result()


def load_duckdb(data_dir: Path, scale: str) -> None:
    duckdb = require_module("duckdb", "pip install duckdb")
    ddl_file = TPCH_ROOT / "duckdb" / "ddl.sql"
    db_file = TPCH_ROOT / "duckdb" / f"tpch_sf{scale}.duckdb"
    if db_file.exists():
        db_file.unlink()

    conn = duckdb.connect(str(db_file))
    conn.execute(f"PRAGMA threads={int(os.environ.get('TPCH_DUCKDB_THREADS', '4'))}")
    conn.execute("PRAGMA memory_limit='4GB'")
    conn.execute(ddl_file.read_text(encoding="utf-8"))

    for table in TABLE_ORDER:
        file_path = table_file(data_dir, table)
        schema = DUCKDB_TABLE_SCHEMAS[table]
        select_cols = ", ".join(name for name, _ in schema)
        conn.execute(
            f"""
            INSERT INTO {table}
            SELECT {select_cols}
            FROM read_csv(
                '{file_path.as_posix()}',
                delim='|',
                header=false,
                auto_detect=false,
                columns={columns_sql(schema)},
                sample_size=-1,
                ignore_errors=false
            )
            """
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Loaded {table}: {count} rows")

    conn.execute("ANALYZE")
    conn.close()
    print(f"DuckDB database ready at {db_file}")


def mysql_conn(database: str | None = None, autocommit: bool = True):
    pymysql = require_module("pymysql", "pip install pymysql")
    return pymysql.connect(
        host=os.environ.get("TPCH_STARROCKS_HOST", "127.0.0.1"),
        port=int(os.environ.get("TPCH_STARROCKS_PORT", "9030")),
        user=os.environ.get("TPCH_STARROCKS_USER", "root"),
        password=os.environ.get("TPCH_STARROCKS_PASSWORD", ""),
        database=database,
        autocommit=autocommit,
        read_timeout=3600,
        write_timeout=3600,
    )


def ensure_starrocks_backend() -> None:
    backend_addr = os.environ.get("TPCH_STARROCKS_BACKEND", "be:9050")
    with mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW BACKENDS")
            rows = cur.fetchall()
            if not rows:
                cur.execute(f"ALTER SYSTEM ADD BACKEND '{backend_addr}'")
            for _ in range(60):
                cur.execute("SHOW BACKENDS")
                rows = cur.fetchall()
                if rows and rows[0][8] == "true" and rows[0][-1] == "OK":
                    return
                time.sleep(2)
    raise RuntimeError("StarRocks backend did not become healthy")


def apply_starrocks_ddl() -> None:
    sql = (TPCH_ROOT / "starrocks" / "ddl.sql").read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    with mysql_conn() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def cleaned_file_path(path: Path) -> Path:
    tmp = NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=f"_{path.name}")
    with path.open("r", encoding="utf-8") as infile, tmp:
        for line in infile:
            if line.endswith("|\n"):
                tmp.write(line[:-2] + "\n")
            elif line.endswith("|"):
                tmp.write(line[:-1])
            else:
                tmp.write(line)
    return Path(tmp.name)


def starrocks_auth_header() -> str:
    user = os.environ.get("TPCH_STARROCKS_USER", "root")
    password = os.environ.get("TPCH_STARROCKS_PASSWORD", "")
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def put_stream_load(url: str, headers: dict[str, str], data_path: Path):
    requests = require_module("requests", "pip install requests")
    with data_path.open("rb") as data_file:
        return requests.put(url, headers=headers, data=data_file, allow_redirects=False, timeout=7200)


def load_starrocks_table(database: str, data_dir: Path, table: str) -> None:
    input_path = table_file(data_dir, table)
    tmp_path = cleaned_file_path(input_path)
    label = f"tpch_{table}_{uuid.uuid4().hex}"
    http_host = os.environ.get("TPCH_STARROCKS_HTTP_HOST", "127.0.0.1")
    http_port = int(os.environ.get("TPCH_STARROCKS_HTTP_PORT", "8030"))
    url = f"http://{http_host}:{http_port}/api/{database}/{table}/_stream_load"
    headers = {
        "label": label,
        "column_separator": "|",
        "columns": ",".join(STARROCKS_COLUMNS[table]),
        "Authorization": starrocks_auth_header(),
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

        with mysql_conn(database) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"Loaded {table}: {cur.fetchone()[0]} rows")
    finally:
        tmp_path.unlink(missing_ok=True)


def load_starrocks(data_dir: Path) -> None:
    database = os.environ.get("TPCH_STARROCKS_DB", "tpch")
    ensure_starrocks_backend()
    apply_starrocks_ddl()
    for table in TABLE_ORDER:
        load_starrocks_table(database, data_dir, table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load shared TPC-H data into an engine.")
    parser.add_argument("--engine", required=True, choices=["duckdb", "postgresql", "cedardb", "starrocks"])
    parser.add_argument("--scale", required=True)
    args = parser.parse_args()

    data_dir = DATA_ROOT / args.scale
    marker = data_dir / ".done"
    if not marker.exists():
        raise FileNotFoundError(f"Missing shared data for SF={args.scale}: {data_dir}")

    if args.engine == "duckdb":
        load_duckdb(data_dir, args.scale)
    elif args.engine == "postgresql":
        load_postgresql(data_dir)
    elif args.engine == "cedardb":
        load_cedardb(data_dir)
    else:
        load_starrocks(data_dir)


if __name__ == "__main__":
    main()
