from __future__ import annotations

import os
from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DBGEN_DIR = REPO_ROOT / "TPC-H" / "dbgen"
DDL_FILE = SCRIPT_DIR / "ddl.sql"
DB_FILE = SCRIPT_DIR / "tpch_sf1.duckdb"
THREADS = int(os.environ.get("TPCH_DUCKDB_THREADS", "4"))

TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
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

LOAD_ORDER = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]


def columns_sql(columns: list[tuple[str, str]]) -> str:
    items = columns + [("_trailing", "VARCHAR")]
    return "{" + ", ".join(f"'{name}': '{dtype}'" for name, dtype in items) + "}"


def load_table(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    file_path = DBGEN_DIR / f"{table}.tbl"
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    select_cols = ", ".join(name for name, _ in TABLE_SCHEMAS[table])
    sql = f"""
        INSERT INTO {table}
        SELECT {select_cols}
        FROM read_csv(
            '{file_path.as_posix()}',
            delim='|',
            header=false,
            auto_detect=false,
            columns={columns_sql(TABLE_SCHEMAS[table])},
            sample_size=-1,
            ignore_errors=false
        )
    """
    conn.execute(sql)
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Loaded {table}: {count} rows")


def main() -> None:
    conn = duckdb.connect(str(DB_FILE))
    conn.execute(f"PRAGMA threads={THREADS}")
    conn.execute("PRAGMA memory_limit='4GB'")
    conn.execute(DDL_FILE.read_text())

    for table in LOAD_ORDER:
        load_table(conn, table)

    conn.execute("ANALYZE")
    conn.close()
    print(f"DuckDB database ready at {DB_FILE}")


if __name__ == "__main__":
    main()
