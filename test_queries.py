import duckdb
from pathlib import Path
conn = duckdb.connect("TPC-H/duckdb/tpch_sf1.duckdb", read_only=False)
qdir = Path("TPC-H/queries/duckdb/1")
for i in range(1, 23):
    qpath = qdir / f"{i}.sql"
    if not qpath.exists(): continue
    for stmt in qpath.read_text().split(';'):
        stmt = stmt.strip()
        if not stmt: continue
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"Error in Query {i}: {e}")
            print(f"Statement: {stmt}")
