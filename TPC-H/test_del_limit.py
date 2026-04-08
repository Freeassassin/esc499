import duckdb
conn = duckdb.connect()
conn.execute("CREATE TEMP TABLE t AS SELECT 1 AS a")
conn.execute("DELETE FROM t WHERE 1=0 limit 10")
