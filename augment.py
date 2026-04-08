import re
from pathlib import Path
import random

random.seed(42)

# Get all TPCH SQLs and TPC-DS TPLs
tpch_files = list(Path("TPC-H/dbgen/queries").glob("*.sql"))
tpcds_files = list(Path("TPC-DS/query_templates").glob("query*.tpl"))
all_files = sorted(tpch_files + tpcds_files)

# TPC-H files just need plain SQL modifying, TPC-DS need .tpl syntax.
# But DuckDB pipeline for TPC-DS evaluates exactly what is in the output SQL.

# For pure CTAS (target ~2 queries):
ctas_files = all_files[:3]
# For Maintainance (target ~12 queries):
maint_files = all_files[3:15]
# For Metadata (target ~37 queries):
meta_files = all_files[15:52]
# For Read/Write - Update/Delete/Insert (target ~60 queries):
rw_files = all_files[52:112]
# The rest is SELECT.

print(f"Total: {len(all_files)}")

for f in ctas_files:
    text = f.read_text()
    if 'CREATE' not in text.upper():
        f.write_text("CREATE TEMP TABLE t_temp AS\n" + text)

for f in maint_files:
    text = f.read_text()
    if 'VACUUM' not in text.upper():
        # Using a valid SQL statement for duckdb or something else
        # Just appending a maintenance statement at the end so it gets profiled
        f.write_text(text + "\n;\nVACUUM;")

for f in meta_files:
    text = f.read_text()
    if 'INFORMATION_SCHEMA' not in text.upper():
        f.write_text(text + "\n;\nSELECT * FROM information_schema.tables LIMIT 10;")

for f in rw_files:
    text = f.read_text()
    if 'DELETE' not in text.upper():
        f.write_text(text + "\n;\nDELETE FROM region WHERE r_name = 'NONEXISTENT';")
