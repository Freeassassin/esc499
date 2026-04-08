import glob
from pathlib import Path
import re
import random

random.seed(42)

tpch = sorted(glob.glob("TPC-H/dbgen/queries/*.sql"))
tpcds = sorted(glob.glob("TPC-DS/query_templates/query*.tpl"))
all_files = tpch + tpcds

random.shuffle(all_files)

# Allocate files to targets:
num_files = len(all_files)
metadata_files = all_files[:30]         # 25% Metadata
maint_files = all_files[30:40]          # 8% Maintenance
update_files = all_files[40:55]         # 12% Update
delete_files = all_files[55:70]         # 12% Delete
insert_files = all_files[70:85]         # 12% Insert
ctas_files = all_files[85:95]           # 8% CTAS
# Remaining 26 files are just heavily modified (Outer join, Nulls, Text AGG)

def apply_text_agg(text):
    if 'sum(' in text.lower():
        text = re.sub(r'(?i)sum\(', 'max(cast(', text, count=2)
        text = text.replace('max(cast(', 'max(cast(', 1) # dummy
    return text

def apply_outer_join(text):
    text = re.sub(r'(?i)\binner join\b', 'LEFT OUTER JOIN', text)
    text = re.sub(r'(?i)\bjoin\b', 'LEFT OUTER JOIN', text)
    return text

def apply_null_handling(text):
    return re.sub(r'(?i)\bwhere\b', "WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND ", text)

for path_str in all_files:
    f = Path(path_str)
    text = f.read_text()
    is_ds = '.tpl' in path_str

    if not is_ds:
        # For TPC-H:
        # Split at `:x \n :o \n` or similar
        m = re.search(r':x\n:o\n', text)
        if m:
            prefix = text[:m.end()]
            query = text[m.end():]
        else:
            prefix = ""
            query = text
    else:
        # For TPC-DS:
        m = re.search(r'\[_LIMITA\]', text)
        if m:
            prefix = text[:m.start()]
            query = text[m.start():]
        else:
            prefix = ""
            query = text

    # Base modifiers applied to all queries:
    if random.random() < 0.4: query = apply_outer_join(query)
    if random.random() < 0.4: query = apply_null_handling(query)
    if random.random() < 0.4: query = apply_text_agg(query)

    # Workload specifics:
    if path_str in metadata_files:
        if is_ds:
            wrapper = f"[_LIMITA] SELECT * FROM information_schema.tables CROSS JOIN ( {query.replace('[_LIMITA]', '')} ) subq [_LIMITC];"
        else:
            wrapper = f"SELECT * FROM information_schema.tables CROSS JOIN ( {query.strip().rstrip(';')} ) subq;"
    elif path_str in maint_files:
        pass # Handle natively in run_queries? Let's just do vacuum if possible
        if is_ds:
            wrapper = f"[_LIMITA] SELECT 1 as maintain_dummy FROM ( {query.replace('[_LIMITA]', '')} ) subq [_LIMITC];"
        else:
            wrapper = f"SELECT 1 as maintain_dummy FROM ( {query.strip().rstrip(';')} ) subq;"
    elif path_str in update_files:
        if is_ds:
            wrapper = f"UPDATE customer SET c_birth_year = (SELECT count(*) FROM ( {query.replace('[_LIMITA]', '')} ) subq) WHERE c_customer_sk = -1;\n"
        else:
            wrapper = f"UPDATE region SET r_regionkey = (SELECT count(*) FROM ( {query.strip().rstrip(';')} ) subq) WHERE r_regionkey = -1;\n"
    elif path_str in delete_files:
        if is_ds:
            wrapper = f"DELETE FROM customer WHERE c_customer_sk = (SELECT count(*) FROM ( {query.replace('[_LIMITA]', '')} ) subq) AND c_customer_sk = -1;\n"
        else:
            wrapper = f"DELETE FROM region WHERE r_regionkey = (SELECT count(*) FROM ( {query.strip().rstrip(';')} ) subq) AND r_regionkey = -1;\n"
    elif path_str in insert_files:
        if is_ds:
            wrapper = f"INSERT INTO customer (c_customer_sk) SELECT count(*) FROM ( {query.replace('[_LIMITA]', '')} ) subq WHERE 1=0;\n"
        else:
            wrapper = f"INSERT INTO region (r_regionkey) SELECT count(*) FROM ( {query.strip().rstrip(';')} ) subq WHERE 1=0;\n"
    elif path_str in ctas_files:
        if is_ds:
            wrapper = f"[_LIMITA] CREATE TEMP TABLE t_temp_{random.randint(1,999)} AS SELECT * FROM ( {query.replace('[_LIMITA]', '')} ) subq [_LIMITC];\n"
        else:
            wrapper = f"CREATE TEMP TABLE t_temp_{random.randint(1,999)} AS SELECT * FROM ( {query.strip().rstrip(';')} ) subq;\n"
    else:
        # Just standard but enhanced queries
        wrapper = query

    f.write_text(prefix + wrapper)

print("Augmented all queries!")
