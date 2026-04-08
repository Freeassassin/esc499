import glob
from pathlib import Path
import re
import random

random.seed(42)

tpch = sorted(glob.glob("TPC-H/dbgen/queries/*.sql"))
tpcds = sorted(glob.glob("TPC-DS/query_templates/query*.tpl"))
all_files = tpch + tpcds

random.shuffle(all_files)

num_files = len(all_files)
metadata_files = all_files[:35]
maint_files = all_files[35:47]
update_files = all_files[47:65]
delete_files = all_files[65:80]
insert_files = all_files[80:95]
ctas_files = all_files[95:102]

def apply_text_agg(text):
    if 'sum(' in text.lower():
        text = re.sub(r'(?i)sum\(', 'max(cast(', text, count=2)
        text = text.replace('max(cast(', 'max(cast(', 1)
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
        m = re.search(r':x\n:o\n', text)
        if m:
            prefix = text[:m.end()]
            query = text[m.end():]
        else:
            prefix = ""
            query = text
    else:
        # Better split for TPC-DS
        matches = list(re.finditer(r'(?i)\bdefine\b.*?;', text, flags=re.DOTALL))
        if matches:
            last_match = matches[-1]
            prefix = text[:last_match.end()] + '\n\n'
            query = text[last_match.end():].lstrip()
        else:
            m = re.search(r'(?i)(with|select)', text)
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

    # Strip trailing semicolon for safe wrapping
    bare_query = query.strip()
    if bare_query.endswith(';'):
        bare_query = bare_query[:-1]

    if path_str in metadata_files:
        if is_ds:
            wrapper = f"SELECT * FROM information_schema.tables CROSS JOIN ( {bare_query} ) subq [_LIMITC];"
        else:
            wrapper = f"SELECT * FROM information_schema.tables CROSS JOIN ( {bare_query} ) subq;"
    elif path_str in maint_files:
        if is_ds:
            wrapper = f"SELECT 1 as maintain_dummy FROM ( {bare_query} ) subq [_LIMITC];"
        else:
            wrapper = f"SELECT 1 as maintain_dummy FROM ( {bare_query} ) subq;"
    elif path_str in update_files:
        if is_ds:
            wrapper = f"UPDATE customer SET c_birth_year = (SELECT count(*) FROM ( {bare_query} ) subq) WHERE c_customer_sk = -1;\n"
        else:
            wrapper = f"UPDATE region SET r_regionkey = (SELECT count(*) FROM ( {bare_query} ) subq) WHERE r_regionkey = -1;\n"
    elif path_str in delete_files:
        if is_ds:
            wrapper = f"DELETE FROM customer WHERE c_customer_sk = (SELECT count(*) FROM ( {bare_query} ) subq) AND c_customer_sk = -1;\n"
        else:
            wrapper = f"DELETE FROM region WHERE r_regionkey = (SELECT count(*) FROM ( {bare_query} ) subq) AND r_regionkey = -1;\n"
    elif path_str in insert_files:
        if is_ds:
            wrapper = f"INSERT INTO customer (c_customer_sk) SELECT count(*) FROM ( {bare_query} ) subq WHERE 1=0;\n"
        else:
            wrapper = f"INSERT INTO region (r_regionkey) SELECT count(*) FROM ( {bare_query} ) subq WHERE 1=0;\n"
    elif path_str in ctas_files:
        if is_ds:
            wrapper = f"CREATE TEMP TABLE t_temp_{random.randint(1,999)} AS SELECT * FROM ( {bare_query} ) subq;\n"
        else:
            wrapper = f"CREATE TEMP TABLE t_temp_{random.randint(1,999)} AS SELECT * FROM ( {bare_query} ) subq;\n"
    else:
        # Standard
        wrapper = bare_query + ';\n'

    f.write_text(prefix + wrapper)

print("Re-augmented cleanly!")
