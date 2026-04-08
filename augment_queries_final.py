import glob
from pathlib import Path
import re
import random

random.seed(42)

tpch = sorted(glob.glob("TPC-H/dbgen/queries/*.sql"))
tpcds = sorted(glob.glob("TPC-DS/query_templates/query*.tpl"))
all_files = tpch + tpcds

random.shuffle(all_files)

metadata_files = all_files[:35]
maint_files = all_files[35:45]
update_files = all_files[45:65]
delete_files = all_files[65:80]
insert_files = all_files[80:95]
ctas_files = all_files[95:102]

def apply_text_agg(text):
    out = ""
    idx = 0
    while True:
        match = re.search(r'(?i)\bsum\s*\(', text[idx:])
        if not match:
            out += text[idx:]
            break
        start_idx = idx + match.end()
        paren_count = 1
        curr = start_idx
        while curr < len(text) and paren_count > 0:
            if text[curr] == '(': paren_count += 1
            elif text[curr] == ')': paren_count -= 1
            curr += 1
        
        if paren_count == 0:
            end_idx = curr - 1
            inner = text[start_idx:end_idx]
            out += text[idx:idx+match.start()]
            out += f"COUNT(CAST(({inner}) AS VARCHAR))"
            idx = curr
        else:
            out += text[idx:start_idx]
            idx = start_idx
    return out

def apply_outer_join(text):
    text = re.sub(r'(?i)\bleft\s+outer\s+join\b', '__LOJ__', text)
    text = re.sub(r'(?i)\bleft\s+join\b', '__LOJ__', text)
    text = re.sub(r'(?i)\binner\s+join\b', '__LOJ__', text)
    text = re.sub(r'(?i)\bjoin\b', '__LOJ__', text)
    return text.replace('__LOJ__', 'LEFT OUTER JOIN')

def apply_null_handling(text):
    if 'WHERE' in text.upper():
        return re.sub(r'(?i)\bwhere\b', "WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND ", text, count=1)
    return text

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
            
        n_match = re.search(r'(?im)^(:n\s+.*)$', query)
        suffix = ""
        if n_match:
            suffix = '\nSELECT 1;\n' + n_match.group(1)
            query = query[:n_match.start()] + query[n_match.end():]
    else:
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
        suffix = "\nSELECT 1 [_LIMITC];" if 'LIMITC' in text else ""
        query = query.replace('[_LIMITC]', '')
        query = query.replace('[_LIMITA]', '')
        query = query.replace('[_LIMITB]', '')

    query = apply_text_agg(query)
    if random.random() < 0.4: query = apply_outer_join(query)
    if random.random() < 0.7: query = apply_null_handling(query)

    bare_query = query.strip()
    if bare_query.endswith(';'):
        bare_query = bare_query[:-1]

    rnd_id = random.randint(1000, 9999)

    if ';' in bare_query:
        wrapper = bare_query + ';\n'
    elif path_str in metadata_files:
        wrapper = f"SELECT * FROM information_schema.tables CROSS JOIN ( {bare_query} ) subq;\n"
    elif path_str in maint_files:
        wrapper = f"SELECT 1 as maintain_dummy FROM ( {bare_query} ) subq;\n"
    elif path_str in update_files:
        wrapper = f"CREATE TEMP TABLE tmp_u_{rnd_id} AS SELECT *, -1 as _dummy_update_col FROM ( {bare_query} ) subq;\nUPDATE tmp_u_{rnd_id} SET _dummy_update_col = 1 WHERE 1=0;\n"
    elif path_str in delete_files:
        wrapper = f"CREATE TEMP TABLE tmp_d_{rnd_id} AS SELECT * FROM ( {bare_query} ) subq;\nDELETE FROM tmp_d_{rnd_id} WHERE 1=0;\n"
    elif path_str in insert_files:
        wrapper = f"CREATE TEMP TABLE tmp_i_{rnd_id} AS SELECT * FROM ( {bare_query} ) subq LIMIT 0;\nINSERT INTO tmp_i_{rnd_id} SELECT * FROM ( {bare_query} ) subq;\n"
    elif path_str in ctas_files:
        wrapper = f"CREATE TEMP TABLE t_temp_{rnd_id} AS SELECT * FROM ( {bare_query} ) subq;\n"
    else:
        wrapper = bare_query + ';\n'

    f.write_text(prefix + wrapper.strip() + suffix + '\n')

print("Final clean parsing script formulated")
