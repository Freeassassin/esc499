with open('/home/farbod/benchmark/augment_queries_final.py', 'r') as f:
    content = f.read()

import re
content = re.sub(
    r"    if path_str in metadata_files:",
    r"    if ';' in bare_query:\n        wrapper = bare_query + ';\n'\n    elif path_str in metadata_files:",
    content
)

with open('/home/farbod/benchmark/augment_queries_final.py', 'w') as f:
    f.write(content)
