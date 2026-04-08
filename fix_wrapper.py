with open('/home/farbod/benchmark/augment_queries_final.py', 'r') as f:
    text = f.read()

import re
text = text.replace("    if ';' in bare_query:\n        wrapper = bare_query + ';\n'\n    elif path_str in metadata_files:", "    if path_str in metadata_files:")
text = text.replace("    if path_str in metadata_files:", "    if ';' in bare_query:\n        wrapper = bare_query + ';\\n'\n    elif path_str in metadata_files:")

with open('/home/farbod/benchmark/augment_queries_final.py', 'w') as f:
    f.write(text)
