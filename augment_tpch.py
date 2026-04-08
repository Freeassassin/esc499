import re
from pathlib import Path
import random

queries_dir = Path("TPC-H/dbgen/queries")
for i in range(1, 23):
    q_file = queries_dir / f"{i}.sql"
    if not q_file.exists(): continue
    
    text = q_file.read_text()
    
    # Simple rule: if query is odd, make it a CTAS maybe? 
    # Or let's target specific queries
    
    # Q1: Add varchar aggregation and null checks
    if i == 1:
        text = text.replace("sum(l_quantity) as sum_qty,", "sum(l_quantity) as sum_qty, max(l_shipinstruct) as max_shipinstruct,")
        text = text.replace("where\n", "where\n        l_shipinstruct is not null\n        and ")
    
    # Q2: convert implied inner joins to explicit LEFT OUTER JOIN
    # Wait, Q2 uses comma joins. Let's rewrite it or add a DELETE before it?
    if i == 2:
        text = "DELETE FROM region WHERE r_name = 'NONEXISTENT';\n" + text
        
    # We can inject these things systematically
    q_file.write_text(text)

print("Done")
