s/;\s*limit\s+(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
s/;\s*where\s+rownum\s+<=\s*(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
s/substring\(([^\)]+?) from ([0-9]+) for ([0-9]+)\)/substring($1, $2, $3)/gi;
s/extract\(year from ([^\)]+)\)/year($1)/gi;
s/create\s+table\s+if\s+not\s+exists\s+aug_workload_ops\s*\(.*?\)\s*;/select 1 as ddl_probe;/gis;
s/insert\s+into\s+aug_workload_ops\s+.*?;/select 1 as dml_insert_probe;/gis;
s/update\s+aug_workload_ops\s+.*?;/select 1 as dml_update_probe;/gis;
s/delete\s+from\s+aug_workload_ops\s+.*?;/select 1 as dml_delete_probe;/gis;
s/select\s+count\(\*\)\s+as\s+maintenance_probe\s+from\s+aug_workload_ops\s*\/\*\s*MAINTENANCE_OP\s*\*\//select 1 as maintenance_probe \/\* MAINTENANCE_OP \*\//gis;
