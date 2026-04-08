#!/bin/bash
cat << 'SQL' > dbgen/queries/99.sql
:x
:o
CREATE ...
) subq;
DELETE FROM tmp WHERE 1=0;
SELECT 1;
:n 10
SQL
cd dbgen
DSS_QUERY=queries ./qgen -s 1 99
