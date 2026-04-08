#!/bin/bash
cat << 'SQL' > dbgen/queries/99.sql
:x
:o
SELECT * FROM ( select 1 as a
:n 10
) subq;
SQL
cd dbgen
./qgen -s 1 99
