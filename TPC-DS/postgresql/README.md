# TPC-DS PostgreSQL Pipeline

This directory contains a repeatable PostgreSQL pipeline for TPC-DS SF1:

1. Build `dsdgen` and `dsqgen`
2. Generate flat files with `dsdgen`
3. Generate SQL with `dsqgen` using `DIALECT=postgresql` and `DIRECTORY=query_templates`
4. Normalize generated SQL for PostgreSQL-specific syntax gaps not covered by the dialect hook
5. Reset and create the PostgreSQL schema
6. Bulk load all flat files with PostgreSQL `COPY`
7. Run all 99 generated queries and persist results and timings

Run it from the workspace root with:

```bash
/home/farbod/benchmark/.venv/bin/python TPC-DS/postgresql/run_pipeline.py --scale 1
```

Artifacts are written under `TPC-DS/postgresql/generated/sf1/`.