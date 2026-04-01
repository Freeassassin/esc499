# TPC-DS DuckDB Pipeline (SF=1)

This folder provides a repeatable pipeline to:

1. Compile TPC-DS tools.
2. Generate SF=1 data with dsdgen.
3. Generate SQL with dsqgen using `DIALECT=duckdb` and `DIRECTORY=<query_templates>`.
4. Create DuckDB schema.
5. Bulk-load pipe-delimited flat files.
6. Execute all 99 queries in DuckDB (Python).

## Files

- `run_sf1_duckdb.sh`: end-to-end orchestration script.
- `prepare_duckdb_schema.py`: executes DDL (`tools/tpcds.sql`) in DuckDB.
- `load_tpcds_data_duckdb.py`: bulk loads all `*.dat` files with DuckDB `COPY`.
- `run_tpcds_queries_duckdb.py`: runs all 99 generated queries and writes a JSON summary.

## Run

From repository root:

```bash
PYTHON_BIN=/home/farbod/benchmark/.venv/bin/python THREADS=4 ./TPC-DS/duckdb/run_sf1_duckdb.sh
```

## Outputs

The pipeline writes outputs to:

- `TPC-DS/duckdb/work/data_sf1` (generated flat files)
- `TPC-DS/duckdb/work/queries_sf1` (generated SQL stream)
- `TPC-DS/duckdb/work/tpcds_sf1.duckdb` (DuckDB database)
- `TPC-DS/duckdb/work/query_summary_sf1.json` (query execution results)

## Notes

- `duckdb.tpl` was added under `query_templates` and is used by dsqgen via:
  - `-dialect duckdb`
  - `-directory <query_templates_path>`
- DuckDB loads use `COPY ... (DELIMITER '|', NULLSTR '', AUTO_DETECT FALSE)` to match TPC-DS flat-file conventions.
- Query execution applies a minimal compatibility normalization for this TPC-DS template set and DuckDB parser differences.
