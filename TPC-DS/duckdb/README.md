# TPC-DS DuckDB

DuckDB uses the shared TPC-DS pipeline under `TPC-DS/run.sh`.

## Run

From repository root:

```bash
./TPC-DS/run.sh pipeline --engine duckdb --scale 1 --stream 1
```

## Outputs

- Shared data: `TPC-DS/data/sf1`
- Generated queries: `TPC-DS/queries/duckdb/sf1/stream1`
- DuckDB database file: `TPC-DS/logs/duckdb/sf1/tpcds_sf1.duckdb`
- Query summary: `TPC-DS/logs/duckdb/sf1/stream1/query_summary.json`
