# TPC-DS Unified Pipeline

TPC-DS now uses one shared pipeline for DuckDB, CedarDB, StarRocks, and PostgreSQL.

## Commands

Run from repository root:

```bash
# Generate shared SF1 data once and cache it
./TPC-DS/run.sh generate-data --scale 1

# Generate engine-specific queries
./TPC-DS/run.sh generate-queries --engine duckdb --scale 1 --stream 1

# Full end-to-end pipeline for one engine
./TPC-DS/run.sh pipeline --engine duckdb --scale 1 --stream 1
./TPC-DS/run.sh pipeline --engine cedardb --scale 1 --stream 1
./TPC-DS/run.sh pipeline --engine starrocks --scale 1 --stream 1
./TPC-DS/run.sh pipeline --engine postgresql --scale 1 --stream 1
```

## Design

- Shared flat files are written under `TPC-DS/data/sf{scale}`.
- Engine queries are written under `TPC-DS/queries/{engine}/sf{scale}/stream{n}`.
- Query summaries are written under `TPC-DS/logs/{engine}/sf{scale}/stream{n}/query_summary.json`.
- `dsdgen` runs once per scale factor unless `--force` is used.
- The only executable entrypoint is `TPC-DS/run.sh`.

## Environment

Common knobs:

- `SCALE`, `STREAM`, `SEED`, `THREADS`
- `TPCDS_EXTRA_CFLAGS` (default `-w`) used to suppress make/compiler warning output

CedarDB defaults:

- Host: `localhost`
- Port: `5433`
- User: `admin`
- Password: `admin`
- Database: `db`

StarRocks defaults use:

- `TPCDS_STARROCKS_HOST` (default `127.0.0.1`)
- `TPCDS_STARROCKS_PORT` (default `9030`)
- `TPCDS_STARROCKS_HTTP_HOST` (default `127.0.0.1`)
- `TPCDS_STARROCKS_HTTP_PORT` (default `8030`)
- `TPCDS_STARROCKS_USER` (default `root`)
- `TPCDS_STARROCKS_PASSWORD` (default empty)
- `TPCDS_STARROCKS_DB` (default `tpcds`)
- `TPCDS_STARROCKS_BACKEND` (default `be:9050`)

PostgreSQL defaults use:

- `TPCDS_PGHOST` (default `127.0.0.1`)
- `TPCDS_PGPORT` (default `5432`)
- `TPCDS_PGDATABASE` (default `mydb`)
- `TPCDS_PGUSER` (default `myuser`)
- `TPCDS_PGPASSWORD` (default `mypassword`)
