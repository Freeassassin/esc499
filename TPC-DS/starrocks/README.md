# TPC-DS StarRocks Pipeline

This directory contains a repeatable StarRocks SF1 pipeline for TPC-DS.

The pipeline does the following:

1. Starts the StarRocks FE and BE services from the workspace `docker-compose.yml`
2. Builds `dsdgen`, `dsqgen`, and the distribution index
3. Generates SF1 flat files with `dsdgen`
4. Generates SQL with `dsqgen` using `DIALECT=starrocks` and `DIRECTORY=query_templates`
5. Recreates the StarRocks database and translates `tools/tpcds.sql` into `ENGINE=OLAP` tables
6. Loads all TPC-DS `*.dat` files through FE HTTP stream load
7. Executes all 99 generated queries through the FE MySQL-compatible endpoint
8. Writes a JSON execution summary under `TPC-DS/starrocks/work/`

Run it from the workspace root with:

```bash
PYTHON_BIN=/home/farbod/benchmark/.venv/bin/python ./TPC-DS/starrocks/run_sf1_starrocks.sh
```

Optional environment overrides:

```bash
TPCDS_STARROCKS_HOST=127.0.0.1
TPCDS_STARROCKS_PORT=9030
TPCDS_STARROCKS_HTTP_HOST=127.0.0.1
TPCDS_STARROCKS_HTTP_PORT=8030
TPCDS_STARROCKS_USER=root
TPCDS_STARROCKS_PASSWORD=
TPCDS_STARROCKS_DB=tpcds
TPCDS_STARROCKS_BACKEND=be:9050
SCALE=1
SEED=100
```

Artifacts are written under `TPC-DS/starrocks/work/`.