# TPC-DS StarRocks

StarRocks uses the shared TPC-DS pipeline under `TPC-DS/run.sh`.

## Run

From repository root:

```bash
./TPC-DS/run.sh pipeline --engine starrocks --scale 1 --stream 1
```

## Outputs

- Shared data: `TPC-DS/data/sf1`
- Generated queries: `TPC-DS/queries/starrocks/sf1/stream1`
- Query summary: `TPC-DS/logs/starrocks/sf1/stream1/query_summary.json`

## Optional Environment Overrides

```bash
TPCDS_STARROCKS_HOST=127.0.0.1
TPCDS_STARROCKS_PORT=9030
TPCDS_STARROCKS_HTTP_HOST=127.0.0.1
TPCDS_STARROCKS_HTTP_PORT=8030
TPCDS_STARROCKS_USER=root
TPCDS_STARROCKS_PASSWORD=
TPCDS_STARROCKS_DB=tpcds
TPCDS_STARROCKS_BACKEND=be:9050
```