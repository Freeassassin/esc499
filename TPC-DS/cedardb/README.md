# TPC-DS CedarDB

CedarDB uses the shared TPC-DS pipeline under `TPC-DS/run.sh`.

## Run

From repository root:

```bash
./TPC-DS/run.sh pipeline --engine cedardb --scale 1 --stream 1
```

## Outputs

- Shared data: `TPC-DS/data/sf1`
- Generated queries: `TPC-DS/queries/cedardb/sf1/stream1`
- Query summary: `TPC-DS/logs/cedardb/sf1/stream1/query_summary.json`

## Default Connection

- Host: `localhost`
- Port: `5433`
- Database: `db`
- User: `admin`
- Password: `admin`

Environment variables:

- `CEDAR_HOST`
- `CEDAR_PORT`
- `CEDAR_DB`
- `CEDAR_USER`
- `CEDAR_PASS`
