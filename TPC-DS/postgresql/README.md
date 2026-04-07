# TPC-DS PostgreSQL

PostgreSQL uses the shared TPC-DS pipeline under `TPC-DS/run.sh`.

## Run

From repository root:

```bash
./TPC-DS/run.sh pipeline --engine postgresql --scale 1 --stream 1
```

## Outputs

- Shared data: `TPC-DS/data/sf1`
- Generated queries: `TPC-DS/queries/postgresql/sf1/stream1`
- Query summary: `TPC-DS/logs/postgresql/sf1/stream1/query_summary.json`

## Default Connection

- Host: `127.0.0.1`
- Port: `5432`
- Database: `mydb`
- User: `myuser`
- Password: `mypassword`

Environment variables:

- `TPCDS_PGHOST`
- `TPCDS_PGPORT`
- `TPCDS_PGDATABASE`
- `TPCDS_PGUSER`
- `TPCDS_PGPASSWORD`
