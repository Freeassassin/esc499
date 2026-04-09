# Benchmark Execution Engine

This document describes the core benchmark execution engine implemented in `benchmark.py`, including the CLI interface, engine-specific query dispatchers, concurrency model, and the benchmark main loop.

---

## 1. CLI Interface

The benchmark is invoked via:

```bash
python benchmark.py --engine <engine> [options]
```

### 1.1 Required Arguments

| Argument | Description |
|:---------|:------------|
| `--engine` | Target database engine: `duckdb`, `postgresql`, `cedardb`, `starrocks` |

### 1.2 Connection Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--host` | `127.0.0.1` | Database host (ignored for DuckDB) |
| `--port` | Engine-specific (see below) | Database port |
| `--user` | Engine-specific | Database username |
| `--password` | Engine-specific | Database password |
| `--database` | Engine-specific | Database name |

**Engine-specific defaults** (applied when `--port` is 0 or unspecified):

| Engine | Port | User | Password | Database |
|:-------|:-----|:-----|:---------|:---------|
| `duckdb` | N/A | N/A | N/A | N/A |
| `postgresql` | 5432 | `myuser` | `mypassword` | `mydb` |
| `cedardb` | 5433 | `admin` | `admin` | `db` |
| `starrocks` | 9030 | `root` | (empty) | `tpch` |

### 1.3 Benchmark Scale Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--scale-factors` | `1,5,10,20,50,100,500,1000,3000,5000` | Comma-separated list of scale factors to test |
| `--concurrency-start` | `1` | Starting concurrency level |
| `--concurrency-end` | `512` | Maximum concurrency level |

### 1.4 Resume Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--resume-from-concurrency` | 0 | Skip concurrency levels below this value |
| `--resume-from-sf` | 0 | Skip scale factors below this at the first concurrency level |

These enable resuming a benchmark run after interruption without repeating completed (concurrency, SF) combinations.

### 1.5 Stream and Randomisation Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--tpch-stream` | `1` | TPC-H query stream ID (determines parameter substitutions) |
| `--tpcds-stream` | `1` | TPC-DS query stream ID |
| `--seed` | `42` | RNG seed for per-user query shuffle |

### 1.6 Engine-Specific Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--duckdb-threads` | `4` | Number of DuckDB worker threads per query |
| `--query-timeout` | `3600` | Per-query timeout in seconds |

### 1.7 Output Options

| Argument | Default | Description |
|:---------|:--------|:------------|
| `--output` | `benchmark_results.json` | Path for JSON results output |
| `-v` / `--verbose` | Off | Enable DEBUG-level logging |

---

## 2. Engine Configuration

The `EngineConfig` dataclass encapsulates all connection parameters:

```python
@dataclass
class EngineConfig:
    engine: str           # "duckdb" | "postgresql" | "cedardb" | "starrocks"
    host: str             # Database host
    port: int             # Database port
    user: str             # Database username
    password: str         # Database password
    database: str         # Database name
    duckdb_threads: int   # DuckDB PRAGMA threads value
    query_timeout: int    # Per-query timeout (seconds)
```

---

## 3. Query Loading

### 3.1 TPC-H Queries

`load_tpch_queries(engine, stream)` loads 22 queries from:
```
TPC-H/queries/{engine}/{stream}/{1-22}.sql
```

Returns a list of `(query_id, sql_text)` tuples where `query_id` follows the format `tpch-q{N}`.

### 3.2 TPC-DS Queries

`load_tpcds_queries(engine, sf, stream)` loads 99 queries from:
```
TPC-DS/queries/{engine}/sf{sf}/stream{stream}/query{1-99}.sql
```

Returns tuples with `query_id` format `tpcds-q{N}`.

### 3.3 Combined Workload

`load_all_queries()` combines both sets into a single list of **121 queries** (22 TPC-H + 99 TPC-DS). The `query_id` prefix (`tpch-` or `tpcds-`) determines which engine dispatcher handles schema routing and SQL normalisation.

---

## 4. Engine-Specific Query Dispatchers

Each database engine has a dedicated query execution function. All share the same return structure:

```python
{
    "query_id": "tpch-q1",      # Query identifier
    "elapsed_sec": 0.234567,    # Wall-clock execution time
    "status": "ok" | "error",   # Success or failure
    "row_count": 100,           # Rows returned (if ok)
    "error": "message"          # Error text (if error)
}
```

### 4.1 DuckDB Dispatcher (`_execute_single_query_duckdb`)

**Connection strategy**: Creates a new **in-memory** DuckDB connection per query. The persistent data database is attached as read-only:

```python
conn = duckdb.connect(":memory:")
conn.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")
```

**View creation**: For each table in the attached database, a lightweight view is created in the in-memory catalog:
```sql
CREATE VIEW main."{table}" AS SELECT * FROM {alias}.main."{table}"
```

This allows unqualified table names in queries to resolve correctly while `CREATE TABLE` and `CREATE TEMPORARY TABLE` statements target the writable in-memory database.

**Thread control**: `PRAGMA threads={config.duckdb_threads}` (default: 4)

**SQL normalisation**: TPC-DS queries are normalised via `normalize_sql("duckdb", sql_text)`. TPC-H queries are executed as-is (dialect transformations were applied during generation).

**Statement execution**: Each statement (split on `;`) is executed sequentially. Only the last result set's row count is returned.

### 4.2 PostgreSQL/CedarDB Dispatcher (`_execute_single_query_psycopg`)

**Connection strategy**: Creates a new `psycopg` connection per query with `autocommit=True`:
```python
conn = psycopg.connect(conninfo, autocommit=True)
```

**Schema routing**: Sets `search_path` based on query prefix:
```sql
SET search_path TO {tpch|tpcds}, public
```

**Statement timeout**: If `query_timeout < 3600`:
```sql
SET statement_timeout = '{timeout_ms}'
```

**TPC-H Q15 workaround**: Replaces `CREATE VIEW revenue0` with `CREATE OR REPLACE VIEW revenue0` to prevent conflicts when multiple users execute Q15 concurrently.

**PostgreSQL TPC-DS execution**: For PostgreSQL with TPC-DS queries, the full normalised SQL is executed as a single script, using `cursor.nextset()` to iterate over multiple result sets.

**CedarDB retry logic**: CedarDB may encounter transient serialisation conflicts under concurrent load. The dispatcher implements a **3-attempt retry** strategy:
```python
max_attempts = 3 if engine_label == "cedardb" else 1
```
Retries are triggered when "concurrent" appears in the error message (case-insensitive).

### 4.3 StarRocks Dispatcher (`_execute_single_query_starrocks`)

**Connection strategy**: Creates a new `pymysql` connection per query:
```python
conn = pymysql.connect(host=..., port=..., user=..., password=...,
                       database=..., autocommit=True,
                       read_timeout=query_timeout, write_timeout=query_timeout)
```

**Database routing**: Uses `USE {db}` to switch between `tpch` and `tpcds` databases based on query prefix.

**Query timeout**:
```sql
SET query_timeout = {seconds}
```

**Statement execution**: Each statement is executed individually via the cursor. The same `CREATE OR REPLACE VIEW` workaround is applied for Q15.

### 4.4 Dispatch Routing

The `execute_query()` function routes to the appropriate dispatcher based on `config.engine`:

| `config.engine` | Dispatcher | Driver |
|:----------------|:-----------|:-------|
| `duckdb` | `_execute_single_query_duckdb` | `duckdb` |
| `postgresql` | `_execute_single_query_psycopg` | `psycopg` (v3) |
| `cedardb` | `_execute_single_query_psycopg` | `psycopg` (v3) |
| `starrocks` | `_execute_single_query_starrocks` | `pymysql` |

---

## 5. Concurrency Model

### 5.1 User Simulation (`run_concurrent`)

The `run_concurrent()` function simulates concurrent analytical users:

1. **Thread pool**: Creates a `ThreadPoolExecutor` with `max_workers=concurrency` (1 to 512 users)
2. **Per-user setup**: Each user thread receives:
   - A deterministic RNG seeded with `seed + user_id`
   - A copy of all 121 queries, shuffled using the per-user RNG
3. **Synchronised start**: All user threads wait at a `threading.Barrier(concurrency, timeout=120)` before beginning execution
4. **Execution**: Each user executes all 121 queries **sequentially** in their shuffled order, creating a **new database connection per query**
5. **Result collection**: Results are collected in a thread-safe list protected by a `threading.Lock`

### 5.2 Design Rationale

- **New connection per query**: Models realistic workload where connections are pooled and reused across different queries, rather than long-lived sessions
- **Barrier-synchronised start**: Ensures all users begin simultaneously, creating peak concurrent load
- **Deterministic shuffle**: Using `seed + user_id` ensures reproducible query ordering across runs while ensuring different users execute queries in different orders
- **Sequential per-user execution**: Each user runs queries one after another (not parallel within a user), modelling a typical interactive analyst session

### 5.3 Error Handling

If a user thread fails entirely (e.g., connection failure):
- An error result is recorded for **every** query assigned to that user
- The error message is included in each result record
- Other users continue executing normally

---

## 6. Main Benchmark Loop

### 6.1 Two-Dimensional Search (`run_benchmark`)

The benchmark explores a **concurrency × scale factor** matrix:

```
For each concurrency level C ∈ {1, 2, 4, 8, ..., 512}:
    For each scale factor SF ∈ {1, 5, 10, 20, 50, 100, ..., 5000}:
        1. Generate data & queries (idempotent/cached)
        2. Load data into SUT
        3. Load query SQL texts
        4. Execute C users × 121 queries concurrently
        5. Analyse latency distribution
        6. If bottleneck → record max_sf, advance to next C
```

### 6.2 Per-Run Analysis

After each (C, SF) run:

1. **Latency extraction**: Collect `elapsed_sec` from all query results
2. **Bucket categorisation**: `bucket_latencies()` assigns each latency to one of 9 fleet buckets
3. **Tail comparison**: `is_bottleneck()` compares observed tail percentages against fleet tail thresholds
4. **Error check**: If 100% of queries errored, force bottleneck classification
5. **Logging**: Per-threshold pass/fail status logged with observed vs fleet percentages

### 6.3 Bottleneck Escalation

When a bottleneck is detected at scale factor SF:
- The current SF loop breaks
- `max_sf` is set to the previous (non-bottleneck) SF value
- The benchmark advances to the next concurrency level
- If bottleneck occurs at SF=1 (the minimum), the benchmark stops entirely (SUT cannot meet fleet targets even at minimal data size)

### 6.4 Intermediate Output

After each concurrency level completes, results are written to the output JSON file via `_write_output()`. This provides:
- **Crash recovery**: Resume from the last completed concurrency level
- **Progress monitoring**: Partial results available during long-running benchmarks

### 6.5 Concurrency Level Generation

`build_concurrency_sequence(start, end)` generates powers of 2:
```
start=1, end=512 → [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
```

---

## 7. Output JSON Structure

The benchmark produces a JSON file with the following structure:

```json
{
  "engine": "postgresql",
  "timestamp": "2026-04-09T12:00:00+00:00",
  "seed": 42,
  "config": {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "mydb",
    "scale_factors": [1, 5, 10, 20, 50],
    "concurrency_levels": [1, 2, 4, 8],
    "query_timeout": 3600,
    "duckdb_threads": 4
  },
  "results": [
    {
      "concurrency": 1,
      "max_scale_factor": 50,
      "runs": [
        {
          "scale_factor": 1,
          "concurrency": 1,
          "total_queries": 121,
          "error_count": 0,
          "bottleneck": false,
          "bucket_distribution": {
            "(0s, 10ms]": { "count": 15, "pct": 12.40, "fleet_pct": 13.7 },
            "(10ms, 100ms]": { "count": 60, "pct": 49.59, "fleet_pct": 48.3 },
            ...
          },
          "fleet_comparison": {
            ">1.0s": { "observed_pct": 8.26, "fleet_pct": 13.048, "pass": true },
            ">10.0s": { "observed_pct": 1.65, "fleet_pct": 3.148, "pass": true },
            ...
          },
          "per_query": [
            {
              "query_id": "tpch-q1",
              "elapsed_sec": 0.234567,
              "status": "ok",
              "row_count": 4,
              "user_id": 0
            },
            ...
          ]
        }
      ]
    }
  ]
}
```

### 7.1 Key Fields

| Field | Level | Description |
|:------|:------|:------------|
| `engine` | Top | Database engine identifier |
| `timestamp` | Top | ISO 8601 UTC timestamp of benchmark start |
| `seed` | Top | RNG seed used for query shuffling |
| `results[].concurrency` | Per-concurrency | Concurrency level tested |
| `results[].max_scale_factor` | Per-concurrency | Largest SF before bottleneck (`null` if bottleneck at SF=1) |
| `runs[].bottleneck` | Per-run | Whether fleet tail thresholds were exceeded |
| `runs[].bucket_distribution` | Per-run | Observed latency distribution across 9 buckets |
| `runs[].fleet_comparison` | Per-run | Per-threshold observed vs fleet tail percentages |
| `runs[].per_query` | Per-run | Individual query results with timing |

---

## 8. Quick Validation Harness

`run_all_tests.sh` provides a rapid validation of all four engines at SF=10, concurrency=1:

```bash
# For each engine:
python3 benchmark.py --engine {engine} \
    --scale-factors 10 \
    --concurrency-start 1 --concurrency-end 1 \
    [--query-timeout 300] \
    --output test_{engine}.json -v
```

After all engines complete, an inline Python script analyses the results:
- Counts successful queries (`status == "ok"`) and errors per engine
- Extracts unique error messages (truncated to 80 characters) with occurrence counts
- Prints a summary: `{engine}: {ok} ok, {err} errors` with top error messages

The 300-second query timeout (for PostgreSQL, CedarDB, StarRocks) prevents long-running queries from blocking the validation run.
