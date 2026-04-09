# System Architecture

This document describes the overall system architecture, infrastructure setup, orchestration patterns, and the software environment used in the benchmark framework.

---

## 1. Hardware Environment

All experiments were conducted on the following hardware:

| Component | Specification |
|:----------|:-------------|
| **CPU** | AMD Ryzen AI 5 340 w/ Radeon 840M (6 cores / 12 threads) |
| **RAM** | 57 GiB |
| **Storage** | 1 TB (ext4 on WSL2 virtual disk) |
| **OS** | Arch Linux (rolling) on WSL2 |
| **Kernel** | 6.6.87.2-microsoft-standard-WSL2 |
| **Docker** | Docker Engine 28.4.0 |
| **Python** | Python 3.14.3 |

The system runs under Windows Subsystem for Linux 2 (WSL2), which provides a full Linux kernel with near-native performance for containerised workloads.

---

## 2. Containerised Database Infrastructure

### 2.1 Docker Compose Orchestration

Three of the four database engines run as Docker containers defined in `docker-compose.yml`. DuckDB operates as an in-process embedded engine requiring no container.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Host Machine (WSL2)                        │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐ │
│  │ PostgreSQL 16│  │   CedarDB    │  │StarRocks │  │   DuckDB   │ │
│  │  (postgres   │  │  (cedardb/   │  │  FE + BE │  │ (embedded, │ │
│  │   :16)       │  │   cedardb)   │  │  (ubuntu │  │  in-process│ │
│  │              │  │              │  │   images)│  │  Python)   │ │
│  │  Port: 5432  │  │  Port: 5433  │  │ FE: 9030 │  │  No port   │ │
│  │              │  │              │  │ BE: 8040 │  │            │ │
│  └──────┬───────┘  └──────┬───────┘  └────┬─────┘  └────────────┘ │
│         │                 │               │                        │
│         ▼                 ▼               ▼                        │
│  ┌──────────┐      ┌──────────┐    ┌──────────┐                   │
│  │postgres_ │      │cedardb-  │    │ fe-meta  │                   │
│  │data      │      │data      │    │be-storage│                   │
│  │(volume)  │      │(volume)  │    │(volumes) │                   │
│  └──────────┘      └──────────┘    └──────────┘                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Service Definitions

#### PostgreSQL 16

| Property | Value |
|:---------|:------|
| Image | `postgres:16` (451 MB) |
| Container | `postgres_db` |
| Port | 5432 → 5432 |
| Credentials | user=`myuser`, password=`mypassword`, database=`mydb` |
| Shared Memory | 1 GB (`shm_size: 1gb`) |
| Volume | `postgres_data:/var/lib/postgresql/data` |
| Restart Policy | `always` |

The elevated shared memory allocation (1 GB) is necessary for PostgreSQL to handle large sort and hash operations during TPC analytical queries.

#### CedarDB

| Property | Value |
|:---------|:------|
| Image | `cedardb/cedardb:latest` (210 MB) |
| Container | `cedardb` |
| Port | 5433 → 5432 (remapped to avoid conflict with PostgreSQL) |
| Credentials | user=`admin`, password=`admin`, database=`db` |
| Volume | `cedardb-data:/var/lib/cedardb` |

CedarDB exposes a PostgreSQL-compatible wire protocol, allowing the benchmark to use the same `psycopg` driver for both PostgreSQL and CedarDB connections with only port and credential differences.

#### StarRocks (Frontend + Backend)

StarRocks uses a separated architecture with a **Frontend (FE)** coordinator node and a **Backend (BE)** compute node:

**Frontend:**

| Property | Value |
|:---------|:------|
| Image | `starrocks/fe-ubuntu:latest` (2.19 GB) |
| Container | `starrocks-fe` |
| Ports | 8030 (HTTP UI), 9030 (MySQL protocol) |
| Coordination | `FE_SERVERS=fe:127.0.0.1:9010` |
| Volume | `fe-meta:/opt/starrocks/fe/meta` |
| Startup | Runs FE daemon, then `tail -f /dev/null` to keep container alive |

**Backend:**

| Property | Value |
|:---------|:------|
| Image | `starrocks/be-ubuntu:latest` (2.94 GB) |
| Container | `starrocks-be` |
| Dependency | Starts after FE (`depends_on: fe`) |
| Port | 8040 |
| Registration | `BE_ADDR=be:9050` registers with FE |
| Volume | `be-storage:/opt/starrocks/be/storage` |
| Startup | Sleeps 10 seconds (waits for FE), then starts BE daemon |

The benchmark connects to StarRocks via the MySQL protocol on port 9030 using `pymysql`. Data loading uses the HTTP Stream Load API on port 8030.

#### DuckDB (Embedded)

DuckDB requires no container or server process. It is accessed via the Python `duckdb` module as an in-process embedded database. Each query creates an in-memory connection that **attaches** the pre-loaded database file as read-only:

- TPC-H data: `TPC-H/duckdb/tpch_sf{scale}.duckdb`
- TPC-DS data: `TPC-DS/logs/duckdb/sf{scale}/tpcds_sf{scale}.duckdb`

Lightweight views are created in the in-memory catalog so that unqualified table names resolve to the attached read-only data, while temporary tables and CTEs still target the writable in-memory database.

### 2.3 Persistent Volumes

All containers use named Docker volumes for data persistence:

| Volume | Service | Mount Point | Purpose |
|:-------|:--------|:------------|:--------|
| `postgres_data` | PostgreSQL | `/var/lib/postgresql/data` | Database storage |
| `cedardb-data` | CedarDB | `/var/lib/cedardb` | Database storage |
| `fe-meta` | StarRocks FE | `/opt/starrocks/fe/meta` | Metadata catalog |
| `be-storage` | StarRocks BE | `/opt/starrocks/be/storage` | Data storage |

---

## 3. Unified Pipeline Orchestration

### 3.1 CLI Pattern

Both TPC-H and TPC-DS implement identical CLI patterns via their respective `run.sh` scripts, providing a consistent interface:

```
./TPC-H/run.sh  <subcommand> [options]
./TPC-DS/run.sh <subcommand> [options]
```

**Subcommands:**

| Subcommand | Description | Key Options |
|:-----------|:------------|:------------|
| `generate-data` | Generate synthetic data files | `--scale N`, `--force` |
| `generate-queries` | Generate engine-specific SQL queries | `--engine E`, `--stream N`, `--scale N` (TPC-DS), `--force` |
| `prepare` | Create schema (TPC-DS only, separate step) | `--engine E`, `--scale N` |
| `load` | Load data into target engine | `--engine E`, `--scale N` |
| `run` | Execute queries and capture results | `--engine E`, `--scale N`, `--stream N` |
| `pipeline` | Run all steps in sequence | `--engine E`, `--scale N` |

### 3.2 Top-Level Orchestrator

`benchmark.py` serves as the top-level orchestrator that coordinates both TPC-H and TPC-DS pipelines. For each (concurrency, scale factor) combination, it:

1. Calls `TPC-H/run.sh generate-data` and `TPC-DS/run.sh generate-data` (idempotent)
2. Calls query generation for both benchmarks
3. Handles schema isolation (moving `customer` tables to separate schemas)
4. Loads data into the target engine
5. Loads query SQL texts from generated files
6. Executes concurrent benchmark runs
7. Analyses results against the fleet distribution model

### 3.3 Schema Isolation Strategy

Both TPC-H and TPC-DS define a `customer` table with different column schemas. To run both benchmarks against the same database instance, the framework implements schema isolation for PostgreSQL and CedarDB:

1. **Before loading**: Drop existing `tpch` and `tpcds` schemas via `_cleanup_isolation_schemas()`
2. **After TPC-H load**: Move `public.customer` → `tpch.customer` via `_isolate_customer_table()`
3. **After TPC-DS load**: Move `public.customer` → `tpcds.customer`
4. **During query execution**: Set `search_path` to `tpch` or `tpcds` schema depending on query prefix

For StarRocks, isolation is achieved via separate databases (`USE tpch` / `USE tpcds`).
For DuckDB, isolation is achieved via separate database files (one per benchmark per scale factor).

---

## 4. Directory Structure

```
benchmark/
├── benchmark.py                    # Main orchestrator (concurrency × scale factor benchmark)
├── graph_results.py                # Visualisation and graph generation
├── run_all_tests.sh                # Quick validation test harness
├── docker-compose.yml              # Container definitions (PostgreSQL, CedarDB, StarRocks)
├── augmentation-targets.md         # 14 real-world workload characteristics and targets
│
├── results_*.json                  # Full benchmark results per engine
├── test_*.json                     # Quick test results per engine
│
├── tools/
│   └── compare_augmented_targets.py  # Augmentation validation and metrics comparison
│
├── augmentation-chat-transcript/   # LLM agent session logs
│   ├── claude.json
│   ├── gemini.json
│   └── openai.json
│
├── graphs/                         # Generated visualisation PNGs
│   ├── concurrency_vs_max_sf.png
│   ├── latency_distribution_sf1.png
│   ├── detail_{engine}.png
│   └── error_summary.png
│
├── logs/
│   └── combined/                   # Augmentation comparison results
│       ├── target-comparison.json
│       └── target-comparison.md
│
├── TPC-H/
│   ├── run.sh                      # Unified TPC-H pipeline CLI
│   ├── dbgen/                      # TPC-H data/query generation toolkit (C source)
│   │   ├── dbgen                   # Compiled data generator binary
│   │   ├── qgen                    # Compiled query generator binary
│   │   └── queries/                # DSS_QUERY template directory (qgen input)
│   ├── data/                       # Generated shared data (data/{scale}/*.tbl)
│   ├── ref_data/                   # Pre-computed reference data (not used in pipeline)
│   ├── tools/
│   │   ├── generate_data.sh        # Data generation with locking and caching
│   │   ├── generate_queries.sh     # Query generation with dialect transforms
│   │   ├── load_data.py            # Engine-specific bulk data loading
│   │   ├── run_queries.py          # Query execution and result capture
│   │   ├── profile_duckdb.py       # DuckDB JSON profiling
│   │   ├── digest_profile.py       # Profile-to-Markdown conversion
│   │   └── query_transforms/       # Per-engine sed/Perl transform rules
│   │       ├── {engine}.sed        # Regex substitutions (interval syntax, etc.)
│   │       └── {engine}.pl         # Perl post-processing (UDFs, DDL replacement)
│   ├── queries/                    # Generated queries: queries/{engine}/{stream}/{N}.sql
│   ├── {engine}/                   # Per-engine DDL schema files
│   │   └── ddl.sql
│   └── logs/                       # Execution logs: logs/{engine}/sf{N}/stream{N}/q{N}.log
│
├── TPC-DS/
│   ├── run.sh                      # Unified TPC-DS pipeline CLI
│   ├── tools/
│   │   ├── generate_data.sh        # dsdgen-based data generation with caching
│   │   ├── generate_queries.sh     # dsqgen per-template query generation
│   │   ├── prepare_schema.py       # Engine-specific DDL execution
│   │   ├── load_data.py            # Engine-specific data loading
│   │   ├── run_queries.py          # Query execution with timing
│   │   ├── pipeline_common.py      # Shared: SQL normalisation, statement loading
│   │   ├── tpcds.sql               # Canonical TPC-DS DDL
│   │   ├── profile_duckdb.py       # DuckDB JSON profiling
│   │   └── digest_profile.py       # Profile Markdown generation
│   ├── query_templates/            # 99 TPC-DS query templates (.tpl files)
│   ├── query_variants/             # 14 alternate query formulations
│   ├── data/                       # Generated data: data/sf{N}/*.dat
│   ├── queries/                    # Generated queries: queries/{engine}/sf{N}/stream{N}/
│   ├── answer_sets/                # Reference answer sets (pipe-delimited, for validation)
│   ├── {engine}/                   # Per-engine support files
│   │   └── common.py (StarRocks)   # OLAP table definition helpers
│   └── logs/                       # Execution logs and DuckDB database files
│
└── docs/                           # This documentation
```

---

## 5. Software Dependencies

### 5.1 Python Packages

All database drivers are **lazily imported** via a `_require()` helper function that provides clear install hints if a package is missing. This avoids hard dependencies and allows the codebase to function in environments where not all drivers are installed.

| Package | Purpose | Install Command |
|:--------|:--------|:----------------|
| `duckdb` | DuckDB embedded engine driver | `pip install duckdb` |
| `psycopg[binary]` | PostgreSQL and CedarDB driver (psycopg v3) | `pip install psycopg[binary]` |
| `pymysql` | StarRocks driver (MySQL protocol) | `pip install pymysql` |
| `matplotlib` | Graph generation (Agg backend) | `pip install matplotlib` |
| `numpy` | Numerical processing for graphs | `pip install numpy` |
| `requests` | HTTP Stream Load for StarRocks data loading | `pip install requests` |

Standard library modules used: `argparse`, `json`, `logging`, `subprocess`, `threading`, `concurrent.futures`, `dataclasses`, `datetime`, `pathlib`, `sys`, `re`, `time`, `random`, `os`.

### 5.2 External Tools

| Tool | Source | Purpose |
|:-----|:-------|:--------|
| `dbgen` | TPC-H reference kit (C source in `TPC-H/dbgen/`) | Generate TPC-H synthetic data |
| `qgen` | TPC-H reference kit (C source in `TPC-H/dbgen/`) | Generate TPC-H query parameter substitutions |
| `dsdgen` | TPC-DS reference kit (C source in `TPC-DS/tools/`) | Generate TPC-DS synthetic data |
| `dsqgen` | TPC-DS reference kit (C source in `TPC-DS/tools/`) | Generate TPC-DS query instances from templates |
| `sed` / `perl` | System utilities | SQL dialect transformations |
| `flock` | Linux utility | Concurrent data generation locking |

### 5.3 Docker Images

| Image | Size | Purpose |
|:------|:-----|:--------|
| `postgres:16` | 451 MB | PostgreSQL 16 server |
| `cedardb/cedardb:latest` | 210 MB | CedarDB server |
| `starrocks/fe-ubuntu:latest` | 2.19 GB | StarRocks Frontend (coordinator) |
| `starrocks/be-ubuntu:latest` | 2.94 GB | StarRocks Backend (compute) |

---

## 6. Configuration Strategy

### 6.1 Engine-Specific Defaults

`benchmark.py` provides sensible defaults per engine, avoiding the need for manual connection configuration in standard deployments:

```
Engine       Port   User     Password    Database
─────────────────────────────────────────────────
duckdb       N/A    N/A      N/A         N/A (file-based)
postgresql   5432   myuser   mypassword  mydb
cedardb      5433   admin    admin       db
starrocks    9030   root     (empty)     tpch
```

### 6.2 Environment Variables

The TPC-H and TPC-DS pipeline tools use environment variables for database connections, enabling flexible configuration without code changes:

**TPC-H**: `TPCH_PGHOST`, `TPCH_PGPORT`, `TPCH_PGUSER`, `TPCH_PGPASSWORD`, `TPCH_PGDATABASE`, `TPCH_CEDAR_HOST`, `TPCH_CEDAR_PORT`, `TPCH_CEDAR_USER`, `TPCH_CEDAR_PASSWORD`, `TPCH_CEDAR_DB`, `TPCH_STARROCKS_HOST`, `TPCH_STARROCKS_PORT`, `TPCH_STARROCKS_USER`, `TPCH_STARROCKS_PASSWORD`, `TPCH_STARROCKS_DB`, `TPCH_DUCKDB_THREADS`, `TPCH_LOAD_JOBS`, `TPCH_CEDAR_LOAD_JOBS`.

**TPC-DS**: Analogous `TPCDS_*` prefixed variables, plus `CEDAR_*` and `STARROCKS_*` variants.

### 6.3 Idempotency and Caching

All generation steps implement idempotency checks:

- **TPC-H data**: `.done` marker file in `TPC-H/data/{scale}/` with `metadata.txt` containing row counts
- **TPC-DS data**: `.generated.json` marker file in `TPC-DS/data/sf{scale}/` with scale, seed, timestamp, file count
- **Generated queries**: `.generated.json` markers in query output directories
- **Force regeneration**: `--force` flag bypasses all caches

Concurrent data generation for the same scale factor is protected by `flock(2)` file locking (TPC-H: `data/.locks/sf_{scale}.lock`; TPC-DS: similar lock file mechanism).
