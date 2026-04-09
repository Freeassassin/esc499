# Methodology

This document describes the research methodology underpinning the benchmark framework, including the motivation for augmenting standard TPC workloads, the real-world fleet distribution model, the augmentation target selection process, and the overall benchmarking approach.

---

## 1. Motivation

Standard TPC-H and TPC-DS benchmarks are widely used for evaluating analytical database systems. However, research has shown that these benchmarks fail to capture critical characteristics of real-world production workloads. The paper *"Why TPC Is Not Enough: An Analysis of the Amazon Redshift Fleet"* (the **Redset** dataset) demonstrates significant gaps between TPC benchmark queries and the query patterns observed in Amazon Redshift's production fleet.

This project addresses those gaps by:

1. **Augmenting** TPC-H (22 queries) and TPC-DS (99 queries) with real-world workload characteristics identified from the Redset telemetry data
2. **Using LLM-based AI agents** (GitHub Copilot with Claude, Gemini, and OpenAI backends) to perform query augmentation and validate cross-engine compatibility
3. **Benchmarking four analytical database engines** under concurrent load using a fleet-calibrated bottleneck detection model

---

## 2. Fleet Distribution Model

### 2.1 Source Data

The fleet distribution model is derived from the Amazon Redset dataset, which represents real-world query latency distributions across Amazon Redshift's production fleet. The model defines how queries are distributed across latency buckets in a typical production environment.

### 2.2 Latency Buckets

The model partitions query runtimes into **9 logarithmic latency buckets**, each with an expected percentage of queries and runtime contribution:

| Bucket | Lower Bound | Upper Bound | % of Queries | % of Runtime |
|:-------|:------------|:------------|-------------:|-------------:|
| (0s, 10ms] | 0 s | 0.01 s | 13.70% | 0.01% |
| (10ms, 100ms] | 0.01 s | 0.1 s | 48.30% | 0.40% |
| (100ms, 1s] | 0.1 s | 1.0 s | 24.90% | 2.30% |
| (1s, 10s] | 1.0 s | 10.0 s | 9.90% | 7.30% |
| (10s, 1min] | 10.0 s | 60.0 s | 2.20% | 13.30% |
| (1min, 10min] | 60.0 s | 600.0 s | 0.86% | 35.70% |
| (10min, 1h] | 600.0 s | 3600.0 s | 0.08% | 25.20% |
| (1h, 10h] | 3600.0 s | 36000.0 s | 0.008% | 14.30% |
| ≥10h | 36000.0 s | ∞ | 0.00009% | 1.60% |

These values are encoded as the `FLEET_BUCKETS` constant in `benchmark.py` and represent the expected workload shape of a healthy production analytical database.

### 2.3 Tail Latency Thresholds

From the fleet buckets, **cumulative tail thresholds** are pre-computed. For each threshold starting at 1 second, the system calculates the expected fleet percentage of queries that exceed that latency. These thresholds are stored in `FLEET_TAIL_THRESHOLDS` and computed at module load time by `_build_tail_thresholds()`:

| Threshold | Fleet % of queries above threshold |
|:----------|:-----------------------------------|
| > 1 s | 13.048% |
| > 10 s | 3.148% |
| > 60 s | 0.948% |
| > 600 s | 0.088% |
| > 3600 s | 0.00809% |
| > 36000 s | 0.00009% |

### 2.4 Bottleneck Detection Algorithm

The benchmark determines whether a system under test (SUT) is a **bottleneck** at a given scale factor and concurrency level by comparing the observed query latency tail against the fleet model:

1. **Bucket categorisation**: Each observed query latency is assigned to one of the 9 fleet buckets (`bucket_latencies()`)
2. **Tail comparison**: For each tail threshold (1s, 10s, 60s, …), the observed percentage of queries exceeding that threshold is compared to the fleet percentage (`is_bottleneck()`)
3. **Bottleneck declaration**: If the observed tail percentage **exceeds** the fleet tail percentage at **any** threshold, the SUT is declared a bottleneck for that (concurrency, scale factor) combination
4. **Error override**: If 100% of queries result in errors, the run is automatically classified as a bottleneck

This approach means the benchmark does not require all queries to be fast—it requires the overall latency distribution to match or outperform what is observed in a real production fleet.

---

## 3. Augmentation Targets

### 3.1 Derivation

The augmentation targets are derived from a combination of the Redset paper analysis and broader literature on real-world analytical workload characteristics. They quantify the gap between TPC benchmarks and production workloads across 14 dimensions.

### 3.2 Target Table

| Characteristic | TPC-H/DS Value | Real-World Target | Augmentation Strategy |
|:---------------|:---------------|:------------------|:----------------------|
| **Workload Mix** | ~80% SELECT (read-mostly) | 40–60% Read/Write | Introduce INSERT, COPY, DELETE, UPDATE statements |
| **CTAS Statements** | None | ~1.9% of queries, 16.4% of load | Generate CREATE TABLE AS SELECT statements |
| **Metadata Queries** | None | 31% of all statements are SHOW commands | Inject schema and metadata queries |
| **System Maintenance** | None | ~10% of all queries | Add maintenance-related operations |
| **Query Repetition** | None per run | Up to 80% exact repetitions | Large fraction of duplicate queries |
| **Operator Count** | Low (avg 9–20) | 13% have 101–1000 operators | Increase operator counts for query subsets |
| **Expression Depth** | Shallow (<0.1% depth >10) | 12% have nesting depth 11–100 | Deeper nesting in WHERE/SELECT clauses |
| **Join Type** | 97% Inner Joins (TPC-DS) | 37% Outer Joins | Convert INNER JOINs to LEFT/RIGHT/FULL OUTER |
| **Join Key Type** | 81% Numeric Keys (TPC-DS) | 46% Text-based Keys | Use VARCHAR/TEXT columns as join keys |
| **Aggregation Target** | 98% on numeric types | 34% on text columns | Apply COUNT, GROUP BY to non-numeric columns |
| **Aggregation Function** | SUM dominant (64%) | ANYVALUE dominant (58%) | Use wider variety of aggregation functions |
| **Data Skew** | Low/Uniform | High (Q-Error up to 10²³⁹) | Predicates targeting skewed distributions |
| **Data Types** | INT/DATE common | VARCHAR dominant (52.1%) | Queries operating on VARCHAR, TIMESTAMP, BOOLEAN |
| **Null Values** | None (TPC-H) or very few | High frequency (>99% null columns) | Explicit IS NULL, IS NOT NULL, COALESCE predicates |

### 3.3 Observed vs Target (Post-Augmentation)

After augmentation by LLM agents, the combined TPC-DS + TPC-H workload (2,381 total statements from 121 base queries) was measured against targets:

| Characteristic | Observed | Target |
|:---------------|:---------|:-------|
| Read workload share | 69.51% | 40–60% read |
| Write workload share | 30.49% | 40–60% write |
| CTAS share | 1.60% | ~1.9% |
| Metadata query share | 45.78% | ~31% |
| System maintenance share | 10.16% | ~10% |
| Exact repetition share | 94.20% | up to 80% |
| Outer join share | 80.23% | ~37% |
| Text join key share | 52.33% | ~46% |
| Aggregation on text share | 11.76% | ~34% |
| ANYVALUE aggregation share | 5.67% | ~58% |
| Operator-count bucket (101–1000) | 0.00% | ~13% |
| Expression-depth bucket (11–100) | 37.19% | ~12% |
| Statements with null handling | 17.43% | high frequency |
| Statements using string types | 13.48% | VARCHAR-dominant |
| Statements using timestamp | 15.88% | frequent |
| Statements using boolean | 0.63% | frequent |

These metrics are computed by `tools/compare_augmented_targets.py` and stored in `logs/combined/target-comparison.json`.

---

## 4. Database Engine Selection

Four analytical database engines are evaluated, representing different architectural paradigms:

| Engine | Category | Architecture | Version/Image |
|:-------|:---------|:-------------|:--------------|
| **PostgreSQL** | Traditional RDBMS | Row-oriented, single-node | `postgres:16` |
| **DuckDB** | Embedded OLAP | Columnar, in-process | Python `duckdb` module (latest) |
| **CedarDB** | Next-gen HTAP | Columnar, PostgreSQL-compatible | `cedardb/cedardb:latest` |
| **StarRocks** | Distributed MPP | Columnar, shared-nothing | `starrocks/fe-ubuntu:latest` + `starrocks/be-ubuntu:latest` |

**Selection rationale:**

- **PostgreSQL 16** serves as the industry-standard baseline. As the most widely deployed open-source RDBMS, it provides a familiar reference point. Its row-oriented storage and mature query optimizer represent conventional analytical processing.
- **DuckDB** represents the embedded analytical database paradigm. It runs in-process with no server overhead, uses columnar storage, and is designed for interactive analytical workloads. Its inclusion tests scenarios where deployment simplicity is valued.
- **CedarDB** is a modern HTAP (Hybrid Transactional/Analytical Processing) system that exposes a PostgreSQL-compatible wire protocol. It tests whether newer architectural approaches can improve analytical throughput while maintaining compatibility.
- **StarRocks** represents the distributed MPP (Massively Parallel Processing) category. Its frontend/backend separation, hash-distributed OLAP tables, and optimised vectorised execution test the scalability end of the spectrum.

---

## 5. Benchmark Design

### 5.1 Query Workload

Each benchmark run executes **121 queries per simulated user**:
- **22 TPC-H queries** (Q1–Q22): Decision support queries covering operations on 8 tables (lineitem, orders, customer, supplier, part, partsupp, nation, region)
- **99 TPC-DS queries** (Q1–Q99): Broader decision support queries covering 25 tables (fact tables: catalog_sales, store_sales, web_sales, etc.; dimension tables: customer, item, date_dim, etc.)

Each query includes the standard TPC analytical query plus augmented workload blocks (DML operations, metadata queries, maintenance probes) appended by the LLM augmentation process.

### 5.2 Concurrency × Scale Factor Matrix

The benchmark explores a two-dimensional parameter space:

- **Concurrency levels**: Powers of 2 from 1 to 512 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
- **Scale factors**: 1, 5, 10, 20, 50, 100, 500, 1,000, 3,000, 5,000

For each concurrency level, scale factors are tested in ascending order. The **maximum scale factor** before bottleneck is recorded as the system's capacity at that concurrency level. Once a bottleneck is detected, the benchmark advances to the next concurrency level.

### 5.3 User Simulation

Each simulated user:
1. Receives the full set of 121 queries
2. Shuffles them into a random order using a deterministic RNG seeded with `seed + user_id` (default seed: 42)
3. Synchronises with all other users via a `threading.Barrier` (ensuring simultaneous start)
4. Executes all queries sequentially, creating a **new database connection per query**

This models realistic concurrent analytical workloads where multiple users submit different query sequences against a shared database.

### 5.4 Output Structure

For each (concurrency, scale factor) pair, the benchmark records:
- Total query count, error count, bottleneck status
- Per-query results: query ID, elapsed time, status (ok/error), row count, user ID
- Latency bucket distribution (observed vs fleet)
- Fleet tail comparison with pass/fail per threshold

Results are written incrementally to a JSON file after each concurrency level completes, enabling resume from interruption.
