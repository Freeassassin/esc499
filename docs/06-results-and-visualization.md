# Results and Visualisation

This document describes the output artefacts of the benchmark framework, including the result JSON files, the graph generation pipeline, and the augmentation comparison reports.

---

## 1. Benchmark Result Files

### 1.1 Full Benchmark Results

Full multi-concurrency benchmark results are stored as JSON files:

| File | Engine | Description |
|:-----|:-------|:------------|
| `results_duckdb.json` | DuckDB | Full concurrency × SF benchmark results |
| `results_postgresql.json` | PostgreSQL | Full concurrency × SF benchmark results |
| `results_cedardb.json` | CedarDB | Full concurrency × SF benchmark results |
| `results_starrocks.json` | StarRocks | Full concurrency × SF benchmark results |

Each file follows the JSON structure detailed in `05-benchmark-execution.md`, Section 7.

### 1.2 Quick Validation Results

Quick single-point validation results (SF=10, concurrency=1):

| File | Engine |
|:-----|:-------|
| `test_duckdb.json` | DuckDB |
| `test_postgresql.json` | PostgreSQL |

These are produced by `run_all_tests.sh` and are useful for verifying that the pipeline functions correctly before launching full benchmarks.

### 1.3 Per-Query Execution Logs

Individual query results are also captured by the standalone TPC-H and TPC-DS pipelines:

**TPC-H**: `TPC-H/logs/{engine}/sf{scale}/stream{stream}/q{N}.log`
- Tab-separated format with column headers and data rows
- One file per query (1–22)
- Suitable for diff-based result validation against reference data

**TPC-DS**: `TPC-DS/logs/{engine}/sf{scale}/stream{stream}/query_summary.json`
- JSON array with per-query status, timing, row count, and error information:
```json
[
  {"query_id": 1, "file": "query1.sql", "status": "ok", "elapsed_sec": 0.234, "row_count": 100},
  {"query_id": 2, "file": "query2.sql", "status": "error", "elapsed_sec": 0.05, "error": "..."}
]
```

---

## 2. Graph Generation

### 2.1 Tool: `graph_results.py`

The visualisation pipeline is implemented in `graph_results.py`. It uses `matplotlib` (with the non-interactive `Agg` backend) and `numpy` to generate publication-quality PNG graphs from benchmark result JSON files.

**Usage**:
```bash
python graph_results.py results_duckdb.json results_postgresql.json \
    results_cedardb.json results_starrocks.json \
    [--output-dir graphs/]
```

### 2.2 Engine Colour and Marker Scheme

Each engine has a consistent visual identity across all graphs:

| Engine | Colour | Hex Code | Marker |
|:-------|:-------|:---------|:-------|
| DuckDB | Amber/Orange | `#FFC107` | Circle (○) |
| PostgreSQL | Blue | `#336791` | Square (□) |
| CedarDB | Green | `#4CAF50` | Diamond (◇) |
| StarRocks | Magenta | `#E91E63` | Triangle (△) |

### 2.3 Graph Types

The tool generates **4 graph types**, each saved as a PNG file in the output directory (default: `graphs/`).

---

#### Graph 1: Concurrency vs Maximum Scale Factor

**Filename**: `concurrency_vs_max_sf.png`

**Purpose**: The primary benchmark comparison chart. Shows the maximum data scale factor each engine can handle before becoming a bottleneck, at each concurrency level.

**Axes**:
- **X-axis**: Concurrency level (log₂ scale)
- **Y-axis**: Maximum scale factor (symmetric log scale with linear threshold at 1)

**Visual elements**:
- One line per engine with engine-specific colour and marker
- Point annotations showing the SF value (or "✗" if the engine bottlenecked at SF=1)
- Grid lines at all observed concurrency levels

**Interpretation**: Higher lines indicate better performance—the engine can handle larger data volumes at higher concurrency before exceeding fleet latency thresholds. A "✗" marker indicates the engine cannot meet fleet targets even at the smallest data size for that concurrency level.

---

#### Graph 2: Latency Distribution Comparison

**Filename**: `latency_distribution_sf1.png`

**Purpose**: Compares the observed query latency distribution of each engine at SF=1, concurrency=1 against the fleet target distribution.

**Chart type**: 100% stacked horizontal bar chart

**Bars**: One per engine plus one for "Fleet (target)"

**Segments**: 9 latency buckets stacked to show the percentage of queries in each bucket:
- Fast queries (green: `#2ecc71`) → slow queries (dark: `#1a1a2e`)
- Each segment labelled with its percentage of total queries

**Interpretation**: An engine whose distribution closely matches the Fleet bar is well-calibrated to real-world workload patterns. Engines with disproportionate weight in slower buckets indicate analytical performance limitations.

---

#### Graph 3: Per-Engine Detail Tables

**Filename**: `detail_{engine}.png` (one per engine: `detail_duckdb.png`, `detail_postgresql.png`, `detail_cedardb.png`, `detail_starrocks.png`)

**Purpose**: Detailed breakdown of every (concurrency, scale factor) run for a single engine.

**Chart type**: Table visualisation (rendered as a matplotlib table, not a traditional plot)

**Columns**:

| Column | Description |
|:-------|:------------|
| Concurrency | Number of simulated users |
| Scale Factor | Data scale factor |
| Error Count | Number of failed queries |
| Bottleneck | YES (red) or no (green) |
| <1s% | Percentage of queries completing under 1 second |
| 1-10s% | Percentage in the 1–10 second range |
| 10s-1m% | Percentage in the 10 second to 1 minute range |
| 1-10m% | Percentage in the 1–10 minute range |
| >10m% | Percentage exceeding 10 minutes |

**Row colouring**:
- Green (`#ccffcc`): Non-bottleneck run (fleet targets met)
- Red (`#ffcccc`): Bottleneck run (fleet targets exceeded)

**Font**: Small (9pt), centred text for compact presentation.

---

#### Graph 4: Error Summary

**Filename**: `error_summary.png`

**Purpose**: Overview of query success and failure rates across all engines, aggregated over all benchmark runs.

**Chart type**: Grouped bar chart

**Bars per engine**:
- Green bar: Total successful queries across all runs
- Red bar: Total errors across all runs

**Annotations**: Numeric labels on top of each bar showing exact counts.

**Interpretation**: Engines with high error counts may have compatibility issues with augmented query features, suggesting areas where the SQL normalisation layer needs improvement.

---

## 3. Augmentation Comparison Reports

### 3.1 Target Comparison Markdown

**File**: `logs/combined/target-comparison.md`

A human-readable markdown table comparing observed augmentation metrics against real-world targets:

```markdown
| Characteristic | Observed | Target |
|:--|--:|:--|
| Read workload share | 69.51% | 40-60% read |
| Write workload share | 30.49% | 40-60% write |
| CTAS share | 1.60% | ~1.9% |
| Metadata query share | 45.78% | ~31% |
| ...
```

Includes notes on methodology limitations (e.g., data-skew metrics depending on physical data distribution).

### 3.2 Target Comparison JSON

**File**: `logs/combined/target-comparison.json`

Machine-readable version of the comparison data for programmatic analysis:

```json
{
  "statement_metrics": { ... },
  "profile_metrics": { ... },
  "total_queries": { "tpcds": 99, "tpch": 22 },
  "total_statements": 2381,
  "report_path": "..."
}
```

### 3.3 Generation

Both files are produced by `tools/compare_augmented_targets.py`, which:
1. Loads all augmented TPC-DS and TPC-H queries from DuckDB query directories
2. Performs regex-based SQL analysis for statement-level metrics
3. Reads DuckDB JSON profile data for operator and expression depth analysis
4. Computes percentages for all 14 target characteristics
5. Writes both markdown and JSON outputs

---

## 4. Generated Graph Files

The `graphs/` directory contains the following PNG files after a complete benchmark run:

| File | Graph Type | Contents |
|:-----|:-----------|:---------|
| `concurrency_vs_max_sf.png` | Line chart | All engines compared: concurrency vs max SF |
| `latency_distribution_sf1.png` | Stacked bar chart | Latency bucket distributions at SF=1 |
| `detail_duckdb.png` | Detail table | DuckDB per-run breakdown |
| `detail_postgresql.png` | Detail table | PostgreSQL per-run breakdown |
| `detail_cedardb.png` | Detail table | CedarDB per-run breakdown |
| `detail_starrocks.png` | Detail table | StarRocks per-run breakdown |
| `error_summary.png` | Grouped bar chart | Success/error counts per engine |

These files are suitable for direct inclusion in a LaTeX thesis document via `\includegraphics{}`.

---

## 5. Answer Sets (TPC-DS)

Pre-computed reference answer sets for TPC-DS queries are stored in `TPC-DS/answer_sets/`:

- Format: Pipe-delimited text with a header row
- Files: `{query_number}.ans` or `{query_number}_{NULLS_FIRST|NULLS_LAST}.ans` for queries where NULL ordering affects results
- Coverage: All 99 TPC-DS queries
- Usage: Available for manual or external validation; not currently automated in the benchmark pipeline

The `_NULLS_FIRST` / `_NULLS_LAST` variants exist because different database engines handle NULL sort ordering differently (PostgreSQL defaults to NULLS LAST for ASC, while other engines may default to NULLS FIRST).

---

## 6. Reference Data (TPC-H)

Pre-computed reference data for TPC-H is stored in `TPC-H/ref_data/{scale}/`:

- Scale factors available: 1, 100, 300, 1,000, 3,000, 10,000, 100,000
- Contents: `.tbl` data files (partitioned chunks), `delete.u*` (update/delete streams), `subparam_*` (query parameter substitutions)
- Status: Legacy archive; not actively used by the current pipeline
- Potential use: External result validation or regression testing

---

## 7. End-to-End Output Summary

A complete benchmark execution produces the following outputs:

```
benchmark/
├── results_{engine}.json           # Full benchmark results (main output)
├── test_{engine}.json              # Quick validation results
├── graphs/
│   ├── concurrency_vs_max_sf.png   # Primary comparison chart
│   ├── latency_distribution_sf1.png
│   ├── detail_{engine}.png         # Per-engine detail tables
│   └── error_summary.png
├── logs/
│   └── combined/
│       ├── target-comparison.md    # Augmentation comparison report
│       └── target-comparison.json  # Machine-readable metrics
├── TPC-H/
│   └── logs/{engine}/sf{N}/stream{N}/
│       └── q{1-22}.log            # Per-query results (TSV)
└── TPC-DS/
    └── logs/{engine}/sf{N}/
        ├── tpcds_sf{N}.duckdb     # DuckDB data file (for DuckDB engine)
        └── stream{N}/
            └── query_summary.json  # Per-query result summary
```
