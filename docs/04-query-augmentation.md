# Query Augmentation via LLM Agents

This document describes how Large Language Model (LLM) agents are used to augment standard TPC-H and TPC-DS queries to match real-world workload characteristics derived from the Amazon Redset dataset. This is the core novel contribution of the project.

---

## 1. Augmentation Motivation

Standard TPC-H and TPC-DS benchmarks are read-only workloads composed exclusively of SELECT queries with simple join patterns, numeric aggregations, and shallow expression trees. Real-world analytical workloads (as characterised by the Redset paper) contain:

- Mixed read/write operations (INSERT, UPDATE, DELETE, COPY)
- Schema introspection and metadata queries (SHOW commands)
- CREATE TABLE AS SELECT (CTAS) materialisation patterns
- Outer joins (37% of joins, vs 3% in TPC-DS)
- Text-based join keys and aggregation targets
- Deep expression nesting (12% of expressions have depth 11–100)
- Explicit NULL handling (IS NULL, COALESCE)
- System maintenance operations

The augmentation process modifies the TPC query templates to inject these characteristics while preserving the original analytical query logic and ensuring compatibility across all four target database engines.

---

## 2. Augmentation Targets

Fourteen workload characteristics were identified as augmentation targets, each with a quantified real-world value derived from the Redset dataset and broader literature:

| # | Characteristic | TPC Baseline | Real-World Target |
|:--|:---------------|:-------------|:------------------|
| 1 | Workload Mix (read/write) | ~80% SELECT | 40–60% Read/Write |
| 2 | CTAS Statements | 0% | ~1.9% of queries |
| 3 | Metadata Queries | 0% | ~31% of statements |
| 4 | System Maintenance | 0% | ~10% of queries |
| 5 | Query Repetition | 0% | Up to 80% duplicates |
| 6 | Operator Count | 9–20 avg | 13% with 101–1000 operators |
| 7 | Expression Depth | <0.1% depth >10 | 12% with depth 11–100 |
| 8 | Join Type | 97% Inner Join | 37% Outer Joins |
| 9 | Join Key Type | 81% Numeric | 46% Text-based keys |
| 10 | Aggregation Target | 98% Numeric | 34% on text columns |
| 11 | Aggregation Function | SUM dominant (64%) | ANYVALUE dominant (58%) |
| 12 | Data Skew | Low/Uniform | High (Q-Error up to 10²³⁹) |
| 13 | Data Types | INT/DATE common | VARCHAR dominant (52.1%) |
| 14 | Null Values | None/very few | High frequency, >99% null columns |

These targets are documented in `augmentation-targets.md` and serve as the specification that LLM agents work towards.

---

## 3. LLM Agent Workflow

### 3.1 Agent Platform

The augmentation is performed using **GitHub Copilot** agents within Visual Studio Code. Copilot's agent mode provides:

- Direct access to the workspace file system (reading and editing files)
- Terminal command execution for testing and validation
- Contextual understanding of the codebase structure
- Multi-turn conversation with memory of prior changes

### 3.2 LLM Providers

Three different LLM backends were used, each performing the full augmentation task independently:

| Provider | Model Family | Transcript File |
|:---------|:-------------|:----------------|
| **Anthropic Claude** | Claude (via Copilot) | `augmentation-chat-transcript/claude.json` |
| **Google Gemini** | Gemini (via Copilot) | `augmentation-chat-transcript/gemini.json` |
| **OpenAI** | GPT (via Copilot) | `augmentation-chat-transcript/openai.json` |

Each agent receives the same initial prompt with:
- Reference to `augmentation-targets.md` (the target specification)
- Reference to the TPC-DS template directory (`TPC-DS/query_templates/`)
- Reference to the TPC-H query directory (`TPC-H/dbgen/queries/`)
- Constraint: no modification to data or table structures
- Constraint: augmented queries must be compatible with all 4 engines
- Instruction to use DuckDB profiling at SF=1 for validation

### 3.3 Chat Transcript Format

Each transcript is stored as a JSON file with the following schema:

```json
{
  "responderUsername": "GitHub Copilot",
  "initialLocation": "panel",
  "requests": [
    {
      "requestId": "<UUID>",
      "message": {
        "text": "<user prompt>",
        "parts": [{ "range": ..., "text": ..., "kind": ... }]
      },
      "variableData": {
        "variables": [{ "kind": "file", "id": ..., "name": ..., "value": ... }]
      },
      "response": [
        { "kind": "markdownContent", "value": "<agent response>" },
        { "kind": "toolInvocation", ... }
      ]
    }
  ]
}
```

The transcripts capture the full multi-turn conversation including:
- User task directives and constraints
- Agent exploration of the codebase (file reads, directory listings)
- Agent strategy formulation and planning
- Agent edits to query templates and SQL files
- Validation commands executed via terminal
- Iterative refinement based on profiling results

### 3.4 What the Agents Modify

**TPC-DS modifications** (99 templates in `TPC-DS/query_templates/`):
- `.tpl` template files are edited directly
- Dialect-specific files (`duckdb.tpl`, `postgresql.tpl`, `cedardb.tpl`, `starrocks.tpl`) may be updated
- Augmentations include: converting INNER JOINs to OUTER, adding NULL predicates, adding COALESCE expressions, wrapping queries in CTAS, appending DML blocks, injecting metadata queries

**TPC-H modifications** (22 queries in `TPC-H/dbgen/queries/`):
- `.sql` query template files are edited directly (preserving `:1`–`:9` parameter placeholders for qgen substitution)
- Post-generation augmentation blocks are appended after the main SELECT query

**Augmented query structure**: Each augmented query contains:
1. The original TPC analytical query (modified with outer joins, null handling, etc.)
2. An **augmented workload block** (delimited by `-- AUGMENTED_WORKLOAD_BLOCK_END` marker) containing:
   - `CREATE TABLE IF NOT EXISTS aug_workload_ops` (DDL)
   - 2× INSERT/UPDATE/DELETE cycles (DML operations)
   - 8× metadata introspection queries (`SELECT COUNT(*) FROM information_schema.tables`)
   - 2× maintenance check queries (with `/* MAINTENANCE_OP */` comment marker)
   - Validation aggregation query

### 3.5 Cross-Engine Compatibility

The agents must ensure that augmented queries work across all four engines. Key compatibility constraints:

| Aspect | Approach |
|:-------|:---------|
| Standard SQL | Use ANSI SQL where possible (LEFT OUTER JOIN, COALESCE, IS NULL) |
| DDL/DML on StarRocks | Detected and replaced with `SELECT 1 AS ddl_probe` by `normalize_sql()` |
| Interval syntax | Handled by per-engine normalisation in `pipeline_common.py` |
| View conflicts | `CREATE VIEW` → `CREATE OR REPLACE VIEW` for TPC-H Q15 |
| Subquery aliasing | Added explicit aliases for StarRocks parser compatibility |
| Reserved words | `at` → `at_tbl`, `c_last_review_date_sk` → `c_last_review_date` |

---

## 4. SQL Normalisation Layer

The `pipeline_common.py` module in `TPC-DS/tools/` provides runtime SQL normalisation that adapts augmented queries to each engine's dialect. This is invoked during benchmark execution (not during generation).

### 4.1 `normalize_sql(engine, sql_text)`

This function applies engine-specific transformations:

**DuckDB**:
- Date intervals: `+ N days` → `+ INTERVAL N DAYS`

**PostgreSQL**:
- Date intervals: `+ N days` → `+ INTERVAL 'N days'`
- Division-by-zero: Wraps `amc/pmc` expressions in `NULLIF()`
- `lochierarchy` ORDER BY: Replaces alias-in-expression forms that PostgreSQL rejects with `GROUPING()` expressions

**CedarDB**:
- Date intervals: `+ N days` → `+ INTERVAL 'N days'`
- Same `NULLIF()` division protection as PostgreSQL

**StarRocks**:
- Date intervals: `+ N days` → `+ interval N day`
- `CURRENT_TIMESTAMP` → `cast(current_timestamp as datetime)`
- Missing subquery aliases added (e.g., `starrocks_q49`, `starrocks_q69`)
- DML/DDL statements detected and replaced with probe queries

### 4.2 `load_statements(queries_dir)`

This function handles the two query file formats:
1. **Individual files**: `query1.sql` through `query99.sql` — each loaded directly
2. **Merged file**: `query_0.sql` containing all 99 queries — split on `AUGMENTED_QUERY_END_MARKER` or semicolons

Returns a list of `(query_id, source_file, sql_text)` tuples.

### 4.3 Statement Splitting

For TPC-H queries (which include augmented workload blocks), statements are split on semicolons via `_split_statements()`. Each statement is executed independently, allowing mixed DDL/DML/SELECT patterns within a single query file.

---

## 5. Augmentation Validation

### 5.1 Comparison Tool

`tools/compare_augmented_targets.py` validates the augmented query workload against the 14 target characteristics. It:

1. **Loads queries**: Reads all 99 TPC-DS queries and 22 TPC-H queries from DuckDB-format directories
2. **Parses SQL**: Uses regex-based analysis to extract:
   - Statement types (SELECT, INSERT, UPDATE, DELETE, CREATE, SHOW)
   - Join types (INNER vs OUTER) and join key types (text vs numeric)
   - Aggregation functions and targets (numeric vs text columns)
   - NULL handling patterns (IS NULL, IS NOT NULL, COALESCE)
   - String type usage, timestamp operations, boolean expressions
3. **Analyses DuckDB profiles**: Reads JSON operator trees from profiling output to measure:
   - Operator counts per query (binned into ranges: 1–10, 11–100, 101–1000)
   - Expression nesting depth (binned similarly)
4. **Compares against targets**: Produces a side-by-side comparison table
5. **Outputs results**:
   - Markdown report: `logs/combined/target-comparison.md`
   - JSON payload: `logs/combined/target-comparison.json`

### 5.2 DuckDB Profiling Pipeline

Profiling uses DuckDB's built-in JSON profiling capability:

1. **Profile capture** (`profile_duckdb.py`):
   ```sql
   PRAGMA enable_profiling='json';
   PRAGMA profiling_output='{path}';
   ```
   Output: `logs/duckdb/sf{scale}/profile/q{N}.json` per query

2. **Profile digestion** (`digest_profile.py`):
   - Parses JSON operator trees
   - Walks tree to collect timing, cardinality, operator type statistics
   - Generates Markdown summary reports

### 5.3 Post-Augmentation Results

The comparison tool produces the following observed metrics (from 2,381 total statements across 121 base queries):

```json
{
  "statement_metrics": {
    "total_statements": 2381,
    "read_pct": 69.51,
    "write_pct": 30.49,
    "ctas_pct": 1.60,
    "metadata_pct": 45.78,
    "maintenance_pct": 10.16,
    "duplicate_pct": 94.20,
    "outer_join_pct": 80.23,
    "text_join_key_pct": 52.33,
    "agg_anyvalue_pct": 5.67,
    "agg_text_target_pct": 11.76,
    "null_stmt_pct": 17.43,
    "string_stmt_pct": 13.48,
    "timestamp_stmt_pct": 15.88,
    "boolean_stmt_pct": 0.63
  },
  "profile_metrics": {
    "op_101_1000_pct": 0.0,
    "expr_11_100_pct": 37.19
  }
}
```

**Notes on deviation from targets**:
- Metrics are computed from SQL-level analysis and DuckDB profiling at SF=1
- Some metrics depend on physical data distribution (data skew, real null density) which cannot be fully captured at the query level
- Expression depth (37.19% vs 12% target) and outer join share (80.23% vs 37% target) exceed targets, while ANYVALUE aggregation (5.67% vs 58% target) falls below

---

## 6. Multi-Provider Comparison

The three LLM providers (Claude, Gemini, OpenAI) each independently augment the full query set. This enables comparison of:

- **Augmentation completeness**: Which provider achieves closest alignment to all 14 targets
- **Cross-engine compatibility**: Which provider produces fewest engine-specific errors
- **Augmentation strategy**: How each provider interprets and implements the augmentation requirements
- **Query correctness**: Whether augmented queries produce valid results across engines

The chat transcripts in `augmentation-chat-transcript/` preserve the full decision-making process, providing transparency into how each agent approached the task. This is particularly relevant for evaluating the feasibility of using AI agents as database benchmark developers.
