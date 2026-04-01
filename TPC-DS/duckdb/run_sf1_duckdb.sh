#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
TEMPLATES_DIR="$ROOT_DIR/query_templates"
WORK_DIR="$ROOT_DIR/duckdb/work"
DATA_DIR="$WORK_DIR/data_sf1"
QUERIES_DIR="$WORK_DIR/queries_sf1"
DB_PATH="$WORK_DIR/tpcds_sf1.duckdb"
SUMMARY_JSON="$WORK_DIR/query_summary_sf1.json"
SEED="100"
THREADS="${THREADS:-4}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$WORK_DIR"
rm -rf "$DATA_DIR" "$QUERIES_DIR"
rm -f "$DB_PATH" "$SUMMARY_JSON"
mkdir -p "$DATA_DIR" "$QUERIES_DIR"

echo "[1/7] compile TPC-DS tools"
make -C "$TOOLS_DIR" OS=LINUX -j1 all

echo "[2/7] build dsdgen distribution index"
(
  cd "$TOOLS_DIR"
  ./distcomp -i tpcds.dst -o tpcds.idx
)

echo "[3/7] generate SF=1 data with dsdgen"
(
  cd "$TOOLS_DIR"
  ./dsdgen -scale 1 -dir "$DATA_DIR" -force y -rngseed "$SEED"
)

echo "[4/7] generate 99 DuckDB query templates with dsqgen"
(
  cd "$TOOLS_DIR"
  ./dsqgen \
    -input "$TEMPLATES_DIR/templates.lst" \
    -directory "$TEMPLATES_DIR" \
    -dialect duckdb \
    -output_dir "$QUERIES_DIR" \
    -scale 1 \
    -streams 1 \
    -rngseed "$SEED"
)

echo "[5/7] create DuckDB schema"
"$PYTHON_BIN" "$ROOT_DIR/duckdb/prepare_duckdb_schema.py" \
  --db "$DB_PATH" \
  --ddl "$TOOLS_DIR/tpcds.sql" \
  --threads "$THREADS"

echo "[6/7] bulk load generated flat files"
"$PYTHON_BIN" "$ROOT_DIR/duckdb/load_tpcds_data_duckdb.py" \
  --db "$DB_PATH" \
  --data-dir "$DATA_DIR" \
  --threads "$THREADS"

echo "[7/7] execute all 99 queries in DuckDB"
"$PYTHON_BIN" "$ROOT_DIR/duckdb/run_tpcds_queries_duckdb.py" \
  --db "$DB_PATH" \
  --queries-dir "$QUERIES_DIR" \
  --threads "$THREADS" \
  --summary-json "$SUMMARY_JSON"

echo "done: db=$DB_PATH summary=$SUMMARY_JSON"
