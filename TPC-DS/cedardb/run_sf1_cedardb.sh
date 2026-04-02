#!/usr/bin/env bash
# Repeatable TPC-DS SF=1 benchmark pipeline for CedarDB.
#
# Usage:
#   ./TPC-DS/cedardb/run_sf1_cedardb.sh
#
# Environment overrides (all optional):
#   PYTHON_BIN   path to Python interpreter  (default: python3)
#   CEDAR_HOST   CedarDB host                (default: localhost)
#   CEDAR_PORT   CedarDB port                (default: 5432)
#   CEDAR_DB     CedarDB database name       (default: db)
#   CEDAR_USER   CedarDB user                (default: admin)
#   CEDAR_PASS   CedarDB password            (default: admin)
#   SCALE        dsdgen scale factor         (default: 1)
#   SEED         random seed for dsdgen/dsqgen (default: 100)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
TEMPLATES_DIR="$ROOT_DIR/query_templates"
WORK_DIR="$ROOT_DIR/cedardb/work"
SCALE="${SCALE:-1}"
SEED="${SEED:-100}"
DATA_DIR="$WORK_DIR/data_sf${SCALE}"
QUERIES_DIR="$WORK_DIR/queries_sf${SCALE}"
SUMMARY_JSON="$WORK_DIR/query_summary_sf${SCALE}.json"
PYTHON_BIN="${PYTHON_BIN:-python3}"

CEDAR_HOST="${CEDAR_HOST:-localhost}"
CEDAR_PORT="${CEDAR_PORT:-5432}"
CEDAR_DB="${CEDAR_DB:-db}"
CEDAR_USER="${CEDAR_USER:-admin}"
CEDAR_PASS="${CEDAR_PASS:-admin}"

CONN_ARGS="--host $CEDAR_HOST --port $CEDAR_PORT --dbname $CEDAR_DB --user $CEDAR_USER --password $CEDAR_PASS"

mkdir -p "$WORK_DIR"
rm -rf "$DATA_DIR" "$QUERIES_DIR" "$SUMMARY_JSON"
mkdir -p "$DATA_DIR" "$QUERIES_DIR"

echo "[1/7] compile TPC-DS tools"
make -C "$TOOLS_DIR" OS=LINUX -j1 all

echo "[2/7] build dsdgen distribution index"
(
  cd "$TOOLS_DIR"
  ./distcomp -i tpcds.dst -o tpcds.idx
)

echo "[3/7] generate SF=${SCALE} data with dsdgen"
(
  cd "$TOOLS_DIR"
  ./dsdgen -scale "$SCALE" -dir "$DATA_DIR" -force y -rngseed "$SEED"
)

echo "[4/7] generate 99 CedarDB query templates with dsqgen"
(
  cd "$TOOLS_DIR"
  ./dsqgen \
    -input "$TEMPLATES_DIR/templates.lst" \
    -directory "$TEMPLATES_DIR" \
    -dialect cedardb \
    -output_dir "$QUERIES_DIR" \
    -scale "$SCALE" \
    -streams 1 \
    -rngseed "$SEED"
)

echo "[5/7] create CedarDB schema"
"$PYTHON_BIN" "$ROOT_DIR/cedardb/prepare_cedardb_schema.py" \
  --ddl "$TOOLS_DIR/tpcds.sql" \
  $CONN_ARGS

echo "[6/7] bulk load generated flat files"
"$PYTHON_BIN" "$ROOT_DIR/cedardb/load_tpcds_data_cedardb.py" \
  --data-dir "$DATA_DIR" \
  $CONN_ARGS

echo "[7/7] execute all 99 queries in CedarDB"
"$PYTHON_BIN" "$ROOT_DIR/cedardb/run_tpcds_queries_cedardb.py" \
  --queries-dir "$QUERIES_DIR" \
  --summary-json "$SUMMARY_JSON" \
  $CONN_ARGS

echo "done: summary=$SUMMARY_JSON"
