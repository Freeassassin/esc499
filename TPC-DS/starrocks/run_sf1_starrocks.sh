#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
TEMPLATES_DIR="$ROOT_DIR/query_templates"
WORK_DIR="$ROOT_DIR/starrocks/work"
SCALE="${SCALE:-1}"
SEED="${SEED:-100}"
DATA_DIR="$WORK_DIR/data_sf${SCALE}"
QUERIES_DIR="$WORK_DIR/queries_sf${SCALE}"
SUMMARY_JSON="$WORK_DIR/query_summary_sf${SCALE}.json"
PYTHON_BIN="${PYTHON_BIN:-python3}"

STARROCKS_HOST="${TPCDS_STARROCKS_HOST:-127.0.0.1}"
STARROCKS_PORT="${TPCDS_STARROCKS_PORT:-9030}"
STARROCKS_HTTP_HOST="${TPCDS_STARROCKS_HTTP_HOST:-127.0.0.1}"
STARROCKS_HTTP_PORT="${TPCDS_STARROCKS_HTTP_PORT:-8030}"
STARROCKS_USER="${TPCDS_STARROCKS_USER:-root}"
STARROCKS_PASSWORD="${TPCDS_STARROCKS_PASSWORD:-}"
STARROCKS_DB="${TPCDS_STARROCKS_DB:-tpcds}"
STARROCKS_BACKEND="${TPCDS_STARROCKS_BACKEND:-be:9050}"

mkdir -p "$WORK_DIR"
rm -rf "$DATA_DIR" "$QUERIES_DIR"
rm -f "$SUMMARY_JSON"
mkdir -p "$DATA_DIR" "$QUERIES_DIR"

echo "[1/8] start StarRocks FE and BE"
docker compose up -d fe be

echo "[2/8] compile TPC-DS tools"
make -C "$TOOLS_DIR" OS=LINUX -j1 all

echo "[3/8] build dsdgen distribution index"
(
  cd "$TOOLS_DIR"
  ./distcomp -i tpcds.dst -o tpcds.idx
)

echo "[4/8] generate SF=${SCALE} data with dsdgen"
(
  cd "$TOOLS_DIR"
  ./dsdgen -scale "$SCALE" -dir "$DATA_DIR" -force y -rngseed "$SEED"
)

echo "[5/8] generate 99 StarRocks query templates with dsqgen"
(
  cd "$TOOLS_DIR"
  ./dsqgen \
    -input "$TEMPLATES_DIR/templates.lst" \
    -directory "$TEMPLATES_DIR" \
    -dialect starrocks \
    -output_dir "$QUERIES_DIR" \
    -scale "$SCALE" \
    -streams 1 \
    -rngseed "$SEED"
)

echo "[6/8] create StarRocks schema"
"$PYTHON_BIN" "$ROOT_DIR/starrocks/prepare_starrocks_schema.py" \
  --ddl "$TOOLS_DIR/tpcds.sql" \
  --host "$STARROCKS_HOST" \
  --port "$STARROCKS_PORT" \
  --user "$STARROCKS_USER" \
  --password "$STARROCKS_PASSWORD" \
  --dbname "$STARROCKS_DB" \
  --backend-addr "$STARROCKS_BACKEND"

echo "[7/8] bulk load generated flat files"
"$PYTHON_BIN" "$ROOT_DIR/starrocks/load_tpcds_data_starrocks.py" \
  --ddl "$TOOLS_DIR/tpcds.sql" \
  --data-dir "$DATA_DIR" \
  --host "$STARROCKS_HOST" \
  --port "$STARROCKS_PORT" \
  --http-host "$STARROCKS_HTTP_HOST" \
  --http-port "$STARROCKS_HTTP_PORT" \
  --user "$STARROCKS_USER" \
  --password "$STARROCKS_PASSWORD" \
  --dbname "$STARROCKS_DB" \
  --backend-addr "$STARROCKS_BACKEND"

echo "[8/8] execute all 99 queries in StarRocks"
"$PYTHON_BIN" "$ROOT_DIR/starrocks/run_tpcds_queries_starrocks.py" \
  --queries-dir "$QUERIES_DIR" \
  --host "$STARROCKS_HOST" \
  --port "$STARROCKS_PORT" \
  --user "$STARROCKS_USER" \
  --password "$STARROCKS_PASSWORD" \
  --dbname "$STARROCKS_DB" \
  --summary-json "$SUMMARY_JSON"

echo "done: summary=$SUMMARY_JSON"