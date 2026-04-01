#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DBGEN_DIR="${REPO_ROOT}/TPC-H/dbgen"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
SCALE="${1:-1}"

cd "${DBGEN_DIR}"
make clean
make -j1 DATABASE=DUCKDB

./dbgen -f -s "${SCALE}"

cd "${REPO_ROOT}"
"${SCRIPT_DIR}/generate_queries.sh" 1
"${PYTHON_BIN}" "${SCRIPT_DIR}/load_data.py"
"${PYTHON_BIN}" "${SCRIPT_DIR}/run_queries.py"

echo "TPC-H DuckDB SF=${SCALE} pipeline completed successfully."
