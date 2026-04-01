#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DBGEN_DIR="${REPO_ROOT}/TPC-H/dbgen"
DDL_FILE="${SCRIPT_DIR}/ddl.sql"

COMPOSE="docker compose"
SERVICE="db"
DB_USER="${TPCH_PGUSER:-myuser}"
DB_NAME="${TPCH_PGDATABASE:-mydb}"
SCALE="${1:-1}"

cd "${REPO_ROOT}"
${COMPOSE} up -d

cd "${DBGEN_DIR}"
make clean
make -j1

./dbgen -f -s "${SCALE}"

cd "${REPO_ROOT}"
${COMPOSE} exec -T "${SERVICE}" \
  psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" -f - < "${DDL_FILE}"

"${SCRIPT_DIR}/load_data.sh"
"${SCRIPT_DIR}/generate_queries.sh" 1
"${SCRIPT_DIR}/run_queries.sh"

echo "TPC-H PostgreSQL SF=${SCALE} pipeline completed successfully."
