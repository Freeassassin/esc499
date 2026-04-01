#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DBGEN_DIR="${REPO_ROOT}/TPC-H/dbgen"

COMPOSE="docker compose"
SERVICE="db"
DB_USER="${TPCH_PGUSER:-myuser}"
DB_NAME="${TPCH_PGDATABASE:-mydb}"
PARALLEL_JOBS="${TPCH_LOAD_JOBS:-4}"

load_one() {
  local table="$1"
  local file="${DBGEN_DIR}/${table}.tbl"

  if [[ ! -f "${file}" ]]; then
    echo "Missing data file: ${file}" >&2
    return 1
  fi

  echo "Loading ${table} from ${file}"
  sed 's/|$//' "${file}" | ${COMPOSE} exec -T "${SERVICE}" \
    psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" \
    -c "\\copy ${table} from stdin with (format text, delimiter '|')"
}

run_parallel_phase() {
  local running=0
  local table
  for table in "$@"; do
    load_one "${table}" &
    running=$((running + 1))
    if [[ "${running}" -ge "${PARALLEL_JOBS}" ]]; then
      wait -n
      running=$((running - 1))
    fi
  done
  wait
}

# Respect FK dependency chain and parallelize only independent tables.
load_one region
load_one nation
run_parallel_phase supplier customer part
load_one partsupp
load_one orders
load_one lineitem

echo "All TPC-H tables loaded successfully."
