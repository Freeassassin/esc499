#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_DIR="${SCRIPT_DIR}/queries"
LOG_DIR="${SCRIPT_DIR}/logs"

COMPOSE="docker compose"
SERVICE="db"
DB_USER="${TPCH_PGUSER:-myuser}"
DB_NAME="${TPCH_PGDATABASE:-mydb}"

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}"/q*.log

for i in $(seq 1 22); do
  qfile="${QUERY_DIR}/${i}.sql"
  logfile="${LOG_DIR}/q${i}.log"

  if [[ ! -f "${qfile}" ]]; then
    echo "Missing query file: ${qfile}" >&2
    exit 1
  fi

  echo "Running query ${i}"
  ${COMPOSE} exec -T "${SERVICE}" \
    psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" \
    -f - < "${qfile}" > "${logfile}"
done

echo "All 22 queries executed successfully. Logs are in ${LOG_DIR}."
