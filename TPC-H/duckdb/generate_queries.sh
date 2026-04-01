#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DBGEN_DIR="${REPO_ROOT}/TPC-H/dbgen"
OUT_DIR="${SCRIPT_DIR}/queries"
STREAM="${1:-1}"

mkdir -p "${OUT_DIR}"
rm -f "${OUT_DIR}"/*.sql

cd "${DBGEN_DIR}"

for i in $(seq 1 22); do
  raw_file="${OUT_DIR}/${i}.raw.sql"
  out_file="${OUT_DIR}/${i}.sql"

  DSS_QUERY=queries ./qgen -s "${STREAM}" "${i}" > "${raw_file}"

  sed -E \
    -e 's/\r$//' \
    -e "s/interval '([0-9]+)' day \(3\)/interval '\1 day'/g" \
    -e "s/interval '([0-9]+)' month/interval '\1 month'/g" \
    -e "s/interval '([0-9]+)' year/interval '\1 year'/g" \
    -e '/^[[:space:]]*limit -1;[[:space:]]*$/d' \
    -e '/^\s*$/N;/^\n$/D' \
    "${raw_file}" > "${out_file}"

  LC_ALL=C perl -0777 -i -pe 's/;\n(\s*limit\s+\d+;)/\n$1/g' "${out_file}"

  rm -f "${raw_file}"
  echo "Generated ${out_file}"
done

echo "Generated all 22 DuckDB-compatible query files in ${OUT_DIR}."
