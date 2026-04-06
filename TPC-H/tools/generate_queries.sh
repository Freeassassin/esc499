#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPCH_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DBGEN_DIR="${TPCH_ROOT}/dbgen"
RAW_ROOT="${TPCH_ROOT}/queries/raw"
OUT_ROOT="${TPCH_ROOT}/queries"
TRANSFORM_ROOT="${SCRIPT_DIR}/query_transforms"

engine=""
stream="1"
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine)
      engine="${2:-}"
      shift 2
      ;;
    --stream)
      stream="${2:-}"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    *)
      echo "Unknown option for generate_queries.sh: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${engine}" ]]; then
  echo "Missing required argument --engine" >&2
  exit 1
fi

case "${engine}" in
  duckdb)
    db_target="DUCKDB"
    ;;
  postgresql)
    db_target="POSTGRESQL"
    ;;
  cedardb)
    db_target="CEDARDB"
    ;;
  starrocks)
    db_target="STARROCKS"
    ;;
  *)
    echo "Unsupported engine: ${engine}" >&2
    exit 1
    ;;
esac

sed_rules="${TRANSFORM_ROOT}/${engine}.sed"
perl_rules="${TRANSFORM_ROOT}/${engine}.pl"

if [[ ! -f "${sed_rules}" || ! -f "${perl_rules}" ]]; then
  echo "Missing transform rules for ${engine}." >&2
  exit 1
fi

raw_dir="${RAW_ROOT}/${engine}/${stream}"
out_dir="${OUT_ROOT}/${engine}/${stream}"
mkdir -p "${raw_dir}" "${out_dir}"

cd "${DBGEN_DIR}"
make clean
make -j1 DATABASE="${db_target}" qgen

for i in $(seq 1 22); do
  raw_file="${raw_dir}/${i}.sql"
  out_file="${out_dir}/${i}.sql"

  if [[ "${force}" -eq 1 || ! -f "${raw_file}" ]]; then
    DSS_QUERY=queries ./qgen -s "${stream}" "${i}" > "${raw_file}"
  fi

  sed -E -f "${sed_rules}" "${raw_file}" > "${out_file}"
  LC_ALL=C perl -0777 -i -pe "$(tr '\n' ' ' < "${perl_rules}")" "${out_file}"
done

echo "Generated ${engine} query set in ${out_dir}."
