#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPCH_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DBGEN_DIR="${TPCH_ROOT}/dbgen"
DATA_ROOT="${TPCH_ROOT}/data"

TABLES=(region nation supplier customer part partsupp orders lineitem)

scale=""
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale)
      scale="${2:-}"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    *)
      echo "Unknown option for generate_data.sh: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${scale}" ]]; then
  echo "Missing required argument --scale" >&2
  exit 1
fi

target_dir="${DATA_ROOT}/${scale}"
marker_file="${target_dir}/.done"
meta_file="${target_dir}/metadata.txt"
lock_dir="${DATA_ROOT}/.locks"
lock_file="${lock_dir}/sf_${scale}.lock"
tmp_dir="${DATA_ROOT}/.tmp/sf_${scale}_$$"

mkdir -p "${target_dir}" "${lock_dir}" "${DATA_ROOT}/.tmp"

exec 9>"${lock_file}"
flock 9

if [[ "${force}" -eq 0 && -f "${marker_file}" ]]; then
  echo "Data for SF=${scale} already prepared at ${target_dir}; skipping dbgen."
  exit 0
fi

echo "Building dbgen/qgen..."
cd "${DBGEN_DIR}"
make clean
make -j1 DATABASE="${TPCH_DBGEN_DATABASE:-ORACLE}"

echo "Generating TPC-H data for SF=${scale}..."
./dbgen -f -s "${scale}"

rm -rf "${tmp_dir}"
mkdir -p "${tmp_dir}"

for table in "${TABLES[@]}"; do
  src="${DBGEN_DIR}/${table}.tbl"
  if [[ ! -f "${src}" ]]; then
    echo "Expected output file missing: ${src}" >&2
    exit 1
  fi
  cp "${src}" "${tmp_dir}/${table}.tbl"
done

if [[ "$(wc -l < "${tmp_dir}/region.tbl")" -ne 5 ]]; then
  echo "Sanity check failed for region.tbl (expected 5 rows)." >&2
  exit 1
fi

rm -rf "${target_dir}"
mkdir -p "${target_dir}"
cp "${tmp_dir}"/*.tbl "${target_dir}/"

{
  echo "scale=${scale}"
  echo "generated_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "dbgen_database=${TPCH_DBGEN_DATABASE:-ORACLE}"
  for table in "${TABLES[@]}"; do
    echo "rows_${table}=$(wc -l < "${target_dir}/${table}.tbl")"
  done
} > "${meta_file}"

touch "${marker_file}"
rm -rf "${tmp_dir}"

echo "Shared TPC-H data is ready in ${target_dir}."
