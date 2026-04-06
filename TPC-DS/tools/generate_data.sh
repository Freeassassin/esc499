#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_ROOT="${ROOT_DIR}/data"
TOOLS_DIR="${SCRIPT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ./TPC-DS/tools/generate_data.sh --scale <sf> [--seed <n>] [--force]

Environment:
  TPCDS_EXTRA_CFLAGS  Extra CFLAGS for make (default: -w)
EOF
}

scale=""
seed="100"
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale)
      scale="${2:-}"
      shift 2
      ;;
    --seed)
      seed="${2:-}"
      shift 2
      ;;
    --force)
      force=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${scale}" ]]; then
  echo "--scale is required" >&2
  usage
  exit 1
fi

scale_dir="${DATA_ROOT}/sf${scale}"
marker="${scale_dir}/.generated.json"
lock_file="${DATA_ROOT}/.sf${scale}.lock"
expected_files=$(grep -Eic '^create table' "${TOOLS_DIR}/tpcds.sql")

mkdir -p "${DATA_ROOT}" "${scale_dir}"

exec 9>"${lock_file}"
flock 9

if [[ "${force}" -eq 0 && -f "${marker}" ]]; then
  echo "data_cache_hit:sf${scale}:${scale_dir}"
  exit 0
fi

if [[ "${force}" -eq 0 && -n "$(find "${scale_dir}" -maxdepth 1 -type f -name '*.dat' -print -quit)" ]]; then
  file_count=$(find "${scale_dir}" -maxdepth 1 -type f -name '*.dat' | wc -l | tr -d '[:space:]')
  if [[ "${file_count}" -ne "${expected_files}" ]]; then
    echo "data_cache_incomplete:sf${scale}:files=${file_count}:expected=${expected_files}; regenerating"
    rm -f "${scale_dir}"/*.dat "${marker}"
  else
  cat >"${marker}" <<EOF
{
  "scale": ${scale},
  "seed": ${seed},
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "data_dir": "${scale_dir}",
  "dat_files": ${file_count},
  "cache_promoted_from_existing_files": true
}
EOF
  echo "data_cache_promoted:sf${scale}:${scale_dir}:files=${file_count}"
  exit 0
  fi
fi

if [[ "${force}" -eq 1 ]]; then
  rm -f "${scale_dir}"/*.dat "${marker}"
fi

extra_cflags="${TPCDS_EXTRA_CFLAGS:--w}"

make -C "${TOOLS_DIR}" --no-print-directory -j1 OS=LINUX EXTRA_CFLAGS="${extra_cflags}" all >/dev/null
(
  cd "${TOOLS_DIR}"
  ./distcomp -i tpcds.dst -o tpcds.idx >/dev/null
  ./dsdgen -scale "${scale}" -dir "${scale_dir}" -force y -rngseed "${seed}" -quiet y >/dev/null
)

file_count=$(find "${scale_dir}" -maxdepth 1 -type f -name '*.dat' | wc -l | tr -d '[:space:]')
if [[ "${file_count}" -ne "${expected_files}" ]]; then
  echo "generated data file count ${file_count} does not match expected table count ${expected_files}" >&2
  exit 1
fi
cat >"${marker}" <<EOF
{
  "scale": ${scale},
  "seed": ${seed},
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "data_dir": "${scale_dir}",
  "dat_files": ${file_count},
  "expected_dat_files": ${expected_files}
}
EOF

echo "data_generated:sf${scale}:${scale_dir}:files=${file_count}"
