#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATES_DIR="${ROOT_DIR}/query_templates"
QUERIES_ROOT="${ROOT_DIR}/queries"

usage() {
  cat <<'EOF'
Usage:
  ./TPC-DS/tools/generate_queries.sh --engine <engine> --scale <sf> [--stream <n>] [--seed <n>] [--force]

Engines:
  duckdb | cedardb | starrocks | postgresql
EOF
}

engine=""
scale=""
stream="1"
seed="100"
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine)
      engine="${2:-}"
      shift 2
      ;;
    --scale)
      scale="${2:-}"
      shift 2
      ;;
    --stream)
      stream="${2:-}"
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

if [[ -z "${engine}" || -z "${scale}" ]]; then
  echo "--engine and --scale are required" >&2
  usage
  exit 1
fi

case "${engine}" in
  duckdb|cedardb|starrocks|postgresql) ;;
  *)
    echo "Unsupported engine: ${engine}" >&2
    exit 1
    ;;
esac

queries_dir="${QUERIES_ROOT}/${engine}/sf${scale}/stream${stream}"
marker="${queries_dir}/.generated.json"
mkdir -p "${queries_dir}"

if [[ "${force}" -eq 0 && -f "${marker}" ]]; then
  echo "query_cache_hit:${engine}:sf${scale}:stream${stream}:${queries_dir}"
  exit 0
fi

find "${queries_dir}" -maxdepth 1 -type f -name 'query*.sql' -delete

tmp_workdir="${queries_dir}/.build_tmp"
rm -rf "${tmp_workdir}"
mkdir -p "${tmp_workdir}"

for i in $(seq 1 99); do
  list_file="${tmp_workdir}/q${i}.lst"
  out_dir="${tmp_workdir}/q${i}"
  mkdir -p "${out_dir}"
  echo "query${i}.tpl" > "${list_file}"

  (
    cd "${SCRIPT_DIR}"
    ./dsqgen \
      -input "${list_file}" \
      -directory "${TEMPLATES_DIR}" \
      -dialect "${engine}" \
      -output_dir "${out_dir}" \
      -scale "${scale}" \
      -streams "${stream}" \
      -rngseed "${seed}" \
      >/dev/null
  )

  if [[ -f "${out_dir}/query_0.sql" ]]; then
    mv "${out_dir}/query_0.sql" "${queries_dir}/query${i}.sql"
  elif [[ -f "${out_dir}/query${i}.sql" ]]; then
    mv "${out_dir}/query${i}.sql" "${queries_dir}/query${i}.sql"
  else
    echo "Expected generated file missing for query ${i}" >&2
    rm -rf "${tmp_workdir}"
    exit 1
  fi
done

rm -rf "${tmp_workdir}"

sql_count=$(find "${queries_dir}" -maxdepth 1 -type f -name '*.sql' | wc -l | tr -d '[:space:]')
cat >"${marker}" <<EOF
{
  "engine": "${engine}",
  "scale": ${scale},
  "stream": ${stream},
  "seed": ${seed},
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "queries_dir": "${queries_dir}",
  "sql_files": ${sql_count}
}
EOF

echo "queries_generated:${engine}:sf${scale}:stream${stream}:${queries_dir}:files=${sql_count}"
