#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TOOLS_DIR="${SCRIPT_DIR}/tools"

usage() {
  cat <<'EOF'
Usage:
  ./TPC-H/run.sh generate-data --scale <sf> [--force]
  ./TPC-H/run.sh generate-queries --engine <engine> [--stream <n>] [--force]
  ./TPC-H/run.sh load --engine <engine> --scale <sf>
  ./TPC-H/run.sh run --engine <engine> --scale <sf> [--stream <n>]
  ./TPC-H/run.sh pipeline --engine <engine> --scale <sf> [--stream <n>] [--force-data] [--force-queries]

Engines:
  duckdb | postgresql | cedardb | starrocks
EOF
}

require_arg() {
  local value="$1"
  local name="$2"
  if [[ -z "${value}" ]]; then
    echo "Missing required argument: ${name}" >&2
    usage
    exit 1
  fi
}

validate_engine() {
  case "$1" in
    duckdb|postgresql|cedardb|starrocks) ;;
    *)
      echo "Unsupported engine: $1" >&2
      usage
      exit 1
      ;;
  esac
}

ensure_services() {
  local engine="$1"
  cd "${REPO_ROOT}"
  case "${engine}" in
    postgresql)
      docker compose up -d db
      ;;
    cedardb)
      docker compose up -d cedardb
      ;;
    starrocks)
      docker compose up -d fe be
      ;;
    duckdb)
      ;;
  esac
}

subcommand="${1:-}"
if [[ -z "${subcommand}" ]]; then
  usage
  exit 1
fi
shift

case "${subcommand}" in
  generate-data)
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
          echo "Unknown option for generate-data: $1" >&2
          usage
          exit 1
          ;;
      esac
    done
    require_arg "${scale}" "--scale"
    "${TOOLS_DIR}/generate_data.sh" --scale "${scale}" $([[ "${force}" -eq 1 ]] && echo --force)
    ;;

  generate-queries)
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
          echo "Unknown option for generate-queries: $1" >&2
          usage
          exit 1
          ;;
      esac
    done
    require_arg "${engine}" "--engine"
    validate_engine "${engine}"
    "${TOOLS_DIR}/generate_queries.sh" --engine "${engine}" --stream "${stream}" $([[ "${force}" -eq 1 ]] && echo --force)
    ;;

  load)
    engine=""
    scale=""
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
        *)
          echo "Unknown option for load: $1" >&2
          usage
          exit 1
          ;;
      esac
    done
    require_arg "${engine}" "--engine"
    require_arg "${scale}" "--scale"
    validate_engine "${engine}"
    ensure_services "${engine}"
    python3 "${TOOLS_DIR}/load_data.py" --engine "${engine}" --scale "${scale}"
    ;;

  run)
    engine=""
    scale=""
    stream="1"
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
        *)
          echo "Unknown option for run: $1" >&2
          usage
          exit 1
          ;;
      esac
    done
    require_arg "${engine}" "--engine"
    require_arg "${scale}" "--scale"
    validate_engine "${engine}"
    ensure_services "${engine}"
    python3 "${TOOLS_DIR}/run_queries.py" --engine "${engine}" --scale "${scale}" --stream "${stream}"
    ;;

  pipeline)
    engine=""
    scale=""
    stream="1"
    force_data=0
    force_queries=0
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
        --force-data)
          force_data=1
          shift
          ;;
        --force-queries)
          force_queries=1
          shift
          ;;
        *)
          echo "Unknown option for pipeline: $1" >&2
          usage
          exit 1
          ;;
      esac
    done
    require_arg "${engine}" "--engine"
    require_arg "${scale}" "--scale"
    validate_engine "${engine}"
    "${TOOLS_DIR}/generate_data.sh" --scale "${scale}" $([[ "${force_data}" -eq 1 ]] && echo --force)
    "${TOOLS_DIR}/generate_queries.sh" --engine "${engine}" --stream "${stream}" $([[ "${force_queries}" -eq 1 ]] && echo --force)
    ensure_services "${engine}"
    python3 "${TOOLS_DIR}/load_data.py" --engine "${engine}" --scale "${scale}"
    python3 "${TOOLS_DIR}/run_queries.py" --engine "${engine}" --scale "${scale}" --stream "${stream}"
    ;;

  *)
    echo "Unknown subcommand: ${subcommand}" >&2
    usage
    exit 1
    ;;
esac
