#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPANY="${1:-gainr}"
PUBLISH=0

usage() {
  cat <<'EOF'
Usage: ./scripts/run_scheduled_etl.sh [company] [--publish]

Runs an applied source refresh followed by safe incremental ETL. The pipeline
automatically falls back to a full rebuild when baseline artifacts are missing
or shared lookup data changed.

Options:
  --publish   Atomically publish the validated merged output.

Environment:
  RAG_HT_NICE=5   Process niceness on Linux/macOS. Use 0 to disable.
EOF
}

for argument in "${@:2}"; do
  case "$argument" in
    --publish)
      PUBLISH=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $argument" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$COMPANY" == "-h" || "$COMPANY" == "--help" ]]; then
  usage
  exit 0
fi
if [[ ! "$COMPANY" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]]; then
  echo "Unsafe company slug: $COMPANY" >&2
  exit 2
fi

PIPELINE="$ROOT_DIR/.venv/bin/rag-ht-pipeline"
if [[ ! -x "$PIPELINE" ]]; then
  echo "Pipeline command is missing: $PIPELINE" >&2
  echo "Run ./scripts/setup.sh first." >&2
  exit 1
fi

LOCK_BASE="${TMPDIR:-/tmp}"
LOCK_FILE="$LOCK_BASE/rag-ht-etl-$COMPANY.lock"
LOCK_DIR="$LOCK_FILE.d"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[$(date --iso-8601=seconds 2>/dev/null || date)] $COMPANY ETL is already running; skipping."
    exit 0
  fi
else
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "[$(date)] $COMPANY ETL is already running; skipping."
    exit 0
  fi
  trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
fi

cd "$ROOT_DIR"
export PYTHONUNBUFFERED=1
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"

command=(
  "$PIPELINE"
  --company "$COMPANY"
  --refresh-source configured
  --apply-source-refresh
  --run-all
  --incremental
  --no-csv
)
if [[ "$PUBLISH" -eq 1 ]]; then
  command+=(--publish)
fi

nice_level="${RAG_HT_NICE:-5}"
echo "[$(date --iso-8601=seconds 2>/dev/null || date)] Starting scheduled ETL for $COMPANY."
set +e
if [[ "$nice_level" == "0" ]] || ! command -v nice >/dev/null 2>&1; then
  "${command[@]}"
  status=$?
else
  nice -n "$nice_level" "${command[@]}"
  status=$?
fi
set -e
echo "[$(date --iso-8601=seconds 2>/dev/null || date)] Scheduled ETL for $COMPANY finished with status $status."
exit "$status"
