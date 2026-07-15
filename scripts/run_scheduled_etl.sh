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
  RAG_HT_MAX_ATTEMPTS=3       Retry transient database/network failures.
  RAG_HT_RETRY_DELAY_SECONDS=30
  RAG_HT_TIMEOUT_SECONDS=10800
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
OPS="$ROOT_DIR/.venv/bin/rag-ht-ops"
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

if ! "$OPS" preflight --company "$COMPANY"; then
  "$OPS" notify --company "$COMPANY" --severity error \
    --message "ETL preflight failed; the scheduled run was not started." || true
  exit 1
fi

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
max_attempts="${RAG_HT_MAX_ATTEMPTS:-3}"
retry_delay="${RAG_HT_RETRY_DELAY_SECONDS:-30}"
timeout_seconds="${RAG_HT_TIMEOUT_SECONDS:-10800}"
status_path="$("$OPS" status-path --company "$COMPANY")"
echo "[$(date --iso-8601=seconds 2>/dev/null || date)] Starting scheduled ETL for $COMPANY."
status=1
attempt=1
while [[ "$attempt" -le "$max_attempts" ]]; do
  echo "[$(date --iso-8601=seconds 2>/dev/null || date)] Attempt $attempt/$max_attempts."
  set +e
  if command -v timeout >/dev/null 2>&1; then
    if [[ "$nice_level" == "0" ]] || ! command -v nice >/dev/null 2>&1; then
      timeout --signal=TERM --kill-after=60 "$timeout_seconds" "${command[@]}"
    else
      timeout --signal=TERM --kill-after=60 "$timeout_seconds" nice -n "$nice_level" "${command[@]}"
    fi
  elif [[ "$nice_level" == "0" ]] || ! command -v nice >/dev/null 2>&1; then
    "${command[@]}"
  else
    nice -n "$nice_level" "${command[@]}"
  fi
  status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    break
  fi
  retryable="$($ROOT_DIR/.venv/bin/python -c '
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
try:
    status = json.loads(p.read_text())
except Exception:
    print("yes")
else:
    text = "{} {}".format(status.get("error_type", ""), status.get("error", "")).lower()
    markers = ("operationalerror", "timeout", "connection", "temporar", "deadlock", "lock wait", "broken pipe", "oserror")
    print("yes" if any(marker in text for marker in markers) else "no")
' "$status_path")"
  if [[ "$retryable" != "yes" || "$attempt" -ge "$max_attempts" ]]; then
    break
  fi
  echo "Transient failure detected; retrying after ${retry_delay}s."
  sleep "$retry_delay"
  attempt=$((attempt + 1))
  retry_delay=$((retry_delay * 2))
done
echo "[$(date --iso-8601=seconds 2>/dev/null || date)] Scheduled ETL for $COMPANY finished with status $status."
if [[ "$status" -ne 0 ]]; then
  "$OPS" notify --company "$COMPANY" --severity error \
    --message "Scheduled ETL failed with status $status after $attempt attempt(s)." || true
fi
exit "$status"
