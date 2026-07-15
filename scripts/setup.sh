#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUN_TESTS=1
COMPANY="gainr"
COMPANY_SET=0

usage() {
  cat <<'EOF'
Usage: ./scripts/setup.sh [company] [--skip-tests]

Creates or updates .venv, installs the ETL package with MySQL/PostgreSQL
support, prepares the selected company's credential file when missing, and
verifies the installation.

Environment:
  PYTHON_BIN=/path/to/python3   Override the Python executable.
EOF
}

for argument in "$@"; do
  case "$argument" in
    --skip-tests)
      RUN_TESTS=0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -* )
      echo "Unknown argument: $argument" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ "$COMPANY_SET" -eq 1 ]]; then
        echo "Only one company slug may be supplied." >&2
        usage >&2
        exit 2
      fi
      COMPANY="$argument"
      COMPANY_SET=1
      ;;
  esac
done

if [[ ! "$COMPANY" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]]; then
  echo "Unsafe company slug: $COMPANY" >&2
  exit 2
fi

cd "$ROOT_DIR"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python executable not found: $PYTHON_BIN" >&2
  echo "Install Python 3.11 or newer, or set PYTHON_BIN." >&2
  exit 1
fi

"$PYTHON_BIN" -c '
import sys

minimum = (3, 11)
if sys.version_info < minimum:
    current = ".".join(map(str, sys.version_info[:3]))
    raise SystemExit(f"Python 3.11+ is required; found {current}")
print(f"Using Python {sys.version.split()[0]}")
'

if [[ ! -d .venv ]]; then
  echo "Creating virtual environment: .venv"
  if ! "$PYTHON_BIN" -m venv .venv; then
    echo "Unable to create .venv. On Debian/Ubuntu, install python3-venv." >&2
    exit 1
  fi
else
  echo "Reusing virtual environment: .venv"
fi

VENV_PYTHON="$ROOT_DIR/.venv/bin/python"

echo "Upgrading Python packaging tools"
"$VENV_PYTHON" -m pip install --upgrade pip setuptools wheel

if [[ "$RUN_TESTS" -eq 1 ]]; then
  echo "Installing ETL package, MySQL/PostgreSQL drivers, and test dependencies"
  "$VENV_PYTHON" -m pip install --editable '.[source-db,dev]'
else
  echo "Installing ETL package and MySQL/PostgreSQL drivers"
  "$VENV_PYTHON" -m pip install --editable '.[source-db]'
fi

ENV_FILE="$("$VENV_PYTHON" -c '
import pathlib, sys
from rag_ht_pipeline.config import load_company_config
config = load_company_config(sys.argv[1])
path = pathlib.Path(config.credentials.get("env_file", ".env"))
print(path if path.is_absolute() else config.project_root / path)
' "$COMPANY")"
mkdir -p "$(dirname "$ENV_FILE")"
if [[ ! -f "$ENV_FILE" ]]; then
  cp .env.example "$ENV_FILE"
  echo "Created $ENV_FILE from .env.example; add real credentials before database commands."
else
  echo "Keeping existing $ENV_FILE unchanged."
fi
chmod 600 "$ENV_FILE"

"$VENV_PYTHON" -c '
import sys
from rag_ht_pipeline.config import ensure_output_dirs, load_company_config
ensure_output_dirs(load_company_config(sys.argv[1]))
' "$COMPANY"

echo "Verifying package imports and company profiles"
"$VENV_PYTHON" -c '
from rag_ht_pipeline.adapters import get_adapter
from rag_ht_pipeline.config import discover_company_profiles, load_company_config
from rag_ht_pipeline.source_sync import resolve_source_backend

profiles = discover_company_profiles()
if not profiles:
    raise SystemExit("No company profiles were found")

for company_id in sorted(profiles):
    config = load_company_config(company_id)
    get_adapter(config.adapter)
    backend = resolve_source_backend(config, "configured")
    print(f"  {company_id}: adapter={config.adapter}, source={backend}")
'

"$VENV_PYTHON" -m rag_ht_pipeline.pipeline --help >/dev/null

if [[ "$RUN_TESTS" -eq 1 ]]; then
  echo "Running tests"
  "$VENV_PYTHON" -m pytest -q
else
  echo "Skipping tests."
fi

cat <<EOF

Setup complete.

Next:
  1. Configure source/destination credentials in .env or .env.<company>.
  2. Add company data or configure source database tables.
  3. Run:

     .venv/bin/python -m rag_ht_pipeline.pipeline \
       --company $COMPANY \
       --refresh-source configured \
       --apply-source-refresh \
       --run-all \
       --no-csv

Add --publish only when the validated final table should be written to the
configured destination database. After the first full baseline, use:

     ./scripts/run_scheduled_etl.sh $COMPANY --publish
EOF
