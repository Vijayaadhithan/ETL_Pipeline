#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT="${RAG_HT_INSTALL_ROOT:-/opt/rag-ht}"
COMPANY="${1:-gainr}"

if [[ "$EUID" -ne 0 ]]; then
  echo "Run this installer with sudo/root privileges." >&2
  exit 1
fi
if ! id raght >/dev/null 2>&1; then
  echo "Create the locked-down service account first: useradd --system --home $INSTALL_ROOT --shell /usr/sbin/nologin raght" >&2
  exit 1
fi
if [[ ! -x "$INSTALL_ROOT/scripts/run_scheduled_etl.sh" || ! -x "$INSTALL_ROOT/.venv/bin/rag-ht-ops" ]]; then
  echo "Install the configured repository and virtual environment under $INSTALL_ROOT before enabling services." >&2
  exit 1
fi
cd "$INSTALL_ROOT"
ENV_FILE="$("$INSTALL_ROOT/.venv/bin/rag-ht-ops" env-file --company "$COMPANY")"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Credential file for $COMPANY is missing: $ENV_FILE" >&2
  exit 1
fi
chmod 600 "$ENV_FILE"
chown -R raght:raght "$INSTALL_ROOT"

render_unit() {
  local source="$1"
  local destination="$2"
  sed "s#/opt/rag-ht#$INSTALL_ROOT#g" "$source" > "$destination"
  chmod 0644 "$destination"
}

render_unit "$ROOT_DIR/deploy/systemd/rag-ht-etl@.service" /etc/systemd/system/rag-ht-etl@.service
render_unit "$ROOT_DIR/deploy/systemd/rag-ht-etl@.timer" /etc/systemd/system/rag-ht-etl@.timer
render_unit "$ROOT_DIR/deploy/systemd/rag-ht-status.service" /etc/systemd/system/rag-ht-status.service
sed "s#/opt/rag-ht#$INSTALL_ROOT#g" "$ROOT_DIR/deploy/logrotate/rag-ht" > /etc/logrotate.d/rag-ht
chmod 0644 /etc/logrotate.d/rag-ht

# Migrate the old per-company status unit before starting the aggregate server.
systemctl disable --now "rag-ht-status@$COMPANY.service" >/dev/null 2>&1 || true
systemctl daemon-reload
systemctl enable --now "rag-ht-etl@$COMPANY.timer"
systemctl enable --now rag-ht-status.service
systemctl list-timers "rag-ht-etl@$COMPANY.timer"
