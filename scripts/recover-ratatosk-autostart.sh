#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICE_NAME="ratatosk-serve.service"

cd "${REPO_ROOT}"

"${SCRIPT_DIR}/install-ratatosk-autostart.sh"
systemctl --user daemon-reload
systemctl --user reset-failed "${SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl --user restart "${SERVICE_NAME}"

echo
echo "[ratatosk] status:"
systemctl --user status "${SERVICE_NAME}" --no-pager -l || true

echo
echo "[ratatosk] recent logs:"
journalctl --user -u "${SERVICE_NAME}" -n 50 --no-pager || true
