#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is macOS-only. Use recover-ratatosk-autostart.sh for Linux (systemd)." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_LABEL="dev.ratatosk.serve"
DOMAIN_TARGET="gui/$(id -u)"
LOG_DIR="${HOME}/Library/Logs/Ratatosk"

cd "$(cd "${SCRIPT_DIR}/.." && pwd)"

# --- unload existing agent ---

launchctl bootout "${DOMAIN_TARGET}/${PLIST_LABEL}" 2>/dev/null || true

# --- reinstall ---

"${SCRIPT_DIR}/install-ratatosk-autostart-macos.sh"

# --- wait for startup ---

sleep 2

echo
echo "[ratatosk] status:"
launchctl print "${DOMAIN_TARGET}/${PLIST_LABEL}" 2>&1 | head -20 || true

echo
echo "[ratatosk] port probe:"
if nc -z 127.0.0.1 6379 2>/dev/null; then
  echo "TCP port 6379: open"
else
  echo "TCP port 6379: closed (server may still be starting)"
fi

echo
echo "[ratatosk] recent logs:"
tail -30 "${LOG_DIR}/ratatosk-serve.stderr.log" 2>/dev/null || echo "(no logs yet)"
