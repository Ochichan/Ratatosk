#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is macOS-only. Use set-ratatosk-autostart-port.sh for Linux (systemd)." >&2
  exit 1
fi

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <port>" >&2
  exit 1
fi

PORT="$1"
if ! [[ "${PORT}" =~ ^[0-9]+$ ]] || ((PORT < 1 || PORT > 65535)); then
  echo "Invalid port: ${PORT}" >&2
  exit 1
fi

PLIST_LABEL="dev.ratatosk.serve"
PLIST_PATH="${HOME}/Library/LaunchAgents/${PLIST_LABEL}.plist"
DOMAIN_TARGET="gui/$(id -u)"

if [[ ! -f "${PLIST_PATH}" ]]; then
  echo "Plist not found: ${PLIST_PATH}" >&2
  echo "Run install-ratatosk-autostart-macos.sh first." >&2
  exit 1
fi

# --- update port in plist ---

plutil -replace EnvironmentVariables.RATATOSK_PORT -string "${PORT}" "${PLIST_PATH}"

# --- reload agent ---

launchctl bootout "${DOMAIN_TARGET}/${PLIST_LABEL}" 2>/dev/null || true
launchctl bootstrap "${DOMAIN_TARGET}" "${PLIST_PATH}"

echo "Updated port to ${PORT}"
echo "Plist: ${PLIST_PATH}"
echo
echo "Current status:"
launchctl print "${DOMAIN_TARGET}/${PLIST_LABEL}" 2>&1 | head -10 || true
