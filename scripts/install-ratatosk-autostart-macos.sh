#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is macOS-only. Use install-ratatosk-autostart.sh for Linux (systemd)." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LAUNCHER_PATH="${XDG_BIN_HOME:-${HOME}/.local/bin}/ratatosk"
PLIST_LABEL="dev.ratatosk.serve"
PLIST_PATH="${HOME}/Library/LaunchAgents/${PLIST_LABEL}.plist"
DOMAIN_TARGET="gui/$(id -u)"
LOG_DIR="${HOME}/Library/Logs/Ratatosk"
DATA_DIR="${REPO_ROOT}/data"

# --- prerequisites ---

"${SCRIPT_DIR}/install-ratatosk-launcher.sh"

mkdir -p "${DATA_DIR}"
mkdir -p "${LOG_DIR}"
mkdir -p "$(dirname "${PLIST_PATH}")"

# --- idempotent re-install: unload existing agent ---

launchctl bootout "${DOMAIN_TARGET}/${PLIST_LABEL}" 2>/dev/null \
  || launchctl bootout "${DOMAIN_TARGET}" "${PLIST_PATH}" 2>/dev/null \
  || true

for _ in 1 2 3 4 5; do
  if ! launchctl print "${DOMAIN_TARGET}/${PLIST_LABEL}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# --- generate plist ---

cat > "${PLIST_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${PLIST_LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${LAUNCHER_PATH}</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${HOME}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>${HOME}</string>
    <key>CARGO_HOME</key>
    <string>${HOME}/.cargo</string>
    <key>RUSTUP_HOME</key>
    <string>${HOME}/.rustup</string>
    <key>RATATOSK_BIND</key>
    <string>127.0.0.1</string>
    <key>RATATOSK_PORT</key>
    <string>6379</string>
    <key>RATATOSK_DIR</key>
    <string>${DATA_DIR}</string>
    <key>RATATOSK_AUDIT_LOG</key>
    <string>${DATA_DIR}/ratatosk-audit.log</string>
    <key>RATATOSK_AUDIT_CHAIN_STATE</key>
    <string>${DATA_DIR}/ratatosk-audit-chain.state</string>
    <key>RATATOSK_ALLOW_NO_METRICS</key>
    <string>true</string>
    <key>RATATOSK_CONN_RATE_LIMIT_MAX_ATTEMPTS</key>
    <string>200</string>
    <key>PATH</key>
    <string>${HOME}/.cargo/bin:${HOME}/.local/bin:${HOME}/.nix-profile/bin:/nix/var/nix/profiles/default/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>ThrottleInterval</key>
  <integer>5</integer>

  <key>SoftResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>65536</integer>
  </dict>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/ratatosk-serve.stdout.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/ratatosk-serve.stderr.log</string>
</dict>
</plist>
PLIST

# --- load agent ---

launchctl bootstrap "${DOMAIN_TARGET}" "${PLIST_PATH}" \
  || { sleep 1; launchctl bootstrap "${DOMAIN_TARGET}" "${PLIST_PATH}"; }

echo
echo "Installed and started: ${PLIST_LABEL}"
echo "Plist:  ${PLIST_PATH}"
echo "Logs:   ${LOG_DIR}/"
echo "Data:   ${DATA_DIR}/"
echo "Status: launchctl print ${DOMAIN_TARGET}/${PLIST_LABEL}"
echo "Logs:   tail -f ${LOG_DIR}/ratatosk-serve.stderr.log"
