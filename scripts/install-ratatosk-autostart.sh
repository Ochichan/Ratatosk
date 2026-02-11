#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICE_NAME="ratatosk-serve.service"
LEGACY_SERVICE_NAME="ratatosk.service"
SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"
SERVICE_PATH="${SYSTEMD_USER_DIR}/${SERVICE_NAME}"
LEGACY_SERVICE_PATH="${SYSTEMD_USER_DIR}/${LEGACY_SERVICE_NAME}"
LAUNCHER_PATH="${XDG_BIN_HOME:-${HOME}/.local/bin}/ratatosk"

mkdir -p "${SYSTEMD_USER_DIR}"

"${SCRIPT_DIR}/install-ratatosk-launcher.sh"

cat > "${SERVICE_PATH}" <<UNIT
[Unit]
Description=Ratatosk server (RESP3)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${REPO_ROOT}
Environment=RATATOSK_BIND=127.0.0.1
Environment=RATATOSK_PORT=6380
Environment=RATATOSK_DATA_DIR=${REPO_ROOT}/data
ExecStart=${LAUNCHER_PATH}
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
UNIT

if [[ -e "${LEGACY_SERVICE_PATH}" && ! -L "${LEGACY_SERVICE_PATH}" ]]; then
  BACKUP_PATH="${LEGACY_SERVICE_PATH}.legacy.bak.$(date +%Y%m%d%H%M%S)"
  mv "${LEGACY_SERVICE_PATH}" "${BACKUP_PATH}"
  echo "Backed up legacy unit: ${BACKUP_PATH}"
fi

systemctl --user disable --now "${LEGACY_SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl --user mask "${LEGACY_SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl --user unmask "${SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl --user daemon-reload
systemctl --user enable --now "${SERVICE_NAME}"
systemctl --user restart "${SERVICE_NAME}"

if command -v loginctl >/dev/null 2>&1; then
  if loginctl enable-linger "${USER}" >/dev/null 2>&1; then
    echo "Enabled linger for ${USER} (starts on boot without login)."
  else
    echo "Could not enable linger automatically."
    echo "Run manually if needed: sudo loginctl enable-linger ${USER}"
  fi
fi

echo "Installed and started: ${SERVICE_NAME}"
echo "Service file: ${SERVICE_PATH}"
echo "Status: systemctl --user status ${SERVICE_NAME} --no-pager"
echo "Logs:   journalctl --user -u ${SERVICE_NAME} -f"
