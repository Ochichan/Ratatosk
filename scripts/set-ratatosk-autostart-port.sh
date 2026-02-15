#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <port>" >&2
  exit 1
fi

PORT="$1"
if ! [[ "${PORT}" =~ ^[0-9]+$ ]] || ((PORT < 1 || PORT > 65535)); then
  echo "Invalid port: ${PORT}" >&2
  exit 1
fi

SERVICE_NAME="ratatosk-serve.service"
OVERRIDE_DIR="${HOME}/.config/systemd/user/${SERVICE_NAME}.d"
OVERRIDE_PATH="${OVERRIDE_DIR}/override.conf"

mkdir -p "${OVERRIDE_DIR}"
cat > "${OVERRIDE_PATH}" <<EOF
[Service]
Environment=RATATOSK_PORT=${PORT}
EOF

systemctl --user daemon-reload
systemctl --user reset-failed "${SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl --user restart "${SERVICE_NAME}"

echo "Updated port override: ${OVERRIDE_PATH}"
echo "Current status:"
systemctl --user status "${SERVICE_NAME}" --no-pager -l || true
