#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_DIR="${XDG_BIN_HOME:-${HOME}/.local/bin}"
LAUNCHER_PATH="${INSTALL_DIR}/ratatosk"

mkdir -p "${INSTALL_DIR}"

cat > "${LAUNCHER_PATH}" <<SCRIPT
#!/bin/sh
set -eu

REPO_ROOT="${REPO_ROOT}"
BIN_PATH="\${REPO_ROOT}/target/release/ratatosk"
DEFAULT_MAX_CLIENTS=4096
FD_HEADROOM=128

if [ ! -x "\${BIN_PATH}" ]; then
  echo "[ratatosk] Building release binary (ratatosk)..." >&2
  cargo build --manifest-path "\${REPO_ROOT}/Cargo.toml" -p ratatosk-server --release >&2
fi

# Auto-tune max clients when no explicit value is provided. This prevents
# startup preflight failures on systems where nofile is still 1024.
if [ -z "\${RATATOSK_MAX_CLIENTS:-}" ]; then
  NOFILE_SOFT="\$(ulimit -Sn 2>/dev/null || true)"
  case "\${NOFILE_SOFT}" in
    ''|*[!0-9]*)
      ;;
    *)
      if [ "\${NOFILE_SOFT}" -gt "\${FD_HEADROOM}" ]; then
        AUTO_MAX_CLIENTS=\$((NOFILE_SOFT - FD_HEADROOM))
        if [ "\${AUTO_MAX_CLIENTS}" -gt "\${DEFAULT_MAX_CLIENTS}" ]; then
          AUTO_MAX_CLIENTS="\${DEFAULT_MAX_CLIENTS}"
        fi
        if [ "\${AUTO_MAX_CLIENTS}" -lt 64 ]; then
          AUTO_MAX_CLIENTS=64
        fi
        export RATATOSK_MAX_CLIENTS="\${AUTO_MAX_CLIENTS}"
        echo "[ratatosk] Auto-tuned RATATOSK_MAX_CLIENTS=\${RATATOSK_MAX_CLIENTS} from nofile=\${NOFILE_SOFT}" >&2
      fi
      ;;
  esac
fi

exec "\${BIN_PATH}" "\$@"
SCRIPT

chmod +x "${LAUNCHER_PATH}"

echo "Installed: ${LAUNCHER_PATH}"
if [[ ":${PATH}:" != *":${INSTALL_DIR}:"* ]]; then
  echo "PATH에 ${INSTALL_DIR} 추가 필요"
  echo "예: echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.bashrc"
fi

echo "Run: ratatosk"
