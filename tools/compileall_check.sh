#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

if PYTHONDONTWRITEBYTECODE=1 "$PYTHON_BIN" -m compileall tools src; then
  echo "[compileall] PASS"
  exit 0
fi

echo "[compileall] FAIL: PermissionError or cache issues; try PYTHONDONTWRITEBYTECODE=1 or fix cache permissions" >&2
exit 1
