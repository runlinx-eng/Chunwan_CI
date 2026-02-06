#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

rm -rf .cache || true

"$PYTHON_BIN" specpack/real_theme_effectiveness/audit_real_theme_effectiveness.py
