#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

rm -rf .cache || true

"$PYTHON_BIN" specpack/theme_ablation/audit_theme_ablation.py
