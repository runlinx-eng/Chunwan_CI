#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

"$PYTHON_BIN" specpack/screener_coverage/audit_screener_coverage.py
