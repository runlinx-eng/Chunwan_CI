#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

mkdir -p artifacts_metrics
"$PYTHON_BIN" tools/theme_precision_metrics.py --out artifacts_metrics/theme_precision_latest.json
"$PYTHON_BIN" specpack/theme_precision/audit_theme_precision.py
