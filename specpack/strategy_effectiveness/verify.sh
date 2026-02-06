#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

bash specpack/backtest_regression/verify.sh
"$PYTHON_BIN" tools/strategy_effectiveness_metrics.py \
  --input outputs/backtest_regression_2026-01-20.json \
  --out artifacts_metrics/strategy_effectiveness_latest.json \
  --write-history
"$PYTHON_BIN" specpack/strategy_effectiveness/audit_strategy_effectiveness.py
