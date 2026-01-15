#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

rm -rf .cache || true
rm -f outputs/backtest_regression_2026-01-20.json || true

python3 specpack/backtest_regression/run_backtest_regression.py

python3 - <<'PY'
import json
import math
from pathlib import Path

path = Path("outputs/backtest_regression_2026-01-20.json")
if not path.exists():
    raise SystemExit(f"missing output: {path}")

report = json.loads(path.read_text(encoding="utf-8"))
for key in ("dates", "results", "config"):
    if key not in report:
        raise AssertionError(f"missing key: {key}")

if len(report.get("dates", [])) != 30:
    raise AssertionError("dates count != 30")

for row in report.get("results", []):
    for section in ("baseline", "enhanced"):
        data = row.get(section, {})
        tickers = data.get("tickers", [])
        weights = data.get("weights", [])
        if len(tickers) != 5 or len(weights) != 5:
            raise AssertionError("invalid selection size")
        weight_sum = sum(weights)
        if abs(weight_sum - 1.0) > 1e-6:
            raise AssertionError("weights do not sum to 1")
        for w in weights:
            if w is None or (isinstance(w, float) and math.isnan(w)):
                raise AssertionError("invalid weight")
    for h, data in row.get("horizons", {}).items():
        for key in ("baseline_return", "enhanced_return"):
            value = data.get(key)
            if value is None or (isinstance(value, float) and math.isnan(value)):
                raise AssertionError("invalid return")

print("[backtest_regression] verify passed")
PY
