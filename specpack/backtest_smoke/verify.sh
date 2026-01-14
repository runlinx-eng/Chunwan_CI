#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

rm -rf .cache || true
rm -f outputs/backtest_smoke_2026-01-20.json || true

python3 specpack/backtest_smoke/run_backtest_smoke.py

python3 - <<'PY'
import json
import math
from pathlib import Path

path = Path("outputs/backtest_smoke_2026-01-20.json")
if not path.exists():
    raise SystemExit(f"missing output: {path}")

report = json.loads(path.read_text(encoding="utf-8"))
for key in ("dates", "results", "summary"):
    if key not in report:
        raise AssertionError(f"missing key: {key}")

for row in report.get("results", []):
    for section in ("baseline", "enhanced"):
        data = row.get(section, {})
        if "tickers" not in data or "forward_return" not in data:
            raise AssertionError("missing baseline/enhanced fields")
        value = data.get("forward_return")
        if value is None or (isinstance(value, float) and math.isnan(value)):
            raise AssertionError("invalid forward_return")

summary = report.get("summary", {})
for section in ("baseline", "enhanced"):
    stats = summary.get(section, {})
    for metric in ("mean", "std", "win_rate"):
        value = stats.get(metric)
        if value is None or (isinstance(value, float) and math.isnan(value)):
            raise AssertionError("invalid summary metric")

print("[backtest_smoke] verify passed")
PY
