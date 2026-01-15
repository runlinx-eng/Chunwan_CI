#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

rm -rf .cache || true
rm -f outputs/report_2026-01-20_top5.json || true

python3 - <<'PY'
import hashlib
import json
from pathlib import Path
import sys
import yaml
import subprocess

spec_path = Path("specpack/snapshot_replay/assertions.yaml")
conf = yaml.safe_load(spec_path.read_text(encoding="utf-8"))

print("[specpack] running: python3 -m pytest -q")
ret = subprocess.call("python3 -m pytest -q", shell=True)
if ret != 0:
    sys.exit(ret)

cmd = conf["run"]["cmd"]
output_json = Path(conf["run"]["output_json"])
as_of = conf["run"]["as_of"]

print(f"[specpack] running: {cmd}")
ret = subprocess.call(cmd, shell=True)
if ret != 0:
    sys.exit(ret)

if not output_json.exists():
    raise SystemExit(f"Missing output: {output_json}")
report_1 = json.loads(output_json.read_text(encoding="utf-8"))

prices_path = Path("data/snapshots") / as_of / "prices.csv"
if prices_path.exists():
    import pandas as pd

    prices = pd.read_csv(prices_path)
    counts = prices.groupby("ticker").size()
    if (counts < 121).any():
        raise AssertionError("min_count < 121 in snapshot prices.csv")
else:
    raise AssertionError(f"missing snapshot prices.csv at {prices_path}")

if conf["assertions"].get("replay"):
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        sys.exit(ret)
    report_2 = json.loads(output_json.read_text(encoding="utf-8"))
    if report_1 != report_2:
        raise AssertionError("snapshot replay output mismatch")

results = report_1.get("results", [])
expected_top = conf["assertions"]["top_n"]
assert len(results) == expected_top, f"expected {expected_top} results, got {len(results)}"
assert report_1.get("as_of") == as_of, f"as_of mismatch: {report_1.get('as_of')} != {as_of}"
issues_expected = conf["assertions"].get("issues")
if issues_expected is not None:
    if report_1.get("issues") != issues_expected:
        raise AssertionError(f"issues mismatch: {report_1.get('issues')} != {issues_expected}")

required_fields = conf["assertions"].get("required_fields", [])
for row in results:
    for field in required_fields:
        if field not in row:
            raise AssertionError(f"missing field: {field}")
    assert row.get("data_date") <= as_of, "future data detected"
    reason = row.get("reason", "")
    if isinstance(reason, dict):
        for key in ("themes_used", "concept_hits", "why_in_top5"):
            if key not in reason:
                raise AssertionError(f"reason missing key: {key}")
        if not reason.get("why_in_top5"):
            raise AssertionError("reason why_in_top5 empty")
    else:
        for token in conf["assertions"]["reason_contains"]:
            if token not in reason:
                raise AssertionError(f"reason missing token: {token}")
    indicators = row.get("indicators", {})
    for key, value in indicators.items():
        if value is None:
            raise AssertionError(f"indicator {key} is None")
        if isinstance(value, float) and value != value:
            raise AssertionError(f"indicator {key} is NaN")
    breakdown = row.get("score_breakdown", {})
    for key, value in breakdown.items():
        if value is None:
            raise AssertionError(f"score_breakdown {key} is None")
        if isinstance(value, float) and value != value:
            raise AssertionError(f"score_breakdown {key} is NaN")

core_themes = set()
for row in results:
    themes_per_row = []
    for hit in row.get("theme_hits", []):
        theme = hit.get("theme")
        if theme:
            core_themes.add(theme)
            themes_per_row.append(theme)
    if len(themes_per_row) != len(set(themes_per_row)):
        raise AssertionError("duplicate core theme in a single stock")

min_t, max_t = conf["assertions"]["theme_core_range"]
if len(core_themes) > max_t:
    raise AssertionError(f"core themes exceed max: {len(core_themes)} > {max_t}")
print(f"[specpack] core theme count: {len(core_themes)}")

print("[specpack] snapshot_replay passed")
PY
