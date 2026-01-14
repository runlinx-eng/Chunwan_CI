#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

python3 - <<'PY'
import json
from pathlib import Path
import sys
import yaml

spec_path = Path("specpack/mvp_smoke/assertions.yaml")
conf = yaml.safe_load(spec_path.read_text(encoding="utf-8"))

cmd = conf["run"]["cmd"]
output_json = Path(conf["run"]["output_json"])
as_of = conf["run"]["as_of"]

print(f"[specpack] running: {cmd}")
ret = __import__("subprocess").call(cmd, shell=True)
if ret != 0:
    sys.exit(ret)

if not output_json.exists():
    raise SystemExit(f"Missing output: {output_json}")
report = json.loads(output_json.read_text(encoding="utf-8"))

results = report.get("results", [])
expected_top = conf["assertions"]["top_n"]
assert len(results) == expected_top, f"expected {expected_top} results, got {len(results)}"
assert report.get("as_of") == as_of, f"as_of mismatch: {report.get('as_of')} != {as_of}"

for row in results:
    assert row.get("data_date") <= as_of, "future data detected"
    reason = row.get("reason", "")
    for token in conf["assertions"]["reason_contains"]:
        if token not in reason:
            raise AssertionError(f"reason missing token: {token}")

core_themes = set()
for row in results:
    for hit in row.get("theme_hits", []):
        theme = hit.get("theme")
        if theme:
            core_themes.add(theme)

min_t, max_t = conf["assertions"]["theme_core_range"]
if not (min_t <= len(core_themes) <= max_t):
    raise AssertionError(f"core themes out of range: {len(core_themes)}")

print("[specpack] mvp_smoke passed")
PY
