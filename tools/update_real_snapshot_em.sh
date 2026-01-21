#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

if [ $# -ne 1 ]; then
  echo "usage: $0 <AS_OF>"
  exit 1
fi

AS_OF="$1"

"$PYTHON_BIN" tools/build_real_snapshot_em.py \
  --as-of "$AS_OF" \
  --n-concepts 10 \
  --min-members 50 \
  --min-bars 160 \
  --max-tickers 600 \
  --adjust hfq

"$PYTHON_BIN" -m src.run \
  --date "$AS_OF" \
  --top 5 \
  --provider snapshot \
  --no-fallback \
  --snapshot-as-of "$AS_OF" \
  --theme-map theme_to_industry_em_2026-01-16.csv

"$PYTHON_BIN" - "$AS_OF" <<'PY'
import json
import sys
from pathlib import Path

as_of = sys.argv[1]
path = Path("outputs") / f"report_{as_of}_top5.json"
if not path.exists():
    raise SystemExit(f"missing output: {path}")
report = json.loads(path.read_text(encoding="utf-8"))
results_len = len(report.get("results", []))
print(f"results_len={results_len}")
if results_len != 5:
    raise SystemExit("results_len != 5")
PY

./tools/verify_and_log.sh
