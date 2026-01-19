#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree is dirty; commit or stash before running"
  git status --porcelain
  exit 1
fi

if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

mkdir -p artifacts_metrics

python tools/prune_theme_map.py \
  --in theme_to_industry_em_2026-01-20.csv \
  --out artifacts_metrics/theme_to_industry_pruned.csv \
  --lambda 0.5 \
  --min-score 0.0 \
  --min-concepts 1

bash tools/verify_and_log.sh --theme-map artifacts_metrics/theme_to_industry_pruned.csv
python tools/build_regression_matrix.py
python tools/export_screener_topn.py --top-n 50

latest_log="$(ls -t artifacts_logs/verify_*.txt | head -n 1)"
echo "latest_log: ${latest_log}"
grep -n "^git_rev:" "$latest_log"
grep -n "\\[specpack\\] all packs passed" "$latest_log"
grep -n "\\[verify\\] all gates passed" "$latest_log"
stat -f "%Sm %N" "$latest_log"
stat -f "%Sm %N" artifacts_metrics/theme_precision_latest.json
