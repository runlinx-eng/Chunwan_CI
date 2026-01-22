#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

portable_stat() {
  local target="$1"
  local python_bin=""
  if [ -n "${PYTHON_BIN:-}" ] && command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    python_bin="${PYTHON_BIN}"
  elif command -v python3 >/dev/null 2>&1; then
    python_bin="python3"
  elif command -v python >/dev/null 2>&1; then
    python_bin="python"
  else
    return 0
  fi

  "${python_bin}" - "$target" <<'PY' || true
from datetime import datetime
import os
import sys

path = sys.argv[1]
try:
    ts = os.path.getmtime(path)
except OSError:
    sys.exit(0)

dt = datetime.fromtimestamp(ts)
print(dt.strftime("%Y-%m-%d %H:%M:%S"), path)
PY
}

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

"$PYTHON_BIN" tools/prune_theme_map.py \
  --in theme_to_industry_em_2026-01-20.csv \
  --out artifacts_metrics/theme_to_industry_pruned.csv \
  --lambda 0.5 \
  --min-score 0.0 \
  --min-concepts 1

export THEME_MAP="artifacts_metrics/theme_to_industry_pruned.csv"
bash tools/verify_and_log.sh --theme-map "${THEME_MAP}"
"$PYTHON_BIN" tools/build_regression_matrix.py
if [ -z "${CANDIDATES_PATH:-}" ]; then
  build_cmd=("$PYTHON_BIN" tools/build_screener_candidates.py)
  if [ -n "${INPUT_POOL:-}" ]; then
    build_cmd+=(--input-pool "${INPUT_POOL}")
  fi
  "${build_cmd[@]}"
fi
CANDIDATES_HEALTH_PATH="${CANDIDATES_PATH:-artifacts_metrics/screener_candidates_latest.jsonl}"
"$PYTHON_BIN" tools/validate_candidates_health.py --path "${CANDIDATES_HEALTH_PATH}"
EXPORT_TOP_N="${TOP_N:-50}"
EXPORT_SORT_KEY="${SORT_KEY:-final_score}"
EXPORT_SOURCE_PATH="${CANDIDATES_PATH:-}"
export_cmd=("$PYTHON_BIN" tools/export_screener_topn.py --top-n "${EXPORT_TOP_N}" --sort-key "${EXPORT_SORT_KEY}" --modes all,enhanced,tech_only)
if [ -n "${EXPORT_SOURCE_PATH}" ]; then
  export_cmd+=(--source-path "${EXPORT_SOURCE_PATH}")
fi
"${export_cmd[@]}"
"$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_all.jsonl
"$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_enhanced.jsonl
"$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_tech_only.jsonl

latest_log="$(ls -t artifacts_logs/verify_*.txt | head -n 1)"
echo "latest_log: ${latest_log}"
grep -n "^git_rev:" "$latest_log"
grep -n "\\[specpack\\] all packs passed" "$latest_log"
grep -n "\\[verify\\] all gates passed" "$latest_log"
TARGET_FILE="artifacts_metrics/theme_precision_latest.json"
portable_stat "$latest_log" || true
portable_stat "$TARGET_FILE" || true
