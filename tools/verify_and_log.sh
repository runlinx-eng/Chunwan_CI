#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

if [ "${1:-}" = "--theme-map" ]; then
  if [ -z "${2:-}" ]; then
    echo "error: --theme-map requires a path"
    exit 2
  fi
  export THEME_MAP="$2"
  shift 2
fi
if [ "$#" -gt 0 ]; then
  echo "error: unknown arguments: $*"
  exit 2
fi

rm -rf .cache
"$PYTHON_BIN" -m pytest -q
./specpack/verify_all.sh
./tools/capture_verify_log.sh
"$PYTHON_BIN" tools/theme_precision_metrics.py --out artifacts_metrics/theme_precision_latest.json

echo "git_rev: $(git rev-parse HEAD)"
if [ -d artifacts_logs ]; then
  latest_log="$(ls -t artifacts_logs/verify_*.txt 2>/dev/null | head -n 1 || true)"
  echo "latest_log: ${latest_log:-none}"
else
  echo "latest_log: none"
fi
