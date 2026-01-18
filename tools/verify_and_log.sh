#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

rm -rf .cache
python3 -m pytest -q
./specpack/verify_all.sh
./tools/capture_verify_log.sh
python tools/theme_precision_metrics.py --out artifacts_metrics/theme_precision_latest.json

echo "git_rev: $(git rev-parse HEAD)"
if [ -d artifacts_logs ]; then
  latest_log="$(ls -t artifacts_logs/verify_*.txt 2>/dev/null | head -n 1 || true)"
  echo "latest_log: ${latest_log:-none}"
else
  echo "latest_log: none"
fi
