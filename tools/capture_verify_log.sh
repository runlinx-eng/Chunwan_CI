#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts_logs
TS="$(date +%Y%m%d_%H%M%S)"
LOG="artifacts_logs/verify_${TS}.txt"

echo "pwd: $(pwd)" | tee -a "$LOG"
python3 -V 2>&1 | tee -a "$LOG"
uname -a 2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "git rev-parse HEAD:" | tee -a "$LOG"
git rev-parse HEAD 2>&1 | tee -a "$LOG"
echo "git status --porcelain:" | tee -a "$LOG"
git status --porcelain 2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "shasum:" | tee -a "$LOG"
for f in specpack/snapshot_replay/assertions.yaml signals.yaml theme_to_industry.csv specpack/backtest_regression/run_backtest_regression.py specpack/backtest_regression/verify.sh; do
  if [ -f "$f" ]; then
    shasum -a 256 "$f" | tee -a "$LOG"
  fi
done

echo "" | tee -a "$LOG"
echo "[gate] verify_all" | tee -a "$LOG"
./specpack/verify_all.sh 2>&1 | tee -a "$LOG"
VERIFY_STATUS=${PIPESTATUS[0]}
if [ "$VERIFY_STATUS" -ne 0 ]; then
  exit "$VERIFY_STATUS"
fi

echo "[verify] all gates passed" | tee -a "$LOG"
