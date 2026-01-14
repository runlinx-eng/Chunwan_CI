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
echo "shasum:" | tee -a "$LOG"
for f in src/run.py src/data_provider.py src/scoring.py src/report.py specpack/snapshot_replay/verify.sh specpack/backtest_smoke/verify.sh; do
  if [ -f "$f" ]; then
    shasum -a 256 "$f" | tee -a "$LOG"
  fi
done

echo "" | tee -a "$LOG"

echo "[gate] pytest" | tee -a "$LOG"
python3 -m pytest -q 2>&1 | tee -a "$LOG"
PYTEST_STATUS=${PIPESTATUS[0]}
if [ "$PYTEST_STATUS" -ne 0 ]; then
  exit "$PYTEST_STATUS"
fi

echo "" | tee -a "$LOG"

echo "[gate] snapshot_replay" | tee -a "$LOG"
bash specpack/snapshot_replay/verify.sh 2>&1 | tee -a "$LOG"
SNAPSHOT_STATUS=${PIPESTATUS[0]}
if [ "$SNAPSHOT_STATUS" -ne 0 ]; then
  exit "$SNAPSHOT_STATUS"
fi

echo "" | tee -a "$LOG"

echo "[gate] backtest_smoke" | tee -a "$LOG"
bash specpack/backtest_smoke/verify.sh 2>&1 | tee -a "$LOG"
BACKTEST_STATUS=${PIPESTATUS[0]}
if [ "$BACKTEST_STATUS" -ne 0 ]; then
  exit "$BACKTEST_STATUS"
fi

echo "[verify] all gates passed" | tee -a "$LOG"
