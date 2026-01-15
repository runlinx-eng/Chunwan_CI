#!/usr/bin/env bash
set -euo pipefail

bash specpack/mvp_smoke/verify.sh
bash specpack/snapshot_replay/verify.sh
bash specpack/backtest_regression/verify.sh

echo "[specpack] all packs passed"
