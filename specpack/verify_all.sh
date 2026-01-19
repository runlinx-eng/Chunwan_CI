#!/usr/bin/env bash
set -euo pipefail

bash specpack/mvp_smoke/verify.sh
bash specpack/snapshot_replay/verify.sh
bash specpack/backtest_regression/verify.sh
bash specpack/theme_explain/verify.sh
bash specpack/concept_data_health/verify.sh
bash specpack/real_snapshot_smoke/verify.sh
bash specpack/real_theme_effectiveness/verify.sh
bash specpack/theme_ablation/verify.sh
bash specpack/theme_precision/verify.sh
bash specpack/theme_map_sparsity/verify.sh

echo "[specpack] all packs passed"
