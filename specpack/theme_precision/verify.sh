#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts_metrics
python tools/theme_precision_metrics.py --out artifacts_metrics/theme_precision_latest.json
python specpack/theme_precision/audit_theme_precision.py
