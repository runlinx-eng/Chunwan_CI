#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LATEST="artifacts_metrics/theme_precision_latest.json"
BASELINE="artifacts_metrics/theme_precision_baseline.json"

mkdir -p "$(dirname "$BASELINE")"

if [ ! -f "$LATEST" ]; then
  echo "missing latest metrics: $LATEST" >&2
  exit 1
fi

cp "$LATEST" "$BASELINE"
echo "theme_precision baseline updated: $LATEST -> $BASELINE"
