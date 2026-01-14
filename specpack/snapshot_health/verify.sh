#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

rm -rf .cache outputs/report_2026-01-20_top5.json || true

python3 specpack/snapshot_health/audit_snapshot_health.py
