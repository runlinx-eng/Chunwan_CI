#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

SNAPSHOTS="${SNAPSHOTS:-2026-01-20,2026-01-16}"
TOP_N="${TOP_N:-10}"
SKIP_PHASE10=0

usage() {
  cat <<'USAGE'
usage: bash tools/run_release_pipeline.sh [options]

options:
  --snapshots <csv>      default: 2026-01-20,2026-01-16
  --top-n <n>            default: 10
  --skip-phase10         skip phase10 gate (for development diagnostics only)
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --snapshots)
      if [ -z "${2:-}" ]; then
        echo "error: --snapshots requires a value" >&2
        exit 2
      fi
      SNAPSHOTS="$2"
      shift 2
      ;;
    --top-n)
      if [ -z "${2:-}" ]; then
        echo "error: --top-n requires a value" >&2
        exit 2
      fi
      TOP_N="$2"
      shift 2
      ;;
    --skip-phase10)
      SKIP_PHASE10=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [ "$SKIP_PHASE10" -eq 1 ]; then
  bash tools/preflight_gate.sh
else
  bash tools/preflight_gate.sh --require-clean --require-pytest
  STRICT_IO=1 bash tools/phase10_prune_verify.sh
fi

"$PYTHON_BIN" tools/run_snapshot_sweep.py --snapshots "$SNAPSHOTS" --top-n "$TOP_N" --gate

echo "[release_pipeline] ok snapshots=${SNAPSHOTS} top_n=${TOP_N} skip_phase10=${SKIP_PHASE10}"
echo "[release_pipeline] output=artifacts_metrics/regression_matrix_timeseries_latest.json"
