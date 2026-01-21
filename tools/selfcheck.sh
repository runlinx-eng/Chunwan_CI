#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

fail() {
  echo "[selfcheck] FAIL step=$1 hint=$2" >&2
  exit 1
}

pass() {
  echo "[selfcheck] PASS step=$1"
}

if [ -n "$(git status --porcelain)" ]; then
  fail "clean_tree" "commit or git stash -u before running selfcheck"
fi
pass "clean_tree"

if ! STRICT_IO=1 bash tools/phase10_prune_verify.sh; then
  fail "phase10" "check tools/phase10_prune_verify.sh output and verify logs"
fi
pass "phase10"

if ! "$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_all.jsonl; then
  fail "validate_topn_all" "ensure screener_topn_latest_all.jsonl exists"
fi
if ! "$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_enhanced.jsonl; then
  fail "validate_topn_enhanced" "ensure screener_topn_latest_enhanced.jsonl exists"
fi
if ! "$PYTHON_BIN" tools/validate_screener_topn.py --path artifacts_metrics/screener_topn_latest_tech_only.jsonl; then
  fail "validate_topn_tech_only" "ensure screener_topn_latest_tech_only.jsonl exists"
fi
pass "validate_topn"

if ! "$PYTHON_BIN" tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate; then
  fail "snapshot_sweep" "check sweep output for gate errors"
fi
pass "snapshot_sweep"

audit_tag="run_$(date +%Y%m%d_%H%M)"
if ! AUDIT_TAG="${audit_tag}" VENV_PYTHON="${PYTHON_BIN}" bash tools/backup_audit.sh; then
  fail "backup_audit" "check screener_topn_latest_meta.json and verify log paths (tag failures are non-fatal)"
fi
pass "backup_audit"

echo "[selfcheck] PASS all_steps=5 audit_dir=backups/${audit_tag}"
