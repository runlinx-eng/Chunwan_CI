# RUNBOOK

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
# If no requirements.txt, use:
# pip install -e .
```

## One-Click Gate (clean tree required)
```bash
git status --porcelain
# must be empty
bash tools/phase10_prune_verify.sh
```
THEME_MAP controls the theme map input for a run; the one-click flow auto-points it to the pruned map.

## Common Parameters
```bash
TOP_N=10 bash tools/phase10_prune_verify.sh
SORT_KEY=score_total TOP_N=10 bash tools/phase10_prune_verify.sh
INPUT_POOL=./inputs/pool.csv TOP_N=10 bash tools/phase10_prune_verify.sh
```

## Generate Candidates
```bash
python tools/build_screener_candidates.py --snapshot-id 2026-01-20 \
  --theme-map theme_to_industry_em_2026-01-20.csv
```

## Artifacts & Checks
```bash
# TopN buckets
wc -l artifacts_metrics/screener_topn_latest_all.jsonl \
  artifacts_metrics/screener_topn_latest_enhanced.jsonl \
  artifacts_metrics/screener_topn_latest_tech_only.jsonl

# Meta fields
cat artifacts_metrics/screener_topn_latest_meta.json

# Coverage + modes
rg -n "screener_coverage_summary|modes_present" artifacts_metrics/regression_matrix_latest.json

# Regression matrix summaries
rg -n "theme_precision_summary|screener_coverage_summary" \
  artifacts_metrics/regression_matrix_latest.json
```

## Backup
```bash
# Tag the current commit
git tag -a phase10_$(date +%Y%m%d) -m "phase10 snapshot"

# Bundle for offline audit
git bundle create phase10_$(date +%Y%m%d).bundle HEAD

# Audit backup
AUDIT_TAG=run_20260120_0103 bash tools/backup_audit.sh
# Directory includes:
# - screener_topn_latest_meta.json
# - regression_matrix_latest.json
# - verify log referenced by meta.latest_log_path
```

## Troubleshooting (minimal path)
```bash
git status --porcelain
bash tools/phase10_prune_verify.sh
cat artifacts_metrics/screener_topn_latest_meta.json | rg -n "latest_log_path"
rg -n "\\[specpack\\]|\\[verify\\]|theme_precision|screener_coverage" \
  "$(python - <<'PY'\nimport json\nfrom pathlib import Path\nmeta = json.loads(Path('artifacts_metrics/screener_topn_latest_meta.json').read_text())\nprint(meta.get('latest_log_path',''))\nPY\n)"
```
