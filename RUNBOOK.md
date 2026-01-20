# RUNBOOK

## 一键流程
```bash
STRICT_IO=1 bash tools/phase10_prune_verify.sh
```

## 常用命令
```bash
./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-20
./.venv/bin/python tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
./.venv/bin/python tools/validate_screener_topn.py
./.venv/bin/python tools/inspect_candidates_diversity.py --path artifacts_metrics/screener_candidates_latest.jsonl
```

## 环境约束
- 统一使用 `./.venv/bin/python` 与 `./.venv/bin/pip`。
- `phase10_prune_verify.sh` 需要 clean tree。

## 说明：theme_total 常数（expected）
在 snapshot_universe 模式下，theme_total 可能因为概念命中稀疏或主题映射收敛而成为常数；sweep gate 只看概念多样性（`enhanced_concept_hit_sig_sets` 与概念非空率）。

排障与常见坑：`TROUBLESHOOTING.md`
