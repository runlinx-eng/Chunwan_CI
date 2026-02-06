# FAILURE MATRIX (F0-4)

## Purpose

记录熟悉阶段（F0）实跑中出现的可复现失败，提供“症状 -> 根因 -> 修复命令 -> 预防”。

## Matrix

### F-001: phase10 被 clean-tree gate 阻断

Symptom:

```text
error: working tree is dirty; commit or stash before running
```

Trigger Command:

```bash
STRICT_IO=1 bash tools/phase10_prune_verify.sh
```

Root Cause:

- `tools/phase10_prune_verify.sh` 开头强制要求 `git status --porcelain` 为空。
- 当前正在编辑文档，工作区非干净状态。

Fix:

```bash
git stash push -u -m wip_before_phase10_YYYYMMDD_HHMM
STRICT_IO=1 bash tools/phase10_prune_verify.sh
```

Prevention:

- 把“文档编辑阶段”和“门禁验收阶段”分离。
- 需要验收时先 `commit` 或 `stash`。

---

### F-002: snapshot_sweep 在 regression_matrix 阶段失败（缺指标文件）

Symptom:

```text
FileNotFoundError: missing metrics: .../artifacts_metrics/theme_map_sparsity_latest.json
```

Trigger Command:

```bash
python3 tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
```

Root Cause:

- `tools/build_regression_matrix.py` 强依赖 `artifacts_metrics/theme_map_sparsity_latest.json`。
- 当前运行链路未提前生成该指标文件。

Fix:

```bash
bash tools/preflight_gate.sh --ensure-theme-map-sparsity --theme-map theme_to_industry_em_2026-01-20.csv
python3 tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
```

Prevention:

- 在 sweep 前执行 preflight；脚本会自动补齐 `theme_map_sparsity` 指标产物。
- `tools/run_snapshot_sweep.py` 已接入 preflight，按当前流程默认会做该补齐。

---

### F-003: `enhanced` 与 `tech_only` 排名同质化

Symptom:

- `snapshot=2026-01-20` 下，`enhanced` 与 `tech_only` Top5 代码序列一致。
- 主题分表现为“统一抬升”，未形成排序差异。

Trigger Commands:

```bash
python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20
python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --theme-weight 0
```

Root Cause (current hypothesis):

- 主题命中分配过于常数化（映射覆盖偏宽）。
- 快照指标分布在 TopN 段同质化，主题分未提供额外区分度。

Fix (to be implemented in B/C packages):

1. 主题分从“命中即加常数”改为连续强度信号。
2. 收紧映射证据规则与剪枝阈值。
3. 增加同质化告警（TopN score concentration）。

Status:

- 已通过 `P1/P2` 改造缓解：`snapshot=2026-01-20` 下 `enhanced` 与 `tech_only` TopN 序列已出现差异。

Prevention:

- 把 `enhanced vs tech_only` 排名差异纳入常规 gate 指标。
