# BACKLOG (P1 + P2)

## Scope

本文件与 `EXECUTION_BOARD.md` 对齐，仅覆盖：

1. `P1` 信号内核包
2. `P2` 映射精度包

## Progress

- [x] P1-1 `signals v2` 协议 + 旧协议兼容
- [x] P1-2 评分插件化（theme/tech/risk）
- [x] P1-3 报告解释字段扩展（强度细项）
- [x] P2-1 映射证据化（concept 权重）
- [x] P2-2 剪枝阈值回放（`snapshot_sweep --gate` 已通过）

## Priority Stack

| Priority | ID | Package | Task | Acceptance |
|---|---|---|---|---|
| 1 | P1-1 | P1 | `signals v2` 协议 + 旧协议兼容 | 旧 `signals.yaml` 可跑；新字段可解析且可校验 |
| 2 | P1-2 | P1 | 评分插件化（theme/tech/risk） | `final_score` 由可配置子分合成，结果可解释 |
| 3 | P1-3 | P1 | 主题强度细项写入报告 | `score_breakdown` 新增强度细项且不破坏旧字段 |
| 4 | P2-1 | P2 | 映射证据化（字段+读取规则） | 映射可区分“有证据/弱证据” |
| 5 | P2-2 | P2 | 剪枝阈值回放与阈值固化 | 主题分不再常数化，阈值可回放复现 |

## Detailed Tasks

### P1-1 Signals V2

Files:

- `src/signals.py`
- `signals.yaml`（新增可选 v2 样例）
- `tests/`（新增兼容性测试）

Must Have:

1. 字段：`family`、`formula`、`horizon_days`、`decay`、`guardrails`（均可选，但有校验规则）。
2. 旧字段路径完全保留。
3. 解析失败需明确报错，不允许静默回退到错误值。

### P1-2 Scoring Pluginization

Files:

- `src/scoring.py`
- `src/run.py`（调用改造）

Must Have:

1. 拆分函数：
   - `compute_theme_score`
   - `compute_technical_score`
   - `compute_risk_penalty`
2. 统一归一化策略（横截面）。
3. `theme-weight=0` 仍可作为 `tech_only` 消融入口。

### P1-3 Explainability Extension

Files:

- `src/report.py`

Must Have:

1. 新增强度细项（如 evidence_count/coverage/normalized_theme_strength）。
2. 保留现有键：`theme_hits`、`score_breakdown`、`reason_struct`。

### P2-1 Evidence Mapping

Files:

- `src/theme_pipeline.py`
- `tools/prune_theme_map.py`

Must Have:

1. 映射记录证据强度（来源、日期、置信级别或等效字段）。
2. 读取逻辑优先高证据映射，低证据映射降权。

### P2-2 Pruning Replay

Files:

- `specpack/theme_precision/config.json`
- `tools/run_snapshot_sweep.py`

Must Have:

1. 用 `2026-01-20,2026-01-16` 回放阈值。
2. 输出阈值结果与失败原因，写入 `artifacts_metrics`。
3. 固化最小通过阈值并记录到 `DECISIONS.md`。

## Exit Criteria (P1/P2)

1. `snapshot=2026-01-20` 下 `enhanced` 与 `tech_only` TopN 不完全一致。
2. `theme_score` 唯一值计数 > 1（同质化解除）。
3. 关键报告字段与既有校验兼容。
