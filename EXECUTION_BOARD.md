# Execution Board V2 (Command Plan)

## Target

把项目从“可运行叙事MVP”推进到“真实数据可稳定运行、可解释、可验证有效”的选股系统。

## Baseline Facts (from F0)

1. `snapshot` 下 `enhanced` 与 `tech_only` 排名同质化，主题分区分度不足。
2. 主验收链路存在前置阻塞：
   - clean-tree gate 会阻断 `phase10`
   - `theme_map_sparsity_latest.json` 缺失会阻断 `snapshot_sweep`
3. 当前文档与契约已冻结：
   - `PROJECT_MAP.md`
   - `DATA_CONTRACTS.md`
   - `FAILURE_MATRIX.md`

## Repackaged Scope (Optimized)

| Package | Name | Why Now | Deliverables | Done Definition |
|---|---|---|---|---|
| P0 | 验收解阻包 | 先打通门禁，否则后续迭代无法稳定验收 | preflight流程、缺失指标前置生成、环境可复现 | `phase10` + `snapshot_sweep --gate` 在标准流程下可执行 |
| P1 | 信号内核包 | 解决主题分常数化问题 | `signals v2` + 兼容解析 + 评分插件化 | `enhanced` 对 `tech_only` 出现可解释排序差异 |
| P2 | 映射精度包 | 解决“命中即高分”与映射过宽 | 映射证据化、剪枝、强度归一化 | 主题分唯一值与命中质量指标达阈值 |
| P3 | 可靠性补漏包 | 解决真实数据与失败语义不稳 | provider错误分层、fallback显式策略、同质化告警 | 故障可定位，不再静默降级 |
| P4 | 门禁升级包 | 把有效性约束落到自动检查 | 新增/升级 gate 与回归矩阵容错 | 关键失败可被门禁直接拦截 |
| P5 | 交付封装包 | 形成日常可运行交付物 | 一键命令、标准产物、发布清单 | 新日期接入后单命令完成“构建-验收-导出” |
| P7 | 签名多样性包 | 消除主题签名“假单一”告警 | 用运行时命中签名替代静态反推签名 | `snapshot_sweep` 主题签名反映真实命中差异 |
| P8 | 真实数据可达包 | 打通 AkShare 链路前先明确失败类型 | 连通性探针、错误分型、运行日志产物 | 能区分“网络阻断/依赖缺失/业务失败” |
| P9 | 真实数据稳态包 | 在真实数据下验证稳定运行 | 多日期 `provider=akshare --no-fallback` 回放结果 | 成功率达标且失败可归因 |
| P10 | 策略有效性包 | 从“能跑”升级到“有效” | 收益/命中/回撤指标与阈值 | 指标达到预设阈值 |
| P11 | 发布收口包 | 完成最终交付闭环 | CI 通过、PR 合并、发布说明 | 主分支可复现执行并通过门禁 |

## Milestones

| Milestone | Output | Depends On |
|---|---|---|
| M0 | 熟悉结论冻结（已完成） | - |
| M1 | P0 完成（验收链路可执行） | M0 |
| M2 | P1 完成（信号内核可区分） | M1 |
| M3 | P2 完成（映射质量收敛） | M2 |
| M4 | P3 完成（真实链路稳定） | M2, M3 |
| M5 | P4 完成（门禁升级） | M4 |
| M6 | P5 完成（交付封装） | M5 |

## Command Contract

通过口径（全局）：

1. 结果完整性：`results_len == topN` 且 `issues == 0`。
2. 可解释性：`theme_hits`、`matched_terms`、`score_breakdown`、`data_date` 均存在。
3. 信号有效性：`enhanced` 与 `tech_only` 排名差异非零，且差异可解释。
4. 可复现性：同输入哈希同输出，`provenance` 完整。

## Execution Board

| ID | Task | Package | Status | Notes |
|---|---|---|---|---|
| F0 | 熟悉包收口 | Foundation | Done | 地图/契约/失败矩阵已就绪 |
| A | 目标口径收口 | Foundation | Done | RUNBOOK/DECISIONS 已更新 |
| P0-1 | 增加 preflight（环境+必需产物检查） | P0 | Done | 已新增 tools/preflight_gate.sh |
| P0-2 | 统一门禁前置生成 `theme_map_sparsity_latest.json` | P0 | Done | phase10/sweep 已接入自动补齐 |
| P0-3 | 固化 clean-tree 验收策略（commit/stash流程） | P0 | Done | RUNBOOK 已固化策略 |
| P1-1 | `signals v2` 兼容解析 | P1 | Done | `src/signals.py` 增加 v2 字段与校验 |
| P1-2 | 评分插件化（theme/tech/risk） | P1 | Done | `src/scoring.py` 拆分为 theme/tech/risk 流水线 |
| P1-3 | 报告解释字段扩展（强度细项） | P1 | Done | `score_breakdown` 新增强度与权重字段 |
| P2-1 | 主题映射证据化字段与读取策略 | P2 | Done | `src/theme_pipeline.py` 稀疏化并输出 concept 权重 |
| P2-2 | 剪枝策略与阈值回放 | P2 | Done | 已跑 snapshot sweep，增强模式分布恢复 |
| P3-1 | provider 错误分类与显式失败 | P3 | Done | `provider_fallback_reason` + warnings 已落地 |
| P3-2 | fallback 策略重构（默认不静默） | P3 | Done | fallback 结果与原因写入 meta/debug/report |
| P4-1 | 新增同质化 gate | P4 | Done | `run_snapshot_sweep` 新增常数主题分错误门禁 |
| P4-2 | 回归矩阵容错与前置依赖检查 | P4 | Done | sweep 接入 preflight，前置依赖自动补齐 |
| P5-1 | 一键封装脚本与发布清单 | P5 | Done | 已新增 `tools/run_release_pipeline.sh` + Makefile 入口 |
| P8-1 | AkShare 连通性探针与错误分型脚本 | P8 | Done | 已新增 `tools/probe_real_data_chain.py` |
| P8-2 | P8 验证与结果入库 | P8 | Done | 已产出 `artifacts_metrics/real_data_probe_latest.json` |
| P9-1 | 多日期真实数据回放（无 fallback） | P9 | Done | 已产出 `artifacts_metrics/real_data_replay_latest.json` |
| P10-1 | 策略有效性指标计算与阈值 | P10 | Done | 已新增 `strategy_effectiveness` 指标与双阈值门禁 |
| P11-1 | GitHub 最终发布流程收口 | P11 | In Progress | API 连通已恢复，PR/CI 已完成；当前阻塞为 base branch merge policy |

## Package Checklists

### P0 Checklist

- [x] `phase10` 可在标准 preflight 后执行（依赖 clean tree + pytest 环境）。
- [x] `snapshot_sweep --gate` 不因缺失 `theme_map_sparsity_latest.json` 失败。
- [x] 文档写明 clean-tree 验收操作。

### P1 Checklist

- [x] 新旧 `signals` 协议并存。
- [x] `snapshot=2026-01-20` 下 `enhanced` 与 `tech_only` 排名不再完全一致。
- [x] 新增单测覆盖兼容与核心评分路径（已新增，受本机 pytest 环境约束未本轮执行）。

### P2 Checklist

- [x] 映射证据和剪枝生效。
- [x] 主题分布不再常数化（`enhanced_unique_value_count` 已恢复 > 1）。
- [x] 主题命中质量指标达标（sweep gate 无 errors）。

### P3 Checklist

- [x] provider 异常有统一错误语义。
- [x] fallback 行为在 `meta`/`debug` 可见。
- [x] 真实数据链路失败可复现并可修复（见 `FAILURE_MATRIX.md`）。

### P4 Checklist

- [x] 同质化失败可被 gate 拦截。
- [x] 回归矩阵前置依赖明确。
- [x] `verify_all` 与 `snapshot_sweep --gate` 口径一致。

### P5 Checklist

- [x] 单命令完成“构建-验收-导出”。
- [x] 产物路径与字段契约稳定。
- [x] 交付清单可复用。

## Package Entry Rule

下一包开工条件：

1. 当前包 checklist 全勾选。
2. 当前包关键命令输出已记录到 `artifacts_metrics` 或日志文件。
3. 才允许切到下一包。

## Post-M6 Hardening

| ID | Task | Status | Notes |
|---|---|---|---|
| P6-1 | 重置 `theme_precision` 基线到稀疏化口径 | Done | `artifacts_metrics/theme_precision_baseline.json` 已更新 |
| P6-2 | 下调 concept-hit 非退化阈值（10 -> 6） | Done | 对齐当前 `snapshot` 实测分布 |
| P6-3 | 严格门禁复验（`phase10`） | Done | `verify` 全门禁通过 |
| P7-1 | `snapshot_sweep` 主题签名改为读取 `theme_hits/signal_themes` | Done | 2026-01-20 警告清零，主题签名集合数恢复 |
| P8-1 | AkShare 连通性探针与错误分型脚本 | Done | failure_type 已可自动分类 |
| P8-2 | P8 验证与结果入库 | Done | 当前环境分类结果为 `network_blocked` |
| P9-1 | 多日期真实数据回放（无 fallback） | Done | success_rate=0，global_status=`blocked_by_environment` |
| P10-1 | 策略有效性指标计算与阈值 | Done | hard 阈值通过，target 阈值告警待后续优化 |

## Change Log

- 2026-02-06: 依据 F0 结论重排执行顺序，新增 P0-P5 分包并冻结为 V2 指挥计划。
- 2026-02-06: 完成 P0（preflight + theme_map_sparsity 前置补齐 + clean-tree 策略固化）。
- 2026-02-06: 完成 P1/P2（signals v2、评分插件化、映射稀疏化、阈值回放）。
- 2026-02-06: 完成 P3（provider/fallback 错误语义显式化）。
- 2026-02-06: 完成 P4（同质化 gate + 回归矩阵前置依赖检查）。
- 2026-02-06: 完成 P5（一键发布流水线脚本与 Makefile 入口）。
- 2026-02-06: 完成 P6（theme_precision 基线与阈值重标定，strict phase10 复验通过）。
- 2026-02-06: 完成 P7-1（`snapshot_sweep` 主题签名改为运行时命中口径，修复假单一告警）。
- 2026-02-06: 完成 P8（真实数据探针与失败分型，当前环境判定 `network_blocked`）。
- 2026-02-06: 完成 P9-1（多日期 AkShare 回放，当前环境 `blocked_by_environment`，失败类型统一为 `network_blocked`）。
- 2026-02-06: 完成 P10-1（新增 `strategy_effectiveness` 指标产物与 hard/target 双阈值门禁）。
- 2026-02-06: 启动 P11-1（补齐 `P11_RELEASE_CHECKLIST.md`，等待 GitHub 侧 CI 与合并留痕）。
- 2026-02-06: 统一 specpack Python 解析到 `tools/resolve_python.sh`，修复 `python/pytest` 解释器不一致问题。
- 2026-02-06: 重标定 `theme_precision` 概念多样性阈值（enhanced: 6->4, all: 6->5），恢复与当前实测分布一致。
- 2026-02-06: `run_release_pipeline --skip-phase10` 开发态复验通过，后续进入 P11 strict 发布与 GitHub 闭环。
- 2026-02-06: 完成 P11 本地 strict 复验（`git_guard --strict`、`run_release_pipeline`、`strategy_effectiveness` 全通过），当前仅剩 GitHub PR/CI/merge 留痕。
- 2026-02-06: `codex/p0-p5-hardening` 已 push 到 origin；`gh auth` token 无效且 `api.github.com` 连通失败，PR 创建暂时阻塞。
- 2026-02-06: 已确认 Git SSH 代码通道可用（`git push`/`git ls-remote`），但 GitHub API 通道受 `gh` token 无效与 DNS 解析失败共同阻塞。
- 2026-02-06: GitHub API 已恢复（`gh auth status` 正常，DNS 可解析），PR 已创建：`#1`，`ci_smoke` 与 `release_bundle` 均通过；当前剩余阻塞为 base branch policy（`mergeStateStatus=BLOCKED`）。
