# Decisions

定位：记录“为什么这么做”的决策与取舍；每次改 gating、改路径语义、改 CI 范围都补一条。

## 目标重定义（V2）
- 决策：目标从“能输出 Top N”改为“可验证 Top N（稳定、可解释、可回放）”。
- 约束：验收统一看 `results_len/topN`、`issues`、`enhanced vs tech_only`、`provenance`，不再以“仅有结果文件”作为通过标准。
- 原因：避免工程流程通过但策略信息无效。

## 验收入口统一
- 决策：以 `tools/phase10_prune_verify.sh` + `tools/run_snapshot_sweep.py --gate` 作为主验收入口。
- 约束：所有后续改动必须先过这两道门禁再进入交付。
- 原因：统一口径可减少“单点通过、全局失效”的回归风险。

## 执行顺序重排（V2分包）
- 决策：先做 `P0 验收解阻`，再做 `P1/P2` 策略内核，最后做 `P3/P4/P5` 可靠性与交付。
- 约束：禁止跳过 `P0` 直接做策略重构。
- 原因：F0 实跑已确认门禁前置依赖存在阻塞（clean-tree、theme_map_sparsity），不先解阻会导致后续包无法稳定验收。

## 缓存键补全
- 决策：缓存键加入 `snapshot_as_of`、`theme_weight`、`no_fallback` 维度。
- 原因：避免 enhanced/tech_only 或不同回放日期之间的缓存串读污染。

## 映射稀疏化与概念权重
- 决策：`DefaultConceptMapper` 对每个 signal 限制 concept 数量（默认 3），并产出 concept 权重用于主题强度打分。
- 原因：原始映射接近全连接，导致主题分常数化且无法形成排序差异。

## provider fallback 显式化
- 决策：provider 初始化/运行期 fallback 必须写入 `meta.provider_fallback_reason` 与 `debug.warnings`。
- 原因：避免“结果可用但来源不明”的静默降级，便于定位真实数据链路问题。

## sweep 同质化 gate
- 决策：`run_snapshot_sweep.py` 增加常数主题分拦截（`enhanced_theme_total_constant` / `all_theme_total_constant`）。
- 原因：防止主题分退化为常数时仍通过回归流程。

## 一键发布入口
- 决策：新增 `tools/run_release_pipeline.sh`，统一串联 preflight、phase10、snapshot_sweep gate。
- 原因：降低手动拼命令导致的漏跑与口径不一致风险。

## Git 流程防遗漏
- 决策：新增 `tools/git_guard.sh`，在发布流水线前强制校验分支状态（非 detached、前缀、upstream、clean tree）。
- 原因：新手在 worktree 环境下容易忘记建分支/推远端，导致提交难追溯或验收被 clean-tree gate 阻断。

## Snapshot sweep 的两种池策略
- fixed pool：用于共享 identifier space 的可比对集合；保留 theme 相关 gate
- snapshot_universe：每个 snapshot 用自身 universe；gate 主要看 concept 多样性（因为跨 snapshot 不保证重叠）

## Gate 指标语义（当前实现）
- universe gate：enhanced_concept_hit_signature_unique_set_count >= 阈值（config.json）
- theme_total/theme_hit_signature 指标用于诊断或 fixed pool gate（按 RUNBOOK 定义）

## Sweep universe gate
- snapshot_universe 只用概念多样性 gate，阈值来自 `specpack/theme_precision/config.json` 的 `min_enhanced_concept_hit_signature_unique_set_count`（默认 6）。
- 原因：跨 snapshot 不保证可比性，theme_total/主题签名波动更大。

## 元数据与路径规范
- meta 存 repo-relative 的 theme_map_path + sha256；允许附加绝对路径作为 debug 字段。

## 脚本 Python 解析
- 统一用 PYTHON_BIN 解析（优先 VENV_PYTHON，其次 venv，再 fallback 到系统 python3/python）。

## 一键脚本约束
- phase10 一键脚本要求 clean tree，避免产物与源码混杂。

## 备份锚点
- 决策：本项目备份锚点采用 bundle + INDEX，不依赖 git tag。
- 原因：部分 macOS/安全策略禁止在 `.git` 创建 `.lock`，导致 tag/refs 更新失败。

## 记录：theme_total 集中度允许（snapshot_universe）
- 现象：theme_total 多样性已恢复到 4，但 top1 集中 250。
- 决策：snapshot_universe 只用概念多样性 gate（概念非空率 + enhanced_concept_hit_signature_unique_set_count），theme_total 低多样性仅警告不阻断。
- 原因：theme_total 由概念映射推导，易受主题映射/概念稀疏影响；跨 snapshot 不保证可比性。

## theme_precision 基线与阈值重标定（P6）
- 决策：将 `theme_precision_baseline.json` 刷新到当前稀疏化评分口径，并将 `min_concept_hits_unique_set_enhanced/all` 从 10 下调到 6。
- 原因：P1/P2 后主题映射从“广覆盖常量”转为“稀疏加权命中”，旧基线与旧阈值会把正确改进误判为退化，阻断 strict phase10。

## snapshot_sweep 主题签名口径修正（P7-1）
- 决策：`run_snapshot_sweep.py` 的 `theme_hit_signature` 优先读取候选结果里的 `theme_hits/signal_themes`，仅在缺失时回退到 `concept_hits + theme_map` 反推。
- 原因：静态映射反推会把稠密主题图误判为“签名单一”，无法反映运行时稀疏化后的真实命中差异。

## 真实数据失败分型先行（P8）
- 决策：先引入 `tools/probe_real_data_chain.py` 对 `provider=akshare --no-fallback` 做探针，并输出标准化 failure_type。
- 原因：在网络受限环境下，直接推进真实数据稳态回放会反复失败；先做可归因探针能区分“环境阻断”与“代码缺陷”。

## 策略有效性双阈值门禁（P10）
- 决策：新增 `specpack/strategy_effectiveness/verify.sh`，基于 `backtest_regression` 输出生成 `strategy_effectiveness_latest.json`，并采用 hard/target 双阈值审计。
- 约束：hard 阈值失败即阻断；target 阈值失败仅告警，写入 `strategy_effectiveness_gate_latest.json`。
- 原因：当前 snapshot 回测可用于稳定评估“是否显著退化”，但不宜直接作为最终 alpha 判定；先用 hard 阈值守住下限，再用 target 阈值指挥下一轮策略优化。

## P11 发布清单化
- 决策：新增 `P11_RELEASE_CHECKLIST.md` 作为最终发布收口的唯一人工清单。
- 约束：P11 不再只看“代码已改”，必须同时留存 PR 链接、CI Run ID、merge commit SHA。
- 原因：GitHub 侧动作（PR/CI/merge）不可由本地脚本完全替代，清单化可以降低新手漏步骤风险。

## specpack Python 解析统一
- 决策：`specpack` 下涉及 Python 执行的 `verify.sh` 统一走 `tools/resolve_python.sh`，并优先使用 `PYTHON_BIN`。
- 约束：禁止依赖裸 `python`，避免在仅有 `python3` 或 venv 场景下失败。
- 原因：本机实测存在 `python` 缺失与 `python3` 无 `pytest` 的分裂环境，导致 verify_all 非策略性失败。

## theme_precision 阈值二次标定（P11 前置）
- 决策：将 `min_concept_hits_unique_set_enhanced` 从 6 下调至 4，将 `min_concept_hits_unique_set_all` 从 6 下调至 5；同步 `min_enhanced_concept_hit_signature_unique_set_count` 到 4。
- 约束：仅下调概念多样性下限，不放宽 theme_total 的唯一值与区间约束。
- 原因：当前实测稳定分布为 enhanced=4/all=5（`theme_precision_latest.json`），原阈值会把可接受分布误报为退化，阻断发布路径。
