# PROJECT MAP (F0-1)

## Purpose

本文件是熟悉包（F0）的第一份项目地图，用于在框架重构前冻结当前系统事实：

1. 主链路模块职责
2. 数据与评分路径
3. 运行模式与门禁关系
4. 已知风险点

## Current Stage (2026-02-07)

当前全局阶段：

1. `P0-P11` 已完成（含真实数据探针/回放、策略有效性门禁、GitHub 发布收口）。
2. `R1` 已完成：真实数据链路可运行（AkShare probe/replay 均为 `ok`）。
3. `R2` 已完成：AkShare 路径在 `theme_hits=0` 时给出显式降级解释，并在回放产物输出主题命中覆盖率。
4. `R3` 已完成：AkShare 主题证据桥接上线，`avg_topn_theme_hit_ratio` 从 `0.0` 提升到 `1.0`。
5. `A1` 已启动并完成第一步：分层持仓 + 目标函数 + 回撤约束门禁字段已接入并通过验证。
6. 图形界面封装已完成：`ui/streamlit_app.py` 可交互运行（参数输入 + 一键执行 + 报告下载）。
7. `A2-1` 已启动：完成真实股盘池首轮基线采样（`artifacts_metrics/a2_real_pool_baseline_latest.json`）。
8. `A2-2` 已完成：行情特征扩展（成交额分位、量能比、趋势稳定性、波动收缩）已接入评分与报告。
9. `A2-3` 已完成：实盘池覆盖率/特征缺失率门禁上线（`specpack/real_pool_feature_health`）。

当前验证状态：

1. `bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean` 已通过。
2. `bash tools/run_release_pipeline.sh` 已通过（strict 路径，含 `phase10`）。
3. `bash specpack/strategy_effectiveness/verify.sh` 已通过（hard 阈值通过，target 阈值告警）。
4. PR 已合并：`https://github.com/runlinx-eng/Chunwan_CI/pull/2`，merge commit=`a088cc1efb0ccd8c996b45b51a27c5323628adaf`。
5. GitHub 侧 `phase10_verify` 已通过：Run `21750613821`。
6. 合并后已在 `origin/main` 基线复验（worktree 分支 `codex/p11-main-verify`）。
7. 真实数据探针：`artifacts_metrics/real_data_probe_latest.json` -> `status=ok`, `failure_type=ok`（2026-02-06 17:12 UTC）。
8. 三日真实数据回放：`artifacts_metrics/real_data_replay_latest.json` -> `success_rate=1.0`, `global_status=ok`（2026-02-06 17:45 UTC）。
9. 回放质量指标已更新：`avg_topn_theme_hit_ratio=1.0`，`quality_flags=[]`（主题证据恢复）。
10. `strategy_effectiveness` 已升级 alpha 指标：`objective_alpha`、`avg_turnover_enhanced`、`drawdown_constraint_passed`。
11. UI 本机烟测通过：`streamlit` 可启动并返回本地访问地址。
12. 当前工作区 clean。
13. A2 基线：`as_of=2026-02-06`，`universe_count=72`，`scored_count=72`，`topn_theme_hit_ratio=1.0`。
14. A2 门禁：`bash specpack/real_pool_feature_health/verify.sh` 已通过（`status=passed`）。

当前阻塞点（进入最终目标前）：

1. 当前无硬阻塞。
2. 可选后续为策略层优化（非链路问题）。

当前点位（YOU ARE HERE）：

1. 已完成 `P11` 全部步骤（本地 strict -> PR -> CI -> merge -> main 复验 -> 留痕）。
2. 已完成 `R1`（真实数据链路可运行、回放成功率 100%）。
3. 已完成 `R2`（解释性降级可见、回放质量指标可审计）。
4. 已完成 `R3`（主题证据桥接生效，AkShare 路径主题命中恢复）。
5. 已完成 `A1-1`（组合分层持仓、回测按持仓权重计收益、策略目标函数与回撤约束门禁生效）。
6. 已完成封装任务收尾（CLI + Streamlit UI 双入口就绪）。
7. 已完成 `A2-2`（行情特征扩展）与 `A2-3`（覆盖率/缺失率门禁）。

## Next Itinerary

执行顺序（从现在开始）：

1. 发布/合并流程已闭环完成（PR #2 merged + post-merge strict 复验通过）。
2. 已进入策略 alpha：A1-2（用真实快照分层收益/回撤优化阈值，降低 target 告警数）。
3. A2（真实股盘池/行情特征）已完成 A2-2/A2-3，当前剩余 A2-1 收尾（可交易主表与过滤规则收口）。
4. A2 收尾后进入 A1-2 参数调优（压降 `strategy_effectiveness` target 告警）。

## System Boundary

- 输入：
  - `signals.yaml`
  - `theme_to_industry*.csv`
  - `data/snapshots/<date>/concept_membership.csv`
  - `data/snapshots/<date>/prices.csv|parquet`
- 输出：
  - `outputs/report_<as_of>_topN.json`
  - `outputs/report_<as_of>_topN.csv`
  - `artifacts_metrics/screener_candidates_latest.jsonl`
  - `artifacts_metrics/screener_topn_latest_*.jsonl`

## Core Runtime Flow

`src/run.py` 的实际主流程：

1. 解析参数与日期对齐（`previous_trading_date`）
2. 读取信号与主题映射（`load_signals`、`load_theme_industry_map`）
3. 主题提取与映射补齐（`DefaultThemeExtractor`、`DefaultConceptMapper`）
4. 构建数据源（`mock|snapshot|akshare`）
5. 拉取价格并做历史窗口过滤（最少 61 个交易日）
6. 指标计算（`compute_indicators`）
7. 评分与命中明细（`score_stocks`）
8. 生成报告与候选产物（`build_report` + `write_candidates`）
9. 写缓存、写 provenance、输出结果文件

## Module Responsibilities

- `src/run.py`：编排入口、缓存控制、fallback 策略、debug 埋点。
- `src/data_provider.py`：数据供给层（mock/snapshot/akshare）和快照数据完整性检查。
- `src/theme_pipeline.py`：主题提取、主题映射补齐、snapshot 候选池构建。
- `src/scoring.py`：技术指标计算与主题命中打分。
- `src/report.py`：解释字段、评分拆解、报告结构组织。
- `src/signals.py`：信号配置解析与主题映射读取。
- `tools/*.py|*.sh`：快照构建、验证、导出、回放与门禁流程。
- `specpack/*`：分项质量门禁包与总入口。

## Current Scoring Fact (As-Is)

- 技术分：
  - `0.35*momentum_20_rank + 0.20*momentum_60_rank + 0.15*volume_rank + 0.15*liquidity_rank + 0.10*trend_stability_rank + 0.05*volatility_contraction_rank`
- 主题分：
  - 对每个 signal，按命中强度（map/keyword + concept权重）加分
- 风险分：
  - 预留 `risk_penalty` 通道（默认权重为 0）
- 总分：
  - `final_score = w_theme*theme_score + w_tech*technical_score - w_risk*risk_penalty`

结论：P1/P2 已完成基础改造，主题分不再仅依赖二值命中。

## Runtime Modes to Compare (F0-2)

1. `provider=mock`：本地可复现基线模式。
2. `provider=snapshot`：真实快照主路径（推荐验收路径）。
3. `theme-weight=1`：enhanced 模式。
4. `theme-weight=0`：tech_only 模式（用于消融比较）。

## F0-2 Run Findings (2026-02-06)

已执行命令：

```bash
python3 -m src.run --date 2026-01-20 --top 5 --provider mock
python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20
python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --theme-weight 0
```

观察结论：

1. 三条命令均成功返回 Top 5。
2. `mock` 模式下分数有一定区分度，但主题分占比极高（`theme=7.1`）。
3. `snapshot` 模式下 `enhanced` 与 `tech_only` 的 Top5 代码序列一致，说明主题分在当前数据上更像“统一抬升”而非“排序差异化信号”。
4. `snapshot` 输出中多票指标几乎同质，存在区分度不足风险。

对后续包的影响：

1. B包需把主题分从“命中即常数加分”升级为“可区分强弱”的连续信号。
2. C包需收紧映射与证据约束，避免主题分常数化。
3. D包需加入同质化检测告警（如 TopN score concentration）。

## Gate/Validation Entry Points

1. `STRICT_IO=1 bash tools/phase10_prune_verify.sh`
2. `python tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate`
3. `bash specpack/verify_all.sh`

## Known Risks (Observed)

1. 环境缺少 `pytest` 时，`selfcheck` 会失败。
2. fallback 机制会保证“有结果”，但可能掩盖真实链路故障。
3. 主题映射过宽会造成主题分数虚高。
4. `report.py` 里存在 `theme_pad_*` 补位逻辑，可能影响门禁语义纯度。

## F0 Progress Tracker

- [x] F0-1 静态结构熟悉并产出本地图
- [x] F0-2 运行链路熟悉（命令实跑与差异记录）
- [x] F0-3 数据契约表
- [x] F0-4 失败矩阵
- [x] F0-5 B包改造backlog
