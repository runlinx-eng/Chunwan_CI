# PROJECT MAP (F0-1)

## Purpose

本文件是熟悉包（F0）的第一份项目地图，用于在框架重构前冻结当前系统事实：

1. 主链路模块职责
2. 数据与评分路径
3. 运行模式与门禁关系
4. 已知风险点

## Current Stage (2026-02-06)

当前全局阶段：

1. `P0-P10` 已完成（含真实数据探针/回放、策略有效性门禁）。
2. `P11` 进行中，目标是完成 GitHub 侧闭环（CI/PR/merge 留痕）。

当前验证状态：

1. `bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean` 已通过。
2. `bash tools/run_release_pipeline.sh` 已通过（strict 路径，含 `phase10`）。
3. `bash specpack/strategy_effectiveness/verify.sh` 已通过（hard 阈值通过，target 阈值告警）。
4. 当前工作区 `git status` 为 clean。

当前阻塞点（进入最终发布前）：

1. 必须完成 GitHub 侧 `CI -> PR -> merge` 留痕。
2. 当前 `gh` 环境阻塞：`gh auth status` 显示 token 无效。
3. 当前网络阻塞：`curl https://github.com` 与 `curl https://api.github.com` 均报 `Could not resolve host`（DNS 解析失败）。
4. 合并到 `main` 后需再跑一次 strict 路径并回写留痕。

当前点位（YOU ARE HERE）：

1. 已到 `P11-2`（分支已 push，等待 PR 创建）。
2. 卡点不在代码，不在本地门禁，卡在 GitHub API 可达性与认证。

## Next Itinerary

执行顺序（从现在开始）：

1. 先用 SSH 保持代码通道（已完成）：
   - `origin=git@github.com:runlinx-eng/Chunwan_CI.git`
   - `git push` 已成功。
2. 修复 GitHub API 条件（认证与 DNS）：
   - 登录恢复：`gh auth login -h github.com`
   - 校验：`gh auth status`、`gh api user`
3. 创建 PR（两种路径二选一）：
   - CLI：`gh pr create --base main --head codex/p0-p5-hardening ...`
   - Web：`https://github.com/runlinx-eng/Chunwan_CI/compare/main...codex/p0-p5-hardening?expand=1`
4. 发布收口清单执行：
   - 按 `P11_RELEASE_CHECKLIST.md` 完成 PR、CI、merge。
5. 合并后复验：
   - 在 `main` 重新执行 `bash tools/run_release_pipeline.sh`
6. 留痕回写：
   - 把 PR 链接、CI Run ID、merge SHA 写回 `EXECUTION_BOARD.md` Change Log。

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
  - `0.5 * momentum_20_rank + 0.3 * momentum_60_rank + 0.2 * volume_rank`
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
