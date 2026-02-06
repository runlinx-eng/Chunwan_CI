# RUNBOOK

## 目标口径（V2）

项目目标由“输出 Top N 名单”升级为“输出可验证 Top N 名单”：

1. 结果完整性：`results_len == topN` 且 `issues == 0`。
2. 可解释性：每条结果必须含 `theme_hits`、`matched_terms`、`score_breakdown`、`data_date`。
3. 信号有效性：`enhanced` 相对 `tech_only` 必须有可观测增益。
4. 数据可靠性：优先 `snapshot`，备选 `akshare`，失败必须显式报因。
5. 可复现性：同输入哈希输出一致，`provenance` 完整。

## 验收入口（唯一主流程）

```bash
STRICT_IO=1 bash tools/phase10_prune_verify.sh
./.venv/bin/python tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
```

通过标准：
- `phase10_prune_verify.sh` 无报错退出。
- `run_snapshot_sweep.py --gate` 无 gate 失败。
- 产物存在：`artifacts_metrics/screener_topn_latest_all.jsonl`。

## Git 防遗漏流程（新手强制建议）

开工前先跑：

```bash
bash tools/git_guard.sh --require-prefix --require-upstream
```

验收前再跑：

```bash
bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean
```

通过标准：
- 非 detached HEAD。
- 分支名以 `codex/` 开头。
- 已设置 upstream（避免本地提交未推远端）。
- 严格验收时必须 clean tree。

## P0 Preflight（先于验收执行）

```bash
bash tools/preflight_gate.sh
bash tools/preflight_gate.sh --require-clean --require-pytest
bash tools/preflight_gate.sh --ensure-theme-map-sparsity --theme-map theme_to_industry_em_2026-01-20.csv
```

说明：
- `--require-clean` 用于 release 验收；日常开发可不加。
- `--ensure-theme-map-sparsity` 会自动补齐 `artifacts_metrics/theme_map_sparsity_latest.json`，避免 `snapshot_sweep` 在回归矩阵阶段因缺文件失败。

### theme_precision 基线刷新（口径变更后必做）

触发条件（任一满足）：
- 修改 `src/scoring.py`
- 修改 `src/theme_pipeline.py`
- 修改 `signals.yaml`
- 更换/重剪 `theme_to_industry*.csv`

执行步骤：

```bash
bash tools/update_theme_precision_baseline.sh
git add artifacts_metrics/theme_precision_baseline.json
git commit -m "chore: refresh theme precision baseline"
```

说明：不刷新基线会导致 strict `phase10` 误报“p50/p95/p99 低于 baseline”。

### clean-tree 验收策略

若当前有未提交改动，先暂存再验收：

```bash
git stash push -u -m wip_before_gate_YYYYMMDD_HHMM
STRICT_IO=1 bash tools/phase10_prune_verify.sh
./.venv/bin/python tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
```

验收后再恢复：

```bash
git stash pop
```

### P1/P2 快速验收（主题区分度）

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --no-cache
cp outputs/report_2026-01-20_top5.json /tmp/report_enh.json
PYTHONDONTWRITEBYTECODE=1 python3 -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --theme-weight 0 --no-cache
```

通过标准：
- `enhanced` 与 `tech_only` TopN 序列不完全一致。
- `tools/run_snapshot_sweep.py --gate` 输出 `snapshots_failed=0`。
- `enhanced_theme_hit_sig_sets` 以运行时 `theme_hits/signal_themes` 口径统计，不再使用静态映射反推。

## 一键发布流水线（P5）

标准模式（要求 clean tree + pytest 可用）：

```bash
bash tools/run_release_pipeline.sh
```

开发诊断模式（跳过 phase10）：

```bash
bash tools/run_release_pipeline.sh --skip-phase10 --snapshots 2026-01-20,2026-01-16 --top-n 10
```

## P8 真实数据探针（AkShare）

执行：

```bash
./.venv/bin/python tools/probe_real_data_chain.py --date 2026-02-05 --top 3
```

产物：
- `artifacts_metrics/real_data_probe_latest.json`
- `artifacts_metrics/real_data_probe_YYYYMMDD_HHMMSS.json`

失败分型：
- `network_blocked`：当前运行环境 DNS/网络策略阻断（非策略逻辑问题）
- `dependency_missing`：依赖缺失
- `ssl_or_cert_error`：证书或 SSL 栈问题
- `provider_rate_limit`：数据源限流
- `unknown_runtime_error`：其余错误

## P9 多日期真实数据回放（AkShare）

执行：

```bash
./.venv/bin/python tools/run_real_data_replay.py --dates 2026-02-05,2026-02-04 --top 3
```

产物：
- `artifacts_metrics/real_data_replay_latest.json`

关键字段：
- `success_rate`
- `global_status`（`ok` / `degraded` / `blocked_by_environment`）
- `failure_histogram`

## P10 策略有效性门禁（snapshot 回测）

执行：

```bash
bash specpack/strategy_effectiveness/verify.sh
# 或 make strategy-effectiveness
```

产物：
- `artifacts_metrics/strategy_effectiveness_latest.json`
- `artifacts_metrics/strategy_effectiveness_gate_latest.json`
- `artifacts_metrics/strategy_effectiveness_YYYYMMDD_HHMMSS.json`

门禁语义：
- hard 阈值：不达标直接失败（阻断发布）
- target 阈值：不达标仅告警（用于后续策略优化）

关键字段：
- `per_horizon.<h>.mean_excess_return`
- `per_horizon.<h>.excess_win_rate`
- `per_horizon.<h>.cumulative_spread`
- `per_horizon.<h>.max_drawdown_enhanced`
- `per_horizon.<h>.objective_alpha`
- `per_horizon.<h>.avg_turnover_enhanced`
- `per_horizon.<h>.drawdown_constraint_passed`
- `overall.mean_objective_alpha`

## P11 GitHub 发布收口

发布收口清单见：
- `P11_RELEASE_CHECKLIST.md`

最短本地入口：

```bash
bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean
bash tools/run_release_pipeline.sh
bash specpack/strategy_effectiveness/verify.sh
```

## 执行指挥文档（V2）

- 总指挥：`EXECUTION_BOARD.md`
- 策略内核待办：`BACKLOG_B.md`
- 项目地图：`PROJECT_MAP.md`
- 数据契约：`DATA_CONTRACTS.md`
- 失败矩阵：`FAILURE_MATRIX.md`

## Quickstart（新目录/新 venv）
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

## 图形界面启动（本地交互）
```bash
./.venv/bin/streamlit run ui/streamlit_app.py
# 或 make ui
```

## 可执行工作流
```bash
STRICT_IO=1 bash tools/phase10_prune_verify.sh
```

## 文档更新触发规则（最简）
- 说“这个坑以后别再踩”：写入 `TROUBLESHOOTING.md`
- 说“以后统一这么干”：写入 `RUNBOOK.md`
- 说“这里为什么改成这样”：写入 `DECISIONS.md`

## 常用命令
```bash
./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-20
./.venv/bin/python tools/run_snapshot_sweep.py --snapshots 2026-01-20,2026-01-16 --top-n 10 --gate
./.venv/bin/python tools/validate_screener_topn.py
./.venv/bin/python tools/inspect_candidates_diversity.py --path artifacts_metrics/screener_candidates_latest.jsonl
```

## 自检命令
```bash
bash tools/compileall_check.sh
bash tools/selfcheck.sh
```

## Usage

### Local quick run
1) Activate venv (example)
- source .venv/bin/activate

2) Run selfcheck
- bash tools/selfcheck.sh

3) Locate the latest run dir
- ls -1dt backups/run_* | head -n 1

The run dir contains INDEX.txt, verify log, and generated metrics.

### CI bundle (release_bundle)
1) Pick an audit tag (use the latest local run dir name)
Example: run_YYYYMMDD_HHMM

2) Trigger and watch CI
- bash tools/cw_release_bundle_watch.sh run_YYYYMMDD_HHMM

The script prints RUN_ID and waits until success or failure.

### Download and verify bundle artifact
Given RUN_ID:
- bash tools/cw_bundle_download_extract_verify.sh RUN_ID

This downloads artifacts into:
downloads_release_bundle/RUN_ID/
and verifies INDEX.txt contains:
- as_of_date
- created_at

## 如何接入新日期 snapshot
1) Create pack（生成快照包）
```bash
bash tools/cw_snapshot_pack_create.sh --snapshot-id YYYY-MM-DD --mode minimal
```

2) 传递 pack 文件（避免直接提交大文件）
- snapshot_packs/snapshot_pack_YYYY-MM-DD.tar.gz
- snapshot_packs/snapshot_pack_YYYY-MM-DD.sha256
- snapshot_packs/snapshot_pack_YYYY-MM-DD.manifest.txt

3) Install pack（安装到本地）
```bash
bash tools/cw_snapshot_pack_install.sh --tar snapshot_packs/snapshot_pack_YYYY-MM-DD.tar.gz
```

4) 验证
```bash
./.venv/bin/python -m src.run --date YYYY-MM-DD --top 1 --provider snapshot --no-fallback --snapshot-as-of YYYY-MM-DD
bash tools/selfcheck.sh
```

### FAQ
Q: 缺 snapshot（FileNotFoundError: Missing concept_membership.csv）怎么办？
A: 用上面的 pack install 安装该日期，或先 create pack 再安装。

## 可交付物指针
- AUDIT_TAG=run_20260124_1924
- RUN_ID=21315297685
- ART_NAME=run_20260124_1924
- run_git_rev=9937c1e5...

## 常见陷阱
- python 解析：优先 `./.venv/bin/python`，脚本支持 `VENV_PYTHON=...`。
- clean tree：`phase10_prune_verify.sh` 要求工作区干净。
- pyc 权限：如果 `compileall` 报 PermissionError，先检查缓存目录权限或禁写 bytecode。

排障与命令：`TROUBLESHOOTING.md`
