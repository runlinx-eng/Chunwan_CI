# 交接文档：Phase6 -> Phase7

## 当前阶段与完成状态
- 阶段6_3已完成，真实快照2026-01-16 results_len=5，verify_all全绿。

## 必交文件清单
### 绝对必须
- `specpack/verify_all.sh`
- `specpack/` 下所有门禁包
- `signals.yaml`
- `theme_to_industry.csv`
- `theme_to_industry_em_2026-01-16.csv`
- `data/snapshots/2026-01-16/manifest.json`
- `data/snapshots/2026-01-16/concept_membership.csv`
- `tools/build_real_snapshot_em.py`
- `tools/ingest_concepts.py`
- `tools/verify_and_log.sh`
- `tools/capture_verify_log.sh`

### 强烈建议
- `HANDOFF_phase6_to_7.md`
- `HANDOFF_template.md`
- `tools/update_real_snapshot_em.sh`
- `notes/em_concepts_2026-01-16.txt`
- `data/snapshots/2026-01-16/prices.csv`（若仓库策略允许存大文件）

## 最短复现步骤
```bash
./specpack/verify_all.sh
python3 tools/build_real_snapshot_em.py --as-of 2026-01-16 --out-dir data/snapshots/2026-01-16 --n-concepts 10 --min-members 50 --min-bars 160 --max-tickers 600 --adjust hfq
python3 -m src.run --date 2026-01-16 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-16 --theme-map theme_to_industry_em_2026-01-16.csv
```

## 已知坑与规避
- theme_map 不匹配：真实快照用 `theme_to_industry_em_2026-01-16.csv`，不要误用 `theme_to_industry.csv`。
- membership ticker 不唯一：`concept_membership.csv` 允许多概念，代码里会按 ticker 去重；导入脚本需按 (ticker, concept) 去重。
- ticker 前导 0/dtype：prices 与 membership 读取必须 `dtype={"ticker": str}`，且 `str.strip()`。
- issues 契约：`report["issues"]` 必须为 int；详细列表在 `meta.issue_list`。
- `.cache` 污染：验收前清理 `.cache`，门禁脚本已覆盖。

## 下一阶段（阶段7）计划与验收
- 新增 `real_snapshot_smoke` 门禁：
  - 运行 `python3 -m src.run --date 2026-01-16 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-16 --theme-map theme_to_industry_em_2026-01-16.csv`
  - 断言：results_len=5、issues=0、reason 含“命中主题/命中路径/评分构成”、themes_used 去重后长度 3-5。

## 交接给新窗口的“一条消息模板”
```
请接手阶段7：基线已在阶段6_3完成，真实快照2026-01-16 results_len=5，verify_all全绿。请先运行 ./specpack/verify_all.sh 和 ./tools/capture_verify_log.sh；真实快照命令为 python3 -m src.run --date 2026-01-16 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-16 --theme-map theme_to_industry_em_2026-01-16.csv。后续目标：新增 real_snapshot_smoke 门禁并保持全绿。
```
