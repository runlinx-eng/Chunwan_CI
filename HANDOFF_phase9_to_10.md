阶段9→10 交接说明

1) 当前目标与已完成阶段列表
- 目标：在真实快照下可复现运行，主题匹配可观测，门禁覆盖主题效果与回放一致性
- 已完成阶段：阶段6-9（真实快照+主题门禁链路）
- 新增/固化 gates：
  - concept_data_health
  - real_snapshot_smoke
  - theme_explain
  - theme_ablation
  - real_theme_effectiveness

2) 关键命令（复现/验收）
- 统一门禁：./specpack/verify_all.sh
- 生成日志：./tools/capture_verify_log.sh
- 真实快照增强版：
  python -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --theme-map theme_to_industry.csv
- 主题消融（tech_only）：
  python -m src.run --date 2026-01-20 --top 5 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20 --theme-map theme_to_industry.csv --theme-weight 0

3) 关键 debug 指标与通过标准
- report.debug.theme_key_hit_count > 0
- report.debug.n_theme_hit_tickers > 0
- theme_ablation：
  - enhanced 至少一条 score_theme_total > 0
  - tech_only 所有 score_theme_total == 0（容忍 1e-9）

4) 本窗口关键坑与根因
- issues 契约：issues 只能是致命问题计数（results_len<top、fallback、provider_fallback），非致命统计不得计入
- signal theme key 不一致：signals.core_theme 与 theme_map(中文主题名称)键不一致导致映射为空
- 中文 schema：theme_to_industry.csv 的列为“主题名称/关键词/对应行业/概念”，关键词为空需回退到对应行业/概念
- ticker 前导0：真实快照读入需 normalize_ticker，避免 join 失败
- cache 污染：不同参数复用 .cache 导致 tech_only 读到 enhanced 结果

5) 必交接文件清单
- 代码：
  - src/run.py
  - src/theme_pipeline.py
  - src/report.py
  - src/data_provider.py
- 门禁：
  - specpack/verify_all.sh
  - specpack/concept_data_health/*
  - specpack/real_snapshot_smoke/*
  - specpack/theme_explain/*
  - specpack/theme_ablation/*
  - specpack/real_theme_effectiveness/*
- 数据：
  - data/snapshots/2026-01-16/
  - data/snapshots/2026-01-20/
- 工具：
  - tools/generate_theme_map_em.py
  - tools/generate_theme_map_em_cn.py
  - tools/convert_theme_map_cn.py
  - tools/sync_theme_map_with_snapshot.py
  - tools/build_real_snapshot_em.py
  - tools/ingest_concepts.py
  - tools/generate_snapshot.py
  - tools/capture_verify_log.sh
- 映射文件：
  - theme_to_industry.csv
  - theme_to_industry_em_2026-01-16.csv
- 日志：
  - artifacts_logs/verify_*.txt（latest_log 占位）

6) 下一阶段（阶段10）计划
- 主题质量收紧：
  - 减少全覆盖映射（从笛卡尔积缩减到有证据的概念）
  - 映射稀疏化（按主题强相关概念过滤）
  - theme_total 归一化（避免主题分数随概念数量膨胀）
  - 增加 theme_precision gate（主题命中质量门禁）

7) latest_log 占位
- artifacts_logs/verify_*.txt
