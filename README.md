# Pre-Holiday A-Share Screener (MVP)

目标：读取 `signals.yaml` 的主题信号，结合主题-行业/概念/股票映射与基础指标，输出节前窗口 Top N 候选股，并生成解释报告（含命中主题、主题权重、匹配路径、指标数值与数据日期）。

## 功能范围
- A 股 MVP（本地模拟数据，可复现、可缓存）
- 输入：`signals.yaml`、`theme_to_industry.csv`
- 输出：`outputs/` 下的 JSON 与 CSV 报告
- 可降级：若真实数据源不可用，自动使用本地模拟数据
- 最小单测：时间对齐与无未来函数

## 项目结构
- `src/run.py`：CLI 入口
- `src/data_provider.py`：数据源（mock + snapshot + akshare）
- `src/scoring.py`：指标与评分
- `src/report.py`：报告生成
- `src/cache.py`：缓存读写
- `tests/`：最小单测
- `outputs/`：结果输出
- `notebooks/`：Notebook 示例
- `scripts/fetch_snapshot.py`：一次性抓取快照数据

## 安装
```bash
pip install -r requirements.txt
```

AkShare 说明：
- 需要可用网络环境
- 版本随接口可能变化，建议固定版本后使用

## 运行
```bash
python -m src.run --date 2026-01-20 --top 20
```

可选参数：
- `--signals signals.yaml`
- `--theme-map theme_to_industry.csv`
- `--provider mock|akshare|snapshot`
- `--no-fallback`（真实源失败直接报错）
- `--no-cache`
- `--snapshot-as-of YYYY-MM-DD`（snapshot 回放日期）

AkShare 示例：
```bash
python -m src.run --date 2026-01-12 --top 20 --provider akshare
```

## 输出说明
- `outputs/report_YYYY-MM-DD_topN.json`
- `outputs/report_YYYY-MM-DD_topN.csv`

每只票包含：
- 命中主题（`theme_hits`，含 `signal_id/weight/match_paths/signal_theme`）
- `matched_terms`（关键词/概念/行业名）与 `matched_source`（signals/map）
- `score_breakdown`（评分拆解）
- 指标数值（`momentum_20`/`momentum_60`/`volatility_20`/`avg_volume_20`）
- 数据日期（`data_date`）

## 缓存与可复现
- 同输入同输出：缓存 key 由日期、topN、signals、theme_map 与 provider 决定
- 缓存目录：`.cache/`

## 配置说明
`signals.yaml` 可选字段：
- `weight`：不填则按 `priority` 映射（high=1.0, medium=0.6, low=0.3）
- `phase`：默认 `live`
- `core_theme`：可选的核心主题，用于将多个信号聚合展示
- `signal_009` 默认不参与加分（权重 0），但会作为风险提示出现在报告中

`theme_to_industry.csv` 支持新旧格式：
- 旧格式：`主题ID` + `对应行业/概念`
- 新格式：`主题ID` + `map_type` + `map_values`，其中 `map_type` 可为 `industry/concept/ticker`

## Snapshot Provider
使用本地快照数据：
```bash
python -m src.run --date 2026-01-20 --top 20 --provider snapshot --no-fallback --snapshot-as-of 2026-01-20
```

快照目录格式：
```
data/snapshots/<as_of>/
  concept_membership.csv
  prices.csv (或 prices.parquet)
```

一次性抓取脚本（需要 akshare 与可用网络）：
```bash
python scripts/fetch_snapshot.py --as-of 2026-01-20 --source em --concepts 云计算 互动传媒 数字藏品
```

Makefile 快捷命令：
```bash
make snapshot AS_OF=2026-01-20 CONCEPTS="云计算 互动传媒 数字藏品"
make run DATE=2026-01-20 TOP=20 SNAPSHOT_AS_OF=2026-01-20
make verify
```

## 单测
```bash
pytest -q
```

## Specpack
```bash
bash specpack/verify_all.sh
```

## Notebook
```bash
jupyter notebook notebooks/demo.ipynb
```

## 注意
- 当前为练习用 MVP，mock 数据用于保证本地可跑与可复现
