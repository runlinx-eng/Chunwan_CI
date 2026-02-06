# DATA CONTRACTS (F0-3)

## Goal

定义当前项目输入/输出契约，作为后续重构的兼容边界。

## 1) CLI Contract (`src/run.py`)

主命令：

```bash
python3 -m src.run --date YYYY-MM-DD --top 20
```

参数契约：

| Arg | Required | Default | Notes |
|---|---|---|---|
| `--date` | Yes | - | 交易日对齐输入日期 |
| `--top` | No | `20` | 输出TopN |
| `--signals` | No | `signals.yaml` | 信号配置 |
| `--theme-map` | No | `theme_to_industry_em_2026-01-20.csv` | 主题映射 |
| `--provider` | No | `mock` | `mock|akshare|snapshot` |
| `--no-fallback` | No | `false` | provider异常时是否禁止降级 |
| `--no-cache` | No | `false` | 禁用缓存读取 |
| `--snapshot-as-of` | No | - | snapshot回放日期 |
| `--theme-weight` | No | `1.0` | `0` 时进入 `tech_only` 消融 |

## 2) Input Data Contracts

### 2.1 `signals.yaml`

根节点：

- `signals` (list)

每个 signal 当前字段契约：

| Field | Required | Notes |
|---|---|---|
| `id` | Yes | signal唯一标识 |
| `theme` | Yes | 信号主题名 |
| `core_theme` | No | 核心聚合主题；缺省回退 `theme` |
| `keywords` | No | 关键词数组 |
| `priority` | No | `high|medium|low`，映射默认权重 |
| `weight` | No | 缺省按 `priority`；`signal_009` 缺省强制 `0` |
| `description` | No | 解释信息 |
| `phase` | No | 缺省 `live` |

`v2` 扩展字段（向后兼容）：

| Field | Required | Notes |
|---|---|---|
| `family` | No | 缺省 `legacy_keyword`；有白名单校验 |
| `formula` | No | 缺省 `keyword_hit` |
| `horizon_days` | No | 缺省 `20`，必须 > 0 |
| `decay` | No | 缺省 `1.0`，必须 > 0 |
| `guardrails` | No | 缺省 `{}`，必须是对象 |

### 2.2 `theme_to_industry*.csv`

`src/signals.py` 当前支持两种格式：

1. 旧格式：
   - 必要列：`主题ID`、`对应行业/概念`
2. 新格式：
   - 必要列：`主题ID`、`map_type`、`map_values`

兼容逻辑说明：

- `src/theme_pipeline.py` 在某些补齐路径中会读取 `主题名称`、`关键词`、`对应行业/概念` 做主题键匹配与统计。

### 2.3 Snapshot Files (`data/snapshots/<as_of>/`)

`concept_membership.csv` 当前样例列：

- `ticker,name,concept,industry,description`

硬要求（运行时）：

- 必须有 `ticker`

`prices.csv|parquet` 当前样例列：

- `date,ticker,close,volume`

硬要求（运行时）：

- 必须有 `ticker`、`date`、`close`、`volume`

`manifest.json` 当前样例关键字段：

- `as_of`
- `stats.unique_tickers`
- `stats.unique_concepts`
- `stats.min_concept_members`
- `stats.min_price_bars`
- `files.<name>.sha256`

## 3) Output Data Contracts

### 3.1 Report JSON (`outputs/report_<as_of>_topN.json`)

根字段（当前实跑结果）：

- `as_of,count,data_date,issues,meta,debug,provenance,results,top_n`

`meta` 常见字段（新增）：

- `provider_fallback` (bool)
- `provider_fallback_reason` (optional string)

`results[]` 字段：

- `ticker,name,industry,final_score,theme_hits,score_breakdown,data_date,indicators,reason,reason_struct`

`score_breakdown` 字段：

- `score_total,score_tech_total,score_theme_total,score_risk_total,tech_components,theme_components,theme_strength_components,theme_score,technical_score,risk_penalty,score_weights,momentum_20_rank,momentum_60_rank,volume_rank,final_score`

### 3.2 Candidates JSONL (`artifacts_metrics/screener_candidates_latest.jsonl`)

单行字段（当前样本）：

- `concept_hits,data_date,final_score,item_id,mode,score_breakdown,snapshot_id,theme_hits,ticker`

### 3.3 Screener TopN JSONL (`artifacts_metrics/screener_topn_latest_*.jsonl`)

Schema要求由 `tools/validate_screener_topn.py` 定义，包含：

- `rank,mode,score_total,score_total_source,score_breakdown,theme_hits,concept_hits,snapshot_id,theme_map_path,theme_map_sha256,git_rev,latest_log_path,schema_version`
- 且必须有 `item_id` 或 `ticker`

当前状态：

- 本轮未执行导出流程，`screener_topn_latest_*.jsonl` 尚未生成。

## 4) Compatibility Rules (for upcoming refactor)

1. 保持 `signals.yaml` 旧字段可读（向后兼容）。
2. 报告 `results[]` 的核心解释字段不得删减。
3. 新评分模型允许扩展字段，但不能破坏 `validate_screener_topn.py` 既有校验契约。
4. 对 snapshot 输入文件仅新增可选列，不新增强制列。
