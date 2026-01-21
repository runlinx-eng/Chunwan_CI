# Decisions

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
