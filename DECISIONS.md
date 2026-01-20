# Decisions

## Snapshot sweep 的两种池策略
- fixed pool：用于共享 identifier space 的可比对集合；保留 theme 相关 gate
- snapshot_universe：每个 snapshot 用自身 universe；gate 主要看 concept 多样性（因为跨 snapshot 不保证重叠）

## Gate 指标语义（当前实现）
- universe gate：enhanced_concept_hit_signature_unique_set_count >= 阈值（config.json）
- theme_total/theme_hit_signature 指标用于诊断或 fixed pool gate（按 RUNBOOK 定义）
