# snapshot_replay Spec

目的：验证 snapshot 回放可复现、无未来函数、核心主题收敛。

执行命令：
```bash
bash specpack/snapshot_replay/verify.sh
```

覆盖目标：
- 无未来函数（输出数据日期不晚于 as_of）
- TopN 行数正确
- 解释字段完整（命中主题 + 命中路径 + 评分构成）
- 输出字段齐全（theme_hits/score_breakdown/data_date）
- 禁止 NaN 指标
- 同一核心主题在单只票中不重复
- 核心主题收敛（3-5 个）
- snapshot 可复现（连续两次结果一致）
