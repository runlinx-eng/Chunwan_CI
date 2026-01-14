# mvp_smoke Spec

目的：快速验证 MVP 输出结构与核心目标是否满足。

执行命令：
```bash
bash specpack/mvp_smoke/verify.sh
```

覆盖目标：
- 无未来函数（输出数据日期不晚于 as_of）
- TopN 行数正确
- 解释字段完整（命中主题 + 命中路径 + 评分构成）
- 核心主题收敛（3-5 个）
