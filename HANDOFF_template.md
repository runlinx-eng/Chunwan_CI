# HANDOFF Template

## 阶段名/分支名/基线commit/快照as_of
- 阶段名：
- 分支名：
- 基线 commit（git rev-parse HEAD）：
- 快照 as_of：

## 完成项 checklist
- [ ] 功能已完成
- [ ] 门禁全绿（./specpack/verify_all.sh）
- [ ] 真实快照回放可复现
- [ ] 日志已记录（./tools/capture_verify_log.sh）

## 复现命令区
（终端粘贴）
```bash
./specpack/verify_all.sh
```

（VSCode 终端或 Codex 运行）
```bash
python3 -m src.run --date <YYYY-MM-DD> --top 5 --provider snapshot --no-fallback --snapshot-as-of <YYYY-MM-DD> --theme-map <theme_map.csv>
```

## 已知坑
- 

## 下一阶段计划（目标/门禁/验收）
- 目标：
- 门禁：
- 验收：

## 交接一条消息模板
```
阶段已完成，请按 ./specpack/verify_all.sh 复验并查看 ./tools/capture_verify_log.sh 日志。下一阶段目标：<写这里>。
```
