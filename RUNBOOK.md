# RUNBOOK

## Quickstart（新目录/新 venv）
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
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

## 常见陷阱
- python 解析：优先 `./.venv/bin/python`，脚本支持 `VENV_PYTHON=...`。
- clean tree：`phase10_prune_verify.sh` 要求工作区干净。
- pyc 权限：如果 `compileall` 报 PermissionError，先检查缓存目录权限或禁写 bytecode。

排障与命令：`TROUBLESHOOTING.md`
