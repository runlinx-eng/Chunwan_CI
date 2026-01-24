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

## Usage

### Local quick run
1) Activate venv (example)
- source .venv/bin/activate

2) Run selfcheck
- bash tools/selfcheck.sh

3) Locate the latest run dir
- ls -1dt backups/run_* | head -n 1

The run dir contains INDEX.txt, verify log, and generated metrics.

### CI bundle (release_bundle)
1) Pick an audit tag (use the latest local run dir name)
Example: run_YYYYMMDD_HHMM

2) Trigger and watch CI
- bash tools/cw_release_bundle_watch.sh run_YYYYMMDD_HHMM

The script prints RUN_ID and waits until success or failure.

### Download and verify bundle artifact
Given RUN_ID:
- bash tools/cw_bundle_download_extract_verify.sh RUN_ID

This downloads artifacts into:
downloads_release_bundle/RUN_ID/
and verifies INDEX.txt contains:
- as_of_date
- created_at

## 可交付物指针
- AUDIT_TAG=run_20260124_1924
- RUN_ID=21315297685
- ART_NAME=run_20260124_1924
- run_git_rev=9937c1e5...

## 常见陷阱
- python 解析：优先 `./.venv/bin/python`，脚本支持 `VENV_PYTHON=...`。
- clean tree：`phase10_prune_verify.sh` 要求工作区干净。
- pyc 权限：如果 `compileall` 报 PermissionError，先检查缓存目录权限或禁写 bytecode。

排障与命令：`TROUBLESHOOTING.md`
