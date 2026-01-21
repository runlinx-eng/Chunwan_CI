# Troubleshooting

## 记录模板（必须）
Symptom（现象）：

Root cause（根因）：

Fix（修复步骤，必须可复制命令）：

Prevention（如何避免再次发生）：

## 症状 → 原因 → 命令

### python not found
Root cause：PATH 未包含 python，或脚本未指向 venv。
Fix：
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python -c "import sys; print(sys.executable)"
VENV_PYTHON=./.venv/bin/python bash tools/backup_audit.sh
```

### pandas missing
Root cause：依赖未安装或未使用 venv。
Fix：
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python -c "import pandas; print('pandas ok')"
```

### warning=external_theme_map_path
Root cause：THEME_MAP 解析为 repo 外路径。
Fix：
```bash
unset THEME_MAP
./.venv/bin/python - <<'PY'
import os, subprocess
from pathlib import Path
repo = Path(subprocess.check_output(["git","rev-parse","--show-toplevel"], text=True).strip())
raw = os.environ.get("THEME_MAP", "")
p = Path(raw)
abs_p = (repo / p).resolve() if raw and not p.is_absolute() else p.resolve()
print("repo_root=", repo)
print("THEME_MAP_raw=", raw)
print("THEME_MAP_abs=", abs_p)
print("is_under_repo=", str(abs_p).startswith(str(repo)))
PY
```

### PermissionError: *.pyc
Root cause：Python bytecode cache 目录不可写。
Fix：
```bash
bash tools/compileall_check.sh
```

### macOS 禁止在 .git 创建 .lock（tag/refs 更新失败）
Root cause：文件系统/安全策略禁止写入 `.git`，导致 `git tag` 或 refs 更新失败。
Fix：
复现/验证：
```bash
touch .git/refs/tags/_perm_test && echo "OK touch normal" || echo "FAIL touch normal"
rm -f .git/refs/tags/_perm_test
touch .git/refs/tags/_perm_test.lock && echo "OK touch .lock" || echo "FAIL touch .lock"
rm -f .git/refs/tags/_perm_test.lock
touch .git/packed-refs.lock && echo "OK packed-refs.lock" || echo "FAIL packed-refs.lock"
rm -f .git/packed-refs.lock
```
规避方案：
- 备份以 bundle + INDEX 为锚点（`bash tools/backup_audit.sh` 会写 `INDEX.txt`）。
- tag 失败仅记录 warning，不阻断流程。

### dirty tree
Root cause：一键流程要求 clean tree。
Fix：
```bash
git status --porcelain
git stash -u
```

### zsh: no matches found
Root cause：zsh glob 无匹配时报错。
Fix：
```bash
ls artifacts_logs/verify_*.txt 2>/dev/null || true
```
或临时禁用：
```bash
setopt NO_NOMATCH
```

### 目录迁移导致旧绝对路径
Root cause：产物或基线写入 Desktop/dev 旧路径。
Fix：
```bash
rg -n "/Users/.*/Desktop/.*Chunwan_ByteDance_PreHoliday_Screener" -S .
```
以 repo-relative + sha256 为准，运行产物忽略绝对路径。

### concept_hits 全空
Root cause：membership 文件缺失/无行/缺列，或 merge 后 concept/industry 为空。
Fix：
```bash
snapshot=2026-01-16
path="data/snapshots/${snapshot}/concept_membership.csv"
ls -l "$path"
./.venv/bin/python - <<'PY'
import pandas as pd, pathlib, hashlib
path = pathlib.Path("data/snapshots/2026-01-16/concept_membership.csv")
print("exists", path.exists())
if path.exists():
    data = path.read_bytes()
    print("sha256", hashlib.sha256(data).hexdigest())
    df = pd.read_csv(path, dtype=str)
    print("rows", len(df))
    print("columns", list(df.columns))
PY
```
冻结证据：
```bash
./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-16
cp artifacts_metrics/screener_candidates_latest.jsonl /private/tmp/candidates_2026_01_16.jsonl
cp artifacts_metrics/screener_candidates_latest_meta.json /private/tmp/candidates_2026_01_16_meta.json
```

### clean-tree gate 失败
Root cause：phase10 需要 clean tree。
Fix：
```bash
git status --porcelain
```
