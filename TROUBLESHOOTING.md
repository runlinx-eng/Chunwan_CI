# Troubleshooting

## 记录模板（必须）
Symptom（现象）：

Root cause（根因）：

Fix（修复步骤，必须可复制命令）：

Prevention（如何避免再次发生）：

## 快速判断树
1) 如果 sweep 失败或出现 `concept_hits=0`：先冻结 candidates，再开诊断开关检查 merge 覆盖率，最后查 snapshot membership 文件是否缺列或空值。
2) 如果出现 `warning=external_theme_map_path`：检查 `THEME_MAP` 实际解析路径是否在 repo 内。
3) 如果 phase10 失败：先确认 clean tree，再重跑。

## 条目：concept_hits 全空如何定位
- 数据源：检查 `data/snapshots/<date>/concept_membership.csv` 是否存在、是否有行、是否包含 `ticker/concept/industry` 列。
- 权限：如果读文件报错，按 stderr 输出的 path/pwd/whoami/ls 线索排查权限或路径。
- 冻结证据：先复制 candidates 到 `/private/tmp/...`，避免被覆盖（见“产物覆盖与冻结证据”）。

## 环境与依赖缺失
- 必须用 venv 的 python：`./.venv/bin/python ...`
- 新建 venv 后先装依赖：`./.venv/bin/pip install -r requirements.txt`
- 确认跑的是哪套 repo/venv：
```bash
git rev-parse --show-toplevel
./.venv/bin/python -c "import sys; print(sys.executable)"
```

## 路径规范（必须）
- repo 内一律用相对路径；meta 里写 repo-relative + sha256。
- 绝对路径仅用于 debug 字段，避免 Desktop/dev 迁移后污染仓库。
- 清理 Desktop 旧路径：
```bash
rg -n "/Users/.*/Desktop/.*Chunwan_ByteDance_PreHoliday_Screener" -S .
```

## I/O 护栏（期望行为）
- membership/snapshot 数据加载异常（文件不存在/权限错误/0 行/缺列）应 hard-fail，并输出：path、exists、rows、columns、sha256、异常栈。
- 手动核验 membership 文件：
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

## 诊断开关
- `CANDIDATES_DEBUG=1`：打印 scored_df 行数、industry/concept 覆盖率、样本行 key 集合。
- `SWEEP_DEBUG=1`：打印 resolved theme map 绝对路径、是否在 repo 内、warning 触发源。

## 产物覆盖与冻结证据（concept_hits=0）
出现 `concept_hits=0` 时先冻结证据，避免 `screener_candidates_latest.jsonl` 被后续命令覆盖：
```bash
./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-16
cp artifacts_metrics/screener_candidates_latest.jsonl /private/tmp/candidates_2026_01_16.jsonl
cp artifacts_metrics/screener_candidates_latest_meta.json /private/tmp/candidates_2026_01_16_meta.json
```
快速核验：
```bash
./.venv/bin/python - <<'PY'
import json
p="/private/tmp/candidates_2026_01_16.jsonl"
enh=nonempty=0
with open(p,"r",encoding="utf-8") as f:
    for line in f:
        if not line.strip(): continue
        r=json.loads(line)
        if r.get("mode")!="enhanced": continue
        enh += 1
        ch=((r.get("reason_struct") or {}).get("concept_hits") or [])
        if len(ch)>0: nonempty += 1
print("enhanced_rows", enh, "enhanced_concept_hits_nonempty", nonempty)
PY
```

## Snapshot membership/merge 诊断
```bash
CANDIDATES_DEBUG=1 ./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-16
CANDIDATES_DEBUG=1 ./.venv/bin/python tools/build_screener_candidates.py --snapshot-id 2026-01-20
```
如果 2026-01-16 的 industry/concept 覆盖率为 0，重点检查 `data/snapshots/2026-01-16/concept_membership.csv` 是否空列或空值。

## external_theme_map_path 排查
```bash
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

## zsh glob 无匹配导致 rm 报错
- `verify_*.txt` 没有匹配时 zsh 会报 `no matches found`
- 写法：`ls artifacts_logs/verify_*.txt 2>/dev/null || true` 或用引号包起来

## clean-tree gate
- `phase10_prune_verify.sh` 要求 clean tree
- dirty tree：先 commit，或 `git stash -u`
