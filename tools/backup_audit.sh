#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

meta_path="artifacts_metrics/screener_topn_latest_meta.json"
if [ ! -f "$meta_path" ]; then
  echo "error: missing $meta_path" >&2
  exit 1
fi

echo "[backup_audit] python=${PYTHON_BIN}"

latest_log_path=$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path
meta_path = Path("artifacts_metrics/screener_topn_latest_meta.json")
meta = json.loads(meta_path.read_text(encoding="utf-8"))
print(meta.get("latest_log_path", ""))
PY
)

git_rev=$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path
meta_path = Path("artifacts_metrics/screener_topn_latest_meta.json")
meta = json.loads(meta_path.read_text(encoding="utf-8"))
print(meta.get("git_rev", ""))
PY
)

snapshot_id=$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path
meta_path = Path("artifacts_metrics/screener_topn_latest_meta.json")
meta = json.loads(meta_path.read_text(encoding="utf-8"))
print(meta.get("snapshot_id", ""))
PY
)

created_at=$("$PYTHON_BIN" - <<'PY'
import json
from datetime import datetime, timezone
from pathlib import Path
meta_path = Path("artifacts_metrics/screener_topn_latest_meta.json")
meta = json.loads(meta_path.read_text(encoding="utf-8"))
created_at = meta.get("created_at")
if not created_at:
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
print(created_at)
PY
)

theme_map_sha256=$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path
meta_path = Path("artifacts_metrics/screener_topn_latest_meta.json")
meta = json.loads(meta_path.read_text(encoding="utf-8"))
print(meta.get("theme_map_sha256", ""))
PY
)

if [ -z "$latest_log_path" ]; then
  echo "error: latest_log_path missing in $meta_path" >&2
  exit 1
fi

if [ ! -f "$latest_log_path" ]; then
  echo "error: verify log not found: $latest_log_path" >&2
  exit 1
fi

as_of_date=$("$PYTHON_BIN" - "$latest_log_path" "$snapshot_id" <<'PY'
import re
import sys

log_path = sys.argv[1]
snapshot_id = sys.argv[2]
as_of = ""
try:
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            m = re.search(r"As-of date:\\s*(\\S+)", line)
            if m:
                as_of = m.group(1)
                break
except FileNotFoundError:
    as_of = ""

if not as_of:
    as_of = snapshot_id
print(as_of)
PY
)

if [ -n "${AUDIT_TAG:-}" ]; then
  backup_dir="backups/${AUDIT_TAG}"
else
  backup_dir="backups/audit_$(date +"%Y%m%d")"
fi
mkdir -p "$backup_dir"

tag_failed=0
tag_error=""
if [ -n "${AUDIT_TAG:-}" ]; then
  tag_name="${AUDIT_TAG}"
  if ! tag_error=$(git tag "$tag_name" 2>&1); then
    tag_failed=1
    tag_error="${tag_error//$'\n'/ }"
    tag_reason="tag_failed"
    if echo "$tag_error" | grep -qi "operation not permitted"; then
      tag_reason="operation_not_permitted"
    fi
    echo "[backup_audit] WARN tag_failed=${tag_name} reason=${tag_reason}"
  fi
fi

cp "$meta_path" "$backup_dir/"
cp "artifacts_metrics/regression_matrix_latest.json" "$backup_dir/"
cp "$latest_log_path" "$backup_dir/"

verify_log_basename="$(basename "$latest_log_path")"
cat <<EOF > "$backup_dir/INDEX.txt"
run_git_rev=${git_rev}
verify_log_basename=${verify_log_basename}
snapshot_id=${snapshot_id}
as_of_date=${as_of_date}
created_at=${created_at}
theme_map_sha256=${theme_map_sha256}
tag_failed=${tag_failed}
tag_error=${tag_error}
EOF

echo "[backup_audit] written=${backup_dir} verify_log=${verify_log_basename}"
