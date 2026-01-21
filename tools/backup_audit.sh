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

if [ -n "${AUDIT_TAG:-}" ]; then
  backup_dir="backups/${AUDIT_TAG}"
else
  backup_dir="backups/audit_$(date +"%Y%m%d")"
fi
mkdir -p "$backup_dir"

cp "$meta_path" "$backup_dir/"
cp "artifacts_metrics/regression_matrix_latest.json" "$backup_dir/"
cp "$latest_log_path" "$backup_dir/"

verify_log_basename="$(basename "$latest_log_path")"
cat <<EOF > "$backup_dir/INDEX.txt"
run_git_rev=${git_rev}
verify_log_basename=${verify_log_basename}
snapshot_id=${snapshot_id}
theme_map_sha256=${theme_map_sha256}
EOF

echo "[backup_audit] written=${backup_dir} verify_log=${verify_log_basename}"
