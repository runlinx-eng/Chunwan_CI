#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/resolve_python.sh"

SNAPSHOT_ID="${1:-2026-01-20}"
SRC_ROOT="data/snapshots"
OUT_DIR="snapshot_packs"

snapshot_dir="${SRC_ROOT}/${SNAPSHOT_ID}"
backup_dir="${snapshot_dir}.bak_$(date +%Y%m%d_%H%M%S)"
tar_path="${OUT_DIR}/snapshot_pack_${SNAPSHOT_ID}.tar.gz"

restore_snapshot() {
  if [ -d "$backup_dir" ]; then
    rm -rf "$snapshot_dir"
    mv "$backup_dir" "$snapshot_dir"
    echo "[smoke] restored ${snapshot_dir}"
  fi
}
trap restore_snapshot EXIT

echo "[smoke] create pack snapshot_id=${SNAPSHOT_ID}"
bash tools/cw_snapshot_pack_create.sh --snapshot-id "$SNAPSHOT_ID" --src-root "$SRC_ROOT" --out-dir "$OUT_DIR" --mode minimal >/dev/null

if [ ! -f "$tar_path" ]; then
  echo "[smoke] missing tar: $tar_path"
  exit 1
fi

echo "[smoke] backup ${snapshot_dir}"
mv "$snapshot_dir" "$backup_dir"

echo "[smoke] install pack"
bash tools/cw_snapshot_pack_install.sh --tar "$tar_path" --dest-root "$SRC_ROOT" >/dev/null

echo "[smoke] verify read"
"$PYTHON_BIN" -m src.run --date "$SNAPSHOT_ID" --top 1 --provider snapshot --no-fallback --snapshot-as-of "$SNAPSHOT_ID" | tail -n 20

echo "[smoke] ok"
