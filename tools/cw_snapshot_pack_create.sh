#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: tools/cw_snapshot_pack_create.sh --snapshot-id YYYY-MM-DD [--src-root data/snapshots] [--out-dir snapshot_packs] [--mode minimal|full]"
}

SNAPSHOT_ID=""
SRC_ROOT="data/snapshots"
OUT_DIR="snapshot_packs"
MODE="minimal"

while [ $# -gt 0 ]; do
  case "$1" in
    --snapshot-id)
      SNAPSHOT_ID="${2:-}"
      shift 2
      ;;
    --src-root)
      SRC_ROOT="${2:-}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown arg $1"
      usage
      exit 2
      ;;
  esac
done

if [ -z "$SNAPSHOT_ID" ]; then
  echo "error: --snapshot-id is required"
  usage
  exit 2
fi

if [[ ! "$SNAPSHOT_ID" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "error: invalid snapshot id: $SNAPSHOT_ID"
  exit 2
fi

if [ "$MODE" != "minimal" ] && [ "$MODE" != "full" ]; then
  echo "error: invalid mode: $MODE (expected minimal|full)"
  exit 2
fi

snapshot_dir="${SRC_ROOT}/${SNAPSHOT_ID}"
if [ ! -d "$snapshot_dir" ]; then
  echo "error: snapshot dir not found: $snapshot_dir"
  exit 1
fi

sha256_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  else
    echo "error: sha256 tool not found (shasum/sha256sum)" >&2
    exit 127
  fi
}

check_header() {
  local path="$1"
  local expected="$2"
  local header=""
  header="$(head -n 1 "$path" | tr -d '\r')"
  if [ "$header" != "$expected" ]; then
    echo "error: header mismatch for $path"
    echo "expected: $expected"
    echo "actual:   $header"
    exit 1
  fi
}

required_files=("concept_membership.csv" "prices.csv")
for f in "${required_files[@]}"; do
  if [ ! -f "${snapshot_dir}/${f}" ]; then
    echo "error: missing required file: ${snapshot_dir}/${f}"
    exit 1
  fi
done

check_header "${snapshot_dir}/concept_membership.csv" "ticker,name,concept,industry,description"
check_header "${snapshot_dir}/prices.csv" "date,ticker,close,volume"

files_to_pack=()
if [ "$MODE" = "minimal" ]; then
  files_to_pack+=("${SNAPSHOT_ID}/concept_membership.csv" "${SNAPSHOT_ID}/prices.csv")
  if [ -f "${snapshot_dir}/manifest.json" ]; then
    files_to_pack+=("${SNAPSHOT_ID}/manifest.json")
  fi
else
  while IFS= read -r -d '' f; do
    files_to_pack+=("${SNAPSHOT_ID}/$(basename "$f")")
  done < <(find "$snapshot_dir" -maxdepth 1 -type f -print0)
fi

if [ "${#files_to_pack[@]}" -eq 0 ]; then
  echo "error: no files to pack for snapshot ${SNAPSHOT_ID}"
  exit 1
fi

mkdir -p "$OUT_DIR"
tar_path="${OUT_DIR}/snapshot_pack_${SNAPSHOT_ID}.tar.gz"
sha_path="${OUT_DIR}/snapshot_pack_${SNAPSHOT_ID}.sha256"
manifest_path="${OUT_DIR}/snapshot_pack_${SNAPSHOT_ID}.manifest.txt"

tar -czf "$tar_path" -C "$SRC_ROOT" "${files_to_pack[@]}"

tar_hash="$(sha256_file "$tar_path")"
printf "%s  %s\n" "$tar_hash" "$(basename "$tar_path")" > "$sha_path"

{
  printf "path\tsha256\trows\theader\n"
  for rel in "${files_to_pack[@]}"; do
    path="${SRC_ROOT}/${rel}"
    file_hash="$(sha256_file "$path")"
    rows="$(wc -l < "$path" | tr -d ' ')"
    header="$(head -n 1 "$path" | tr -d '\r')"
    printf "%s\t%s\t%s\t%s\n" "$rel" "$file_hash" "$rows" "$header"
  done
} > "$manifest_path"

echo "wrote ${tar_path}"
echo "wrote ${sha_path}"
echo "wrote ${manifest_path}"
