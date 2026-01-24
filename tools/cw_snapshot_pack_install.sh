#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: tools/cw_snapshot_pack_install.sh --tar <path> [--dest-root data/snapshots] [--overwrite]"
}

TAR_PATH=""
DEST_ROOT="data/snapshots"
OVERWRITE="0"

while [ $# -gt 0 ]; do
  case "$1" in
    --tar)
      TAR_PATH="${2:-}"
      shift 2
      ;;
    --dest-root)
      DEST_ROOT="${2:-}"
      shift 2
      ;;
    --overwrite)
      OVERWRITE="1"
      shift 1
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

if [ -z "$TAR_PATH" ]; then
  echo "error: --tar is required"
  usage
  exit 2
fi

if [ ! -f "$TAR_PATH" ]; then
  echo "error: tar not found: $TAR_PATH"
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

snapshot_id=""
base_name="$(basename "$TAR_PATH")"
if [[ "$base_name" =~ snapshot_pack_([0-9]{4}-[0-9]{2}-[0-9]{2})\.tar\.gz ]]; then
  snapshot_id="${BASH_REMATCH[1]}"
else
  first_entry="$(tar -tzf "$TAR_PATH" | head -n 1 || true)"
  if [ -n "$first_entry" ]; then
    snapshot_id="${first_entry%%/*}"
  fi
fi

if [[ ! "$snapshot_id" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "error: failed to infer snapshot_id from tar: $TAR_PATH"
  exit 1
fi

sha_path="${TAR_PATH%.tar.gz}.sha256"
if [ ! -f "$sha_path" ]; then
  echo "error: sha256 file not found: $sha_path"
  exit 1
fi

expected_sha="$(awk '{print $1}' "$sha_path" | head -n 1)"
actual_sha="$(sha256_file "$TAR_PATH")"
if [ "$expected_sha" != "$actual_sha" ]; then
  echo "error: sha256 mismatch for $TAR_PATH"
  echo "expected: $expected_sha"
  echo "actual:   $actual_sha"
  exit 1
fi

dest_dir="${DEST_ROOT}/${snapshot_id}"
if [ -d "$dest_dir" ] && [ "$OVERWRITE" != "1" ]; then
  echo "error: destination exists (use --overwrite): $dest_dir"
  exit 1
fi

if [ -d "$dest_dir" ] && [ "$OVERWRITE" = "1" ]; then
  rm -rf "$dest_dir"
fi

mkdir -p "$DEST_ROOT"
tar -xzf "$TAR_PATH" -C "$DEST_ROOT"

if [ ! -f "${dest_dir}/concept_membership.csv" ] || [ ! -f "${dest_dir}/prices.csv" ]; then
  echo "error: required files missing after install under $dest_dir"
  exit 1
fi

check_header "${dest_dir}/concept_membership.csv" "ticker,name,concept,industry,description"
check_header "${dest_dir}/prices.csv" "date,ticker,close,volume"

{
  echo "installed_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "snapshot_id=${snapshot_id}"
  echo "tar_path=${TAR_PATH}"
  echo "tar_sha256=${actual_sha}"
  echo "overwrite=${OVERWRITE}"
} > "${dest_dir}/INSTALL_LOG.txt"

echo "installed ${snapshot_id} to ${dest_dir}"
