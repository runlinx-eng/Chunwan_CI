#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:-}"
if [[ -z "$RUN_ID" ]]; then
  echo "usage: tools/cw_bundle_download_extract_verify.sh RUN_ID"
  exit 2
fi

OUT_DIR="downloads_release_bundle/${RUN_ID}"
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

gh run download "${RUN_ID}" -D "${OUT_DIR}"

TAR_PATH="$(find "${OUT_DIR}" -maxdepth 3 -type f -name '*.tar.gz' | head -n 1 || true)"
if [[ -z "${TAR_PATH}" ]]; then
  echo "no tar.gz found under ${OUT_DIR}"
  find "${OUT_DIR}" -maxdepth 3 -type f -ls || true
  exit 1
fi

EXTRACT_DIR="${OUT_DIR}/_extracted"
mkdir -p "${EXTRACT_DIR}"
tar -xzf "${TAR_PATH}" -C "${EXTRACT_DIR}"

INDEX_PATH="$(find "${EXTRACT_DIR}" -type f -name INDEX.txt | head -n 1 || true)"
if [[ -z "${INDEX_PATH}" ]]; then
  echo "no INDEX.txt found after extraction"
  find "${EXTRACT_DIR}" -maxdepth 4 -type f -ls || true
  exit 1
fi

echo "INDEX_PATH=${INDEX_PATH}"
echo "== INDEX preview =="
cat "${INDEX_PATH}"

echo "== required keys check =="
grep -q '^as_of_date=' "${INDEX_PATH}" && echo "OK as_of_date" || (echo "MISSING as_of_date" && exit 1)
grep -q '^created_at=' "${INDEX_PATH}" && echo "OK created_at" || (echo "MISSING created_at" && exit 1)
