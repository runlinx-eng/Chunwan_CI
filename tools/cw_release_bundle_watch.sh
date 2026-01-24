#!/usr/bin/env bash
set -euo pipefail

AUDIT_TAG="${1:-}"
if [[ -z "$AUDIT_TAG" ]]; then
  echo "usage: tools/cw_release_bundle_watch.sh run_YYYYMMDD_HHMM"
  exit 2
fi

gh workflow run release_bundle.yml --ref main -f audit_tag="$AUDIT_TAG" >/dev/null

PREV_ID="${2:-}"

RUN_ID=""
for _ in {1..120}; do
  RUN_ID="$(gh run list --workflow release_bundle.yml --branch main --limit 10 --json databaseId,event,status,createdAt \
    -q 'map(select(.event=="workflow_dispatch")) | .[0].databaseId' || true)"
  if [[ -n "${RUN_ID}" && "${RUN_ID}" != "${PREV_ID}" ]]; then
    break
  fi
  sleep 2
done

if [[ -z "${RUN_ID}" ]]; then
  echo "failed to detect new run id"
  exit 1
fi

echo "RUN_ID=${RUN_ID}"
gh run watch "${RUN_ID}" --exit-status
