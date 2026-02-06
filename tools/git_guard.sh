#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

STRICT=0
REQUIRE_CLEAN=0
REQUIRE_UPSTREAM=0
REQUIRE_PREFIX=0
EXPECT_PREFIX="${EXPECT_PREFIX:-codex/}"

usage() {
  cat <<'USAGE'
usage: bash tools/git_guard.sh [options]

options:
  --strict              treat warnings as failures
  --require-clean       fail when working tree is dirty
  --require-upstream    fail when branch has no upstream
  --require-prefix      fail when branch does not start with EXPECT_PREFIX (default codex/)
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --strict)
      STRICT=1
      shift
      ;;
    --require-clean)
      REQUIRE_CLEAN=1
      shift
      ;;
    --require-upstream)
      REQUIRE_UPSTREAM=1
      shift
      ;;
    --require-prefix)
      REQUIRE_PREFIX=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

warnings=()
errors=()

branch="$(git branch --show-current || true)"
if [ -z "$branch" ]; then
  errors+=("detached_head")
else
  if [[ "$branch" != "${EXPECT_PREFIX}"* ]]; then
    if [ "$REQUIRE_PREFIX" -eq 1 ]; then
      errors+=("branch_prefix_invalid")
    else
      warnings+=("branch_prefix_invalid")
    fi
  fi
fi

if git rev-parse --abbrev-ref --symbolic-full-name "@{upstream}" >/dev/null 2>&1; then
  upstream="$(git rev-parse --abbrev-ref --symbolic-full-name "@{upstream}")"
else
  upstream=""
  if [ -n "$branch" ]; then
    if [ "$REQUIRE_UPSTREAM" -eq 1 ]; then
      errors+=("no_upstream")
    else
      warnings+=("no_upstream")
    fi
  fi
fi

dirty=0
if [ -n "$(git status --porcelain)" ]; then
  dirty=1
  if [ "$REQUIRE_CLEAN" -eq 1 ]; then
    errors+=("dirty_tree")
  else
    warnings+=("dirty_tree")
  fi
fi

if [ "$STRICT" -eq 1 ] && [ "${#warnings[@]}" -gt 0 ]; then
  for item in "${warnings[@]}"; do
    errors+=("$item")
  done
  warnings=()
fi

echo "[git_guard] branch=${branch:-DETACHED}"
echo "[git_guard] upstream=${upstream:-NONE}"
echo "[git_guard] working_tree=$([ "$dirty" -eq 1 ] && echo dirty || echo clean)"

if [ "${#warnings[@]}" -gt 0 ]; then
  echo "[git_guard] warnings=$(IFS=,; echo "${warnings[*]}")"
fi

if [ "${#errors[@]}" -gt 0 ]; then
  echo "[git_guard] errors=$(IFS=,; echo "${errors[*]}")" >&2
  echo "[git_guard] hint: run 'git switch -c codex/<topic>' then 'git push -u origin <branch>'" >&2
  exit 1
fi

echo "[git_guard] ok"
