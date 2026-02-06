#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/tools/resolve_python.sh"

REQUIRE_CLEAN=0
ENSURE_THEME_MAP_SPARSITY=0
REQUIRE_PYTEST=0
THEME_MAP_OVERRIDE=""

usage() {
  cat <<'USAGE'
usage: bash tools/preflight_gate.sh [options]

options:
  --require-clean                 fail when git tree is dirty
  --ensure-theme-map-sparsity     ensure artifacts_metrics/theme_map_sparsity_latest.json exists and matches active theme map
  --require-pytest                require pytest import available in PYTHON_BIN
  --theme-map <path>              override THEME_MAP for sparsity generation/check
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --require-clean)
      REQUIRE_CLEAN=1
      shift
      ;;
    --ensure-theme-map-sparsity)
      ENSURE_THEME_MAP_SPARSITY=1
      shift
      ;;
    --require-pytest)
      REQUIRE_PYTEST=1
      shift
      ;;
    --theme-map)
      if [ -z "${2:-}" ]; then
        echo "error: --theme-map requires a path" >&2
        exit 2
      fi
      THEME_MAP_OVERRIDE="$2"
      shift 2
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

if [ -n "$THEME_MAP_OVERRIDE" ]; then
  export THEME_MAP="$THEME_MAP_OVERRIDE"
fi

if [ "$REQUIRE_CLEAN" -eq 1 ]; then
  if [ -n "$(git status --porcelain)" ]; then
    echo "error: working tree is dirty; commit or stash before running" >&2
    git status --porcelain
    exit 1
  fi
fi

required_modules=("pandas" "numpy" "yaml")
if [ "$REQUIRE_PYTEST" -eq 1 ]; then
  required_modules+=("pytest")
fi

"$PYTHON_BIN" - "$REQUIRE_PYTEST" "${required_modules[@]}" <<'PY'
import importlib
import sys

require_pytest = bool(int(sys.argv[1]))
modules = sys.argv[2:]
missing = []
for mod in modules:
    try:
        importlib.import_module(mod)
    except Exception:
        missing.append(mod)

if missing:
    print("error: missing python modules: " + ", ".join(missing), file=sys.stderr)
    if require_pytest:
        print(
            "hint: create venv and install requirements, e.g. "
            "python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt",
            file=sys.stderr,
        )
    sys.exit(1)
PY

mkdir -p artifacts_metrics

if [ "$ENSURE_THEME_MAP_SPARSITY" -eq 1 ]; then
  METRICS_PATH="$ROOT_DIR/artifacts_metrics/theme_map_sparsity_latest.json"
  ACTIVE_MAP="$($PYTHON_BIN - <<'PY'
from pathlib import Path
import os

root = Path.cwd()
raw = os.environ.get("THEME_MAP", "theme_to_industry_em_2026-01-20.csv")
p = Path(raw)
if not p.is_absolute():
    p = root / p
print(p.resolve())
PY
)"
  if [ ! -f "$ACTIVE_MAP" ]; then
    echo "error: theme map not found: $ACTIVE_MAP" >&2
    exit 1
  fi

  CURRENT_SHA="$($PYTHON_BIN - "$ACTIVE_MAP" <<'PY'
import hashlib
from pathlib import Path
import sys

path = Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
)"

  METRICS_SHA=""
  if [ -f "$METRICS_PATH" ]; then
    METRICS_SHA="$($PYTHON_BIN - "$METRICS_PATH" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit(0)
print(str(payload.get("theme_map_sha256", "")))
PY
)"
  fi

  if [ "$METRICS_SHA" != "$CURRENT_SHA" ]; then
    set +e
    THEME_MAP_SPARSITY_ALLOW_NONZERO=1 "$PYTHON_BIN" specpack/theme_map_sparsity/audit_theme_map_sparsity.py
    rc=$?
    set -e
    if [ "$rc" -ne 0 ]; then
      echo "warning: theme_map_sparsity audit exited non-zero; checking metrics artifact" >&2
    fi
  fi

  if [ ! -f "$METRICS_PATH" ]; then
    echo "error: missing metrics artifact: $METRICS_PATH" >&2
    exit 1
  fi

  FINAL_SHA="$($PYTHON_BIN - "$METRICS_PATH" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
print(str(payload.get("theme_map_sha256", "")))
PY
)"

  if [ "$FINAL_SHA" != "$CURRENT_SHA" ]; then
    echo "error: theme_map_sparsity sha mismatch current=$CURRENT_SHA metrics=$FINAL_SHA" >&2
    exit 1
  fi

  echo "[preflight] theme_map_sparsity_ready path=artifacts_metrics/theme_map_sparsity_latest.json"
fi

echo "[preflight] ok python=${PYTHON_BIN} require_clean=${REQUIRE_CLEAN} require_pytest=${REQUIRE_PYTEST} ensure_theme_map_sparsity=${ENSURE_THEME_MAP_SPARSITY}"
