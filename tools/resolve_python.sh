#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-}"
if [ -z "$ROOT_DIR" ]; then
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  if [ -n "${VENV_PYTHON:-}" ]; then
    PYTHON_BIN="${VENV_PYTHON}"
  elif [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
    PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "error: python not found (create venv and install requirements)" >&2
    exit 127
  fi
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "error: python not executable: ${PYTHON_BIN}" >&2
  exit 127
fi
