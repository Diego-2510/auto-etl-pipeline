#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." &&
  pwd
)"

cd "$PROJECT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "error: .venv not found; create it with: python -m venv .venv" >&2
  exit 2
fi

CONFIG_PATH="${1:-config.yaml}"

exec \
  .venv/bin/python \
  -m src.pipeline \
  --config "$CONFIG_PATH"