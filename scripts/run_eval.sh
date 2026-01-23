#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/backend"

python -m app.eval.runner --dataset "$ROOT_DIR/data/eval/cases.jsonl" --schema "$ROOT_DIR/data/eval/schema.json" --artifacts "$ROOT_DIR/artifacts/eval"
