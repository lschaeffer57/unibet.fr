#!/usr/bin/env bash
# Sortie non bufferisée → les logs Railway apparaissent en direct.
set -euo pipefail
export PYTHONUNBUFFERED=1
exec python -u unibet_prematch_odds.py \
  -o "${OUTPUT_PATH:-/tmp/output_prematch.json}" \
  --loop-seconds "${SCRAPER_LOOP_SECONDS:-0}" \
  "$@"
