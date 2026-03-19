#!/bin/sh
set -e
OUT="${OUTPUT_PATH:-/app/output.json}"
# 0 = une seule exécution (adapté à un job / cron Railway) ; >0 = boucle en secondes
INTERVAL="${SCRAPE_INTERVAL_SECONDS:-0}"

run_once() {
  python -u unibet_all_json.py -o "$OUT"
  echo "$(date -Iseconds) Écrit $OUT ($(wc -c < "$OUT" 2>/dev/null || echo 0) octets)"
}

if [ "$INTERVAL" -gt 0 ] 2>/dev/null; then
  echo "Boucle toutes les ${INTERVAL}s → $OUT"
  while true; do
    run_once || true
    sleep "$INTERVAL"
  done
else
  run_once
fi
