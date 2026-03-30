#!/usr/bin/env bash
# Sortie non bufferisée → les logs Railway apparaissent en direct.
set -euo pipefail
export PYTHONUNBUFFERED=1
exec python -u unibet.py serve
