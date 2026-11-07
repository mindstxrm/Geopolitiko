#!/usr/bin/env bash
# Always-on Analyst Desk worker with logs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
echo "Starting Analyst Desk worker… (Ctrl+C to stop)"
exec python run_analyst_desk_worker.py
