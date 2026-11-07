#!/usr/bin/env bash
# Start Desk UI + worker (Terminal: run `python run.py` in another terminal).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p logs

echo "Desk UI → http://127.0.0.1:5005"
echo "Worker heartbeats → Terminal /ops and Desk /health"
echo "Stop with: kill \$(cat logs/desk_ui.pid) \$(cat logs/desk_worker.pid)"

python run_analyst_desk.py >> logs/desk_ui.log 2>&1 &
echo $! > logs/desk_ui.pid
python run_analyst_desk_worker.py >> logs/desk_worker.log 2>&1 &
echo $! > logs/desk_worker.pid

echo "PIDs: UI $(cat logs/desk_ui.pid) worker $(cat logs/desk_worker.pid)"
echo "Tail: tail -f logs/desk_ui.log logs/desk_worker.log"
