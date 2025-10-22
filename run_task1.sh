#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "Starting Task 1 (3 nodes)..."

# node1
python3 "$ROOT/myleprocess.py" --task 1 \
  --config "$ROOT/config_task1_node1.txt" \
  --log "$ROOT/task1/log1.txt" & PID1=$!

# node2
python3 "$ROOT/myleprocess.py" --task 1 \
  --config "$ROOT/config_task1_node2.txt" \
  --log "$ROOT/task1/log2.txt" & PID2=$!

# node3
python3 "$ROOT/myleprocess.py" --task 1 \
  --config "$ROOT/config_task1_node3.txt" \
  --log "$ROOT/task1/log3.txt" & PID3=$!

echo "PIDs: $PID1 $PID2 $PID3"
echo "Logs: task1/log1.txt task1/log2.txt task1/log3.txt"

wait
