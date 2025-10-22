#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "Starting Task 2 (5 nodes)..."

# y (two servers)
python3 "$ROOT/myleprocess.py" --task 2 --type y \
  --config "$ROOT/config_task2_y.txt" \
  --log "$ROOT/task2/logy.txt" & PIDY=$!

# x (two clients)
python3 "$ROOT/myleprocess.py" --task 2 --type x \
  --config "$ROOT/config_task2_x.txt" \
  --log "$ROOT/task2/logx.txt" & PIDX=$!

# n1
python3 "$ROOT/myleprocess.py" --task 2 --type n \
  --config "$ROOT/config_task2_n1.txt" \
  --log "$ROOT/task2/log1.txt" & PID1=$!

# n2
python3 "$ROOT/myleprocess.py" --task 2 --type n \
  --config "$ROOT/config_task2_n2.txt" \
  --log "$ROOT/task2/log2.txt" & PID2=$!

# n3
python3 "$ROOT/myleprocess.py" --task 2 --type n \
  --config "$ROOT/config_task2_n3.txt" \
  --log "$ROOT/task2/log3.txt" & PID3=$!

echo "PIDs: Y=$PIDY X=$PIDX N1=$PID1 N2=$PID2 N3=$PID3"
echo "Logs: task2/logy.txt task2/logx.txt task2/log1.txt task2/log2.txt task2/log3.txt"

wait
