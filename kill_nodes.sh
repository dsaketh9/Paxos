#!/bin/bash
ports=(60691 60692 60693 60694 60695)
for p in "${ports[@]}"; do
  pid=$(lsof -t -i:$p)
  if [ -n "$pid" ]; then
    echo "Killing process on port $p (PID=$pid)"
    kill -9 $pid
  fi
done