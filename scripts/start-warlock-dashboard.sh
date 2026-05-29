#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/kilo

# Stop old instance if it exists
pkill -f "/home/loc-llm/llm-status-ui/app.py" || true
pkill -f "/home/loc-llm/warlock_ingester/ui/app.py" || true

# Start new dashboard
nohup /home/loc-llm/warlock_ingester/.venv/bin/python \
  /home/loc-llm/warlock_ingester/ui/app.py \
  >/tmp/kilo/warlock-dashboard.log 2>&1 &
