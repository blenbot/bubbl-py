#!/usr/bin/env bash

echo "Checking Redis service..."
if ! brew services list | grep -E '^redis\s.*started' > /dev/null; then
  echo "Starting Redis…"
  brew services start redis
  until redis-cli ping 2>/dev/null | grep -q PONG; do
    echo -n "."
    sleep 1
  done
  echo " Redis is up."
else
  echo "Redis already running."
fi

cd "$HOME/Desktop/bubbl-py" || { echo "Project folder not found"; exit 1; }

PID=$(sudo lsof -ti tcp:8080)
if [ -n "${PID}" ]; then
  echo "Killing process(es) on port 8080: ${PID}"
  echo "${PID}" | xargs sudo kill -9
fi

if [ ! -d .venv ]; then
  echo "Creating Python 3.12 venv…"
  uv venv --python 3.12.0
fi

source .venv/bin/activate

echo "Installing dependencies…"
uv sync

echo "Starting bubbl on port 8080…"
uv run main.py