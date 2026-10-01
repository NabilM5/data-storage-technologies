#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose up -d --build
docker compose ps

python3 scripts/load_data.py
python3 scripts/benchmark.py
python3 scripts/backup_restore.py
python3 scripts/mvcc_demo.py
python3 scripts/patroni_demo.py

echo "All checks finished. Evidence is in evidence/."

