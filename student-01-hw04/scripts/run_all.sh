#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p evidence
exec > >(tee evidence/clean_run.txt) 2>&1

if [[ "${1:-}" == "--clean" ]]; then
    echo "Removing only the HW4 Docker volumes for a clean run."
    docker compose down -v
fi

docker compose up -d --wait
docker compose ps | tee evidence/environment.txt

python3 scripts/load.py
python3 scripts/check_equal.py
python3 scripts/benchmark.py

echo "All checks finished successfully. Evidence is in evidence/."

