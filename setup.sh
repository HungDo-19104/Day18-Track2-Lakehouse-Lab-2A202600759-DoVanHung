#!/usr/bin/env bash
# One-shot setup for Day 18 Lakehouse Lab.
# Equivalent to `make up && make smoke` for users without `make`.
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found. Install Docker Desktop first." >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "ERROR: 'docker compose' (v2) not available. Update Docker Desktop." >&2
  exit 1
fi

COMPOSE="docker compose -f docker/docker-compose.yml"

echo "[1/3] Starting MinIO + Spark/Jupyter…"
$COMPOSE up -d

echo "[2/3] Waiting for Spark container to finish init (max 90s)…"
for i in {1..18}; do
  if $COMPOSE exec -T spark bash -c 'test -f /home/jovyan/.local/bin/jupytext' 2>/dev/null; then
    break
  fi
  sleep 5
done

echo "[3/3] Running smoke test…"
$COMPOSE exec -T spark python /workspace/scripts/verify.py

cat <<EOF

  Lab is ready.
  Jupyter Lab → http://localhost:8888  (token: lakehouse)
  MinIO       → http://localhost:9001  (minioadmin / minioadmin)

  Next: open notebooks/01_delta_basics.ipynb
EOF
