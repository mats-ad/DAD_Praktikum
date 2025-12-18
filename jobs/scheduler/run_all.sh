#!/usr/bin/env bash
set -euo pipefail

echo "[SCHED] run_all start: $(date -Is)"

python -u /app/batch/prices_batch_daily.py

python -u /app/batch/social_batch_daily.py

python -u /app/batch/joined_daily.py

python -u /app/batch/load_joined_to_postgres.py

export PGPASSWORD="${POSTGRES_PASSWORD}"
psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /app/serving/investment_signal.sql

echo "[SCHED] run_all done: $(date -Is)"