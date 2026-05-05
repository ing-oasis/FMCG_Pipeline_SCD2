#!/usr/bin/env bash
# One-time setup after `docker compose up -d --build`.
#
# Registers the retail_postgres connection in Airflow (so the DAG can reach
# the warehouse database), and deploys the warehouse schema.
#
# Safe to re-run: re-registering the connection is a no-op, and the schema
# script does DROP SCHEMA ... CASCADE before recreating, so it always lands
# in a clean state.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo "→ Step 1: Wait for Airflow webserver to be reachable..."
for i in $(seq 1 60); do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health \
            | grep -q 200; then
        echo "  Airflow up after ${i}0s"
        break
    fi
    sleep 10
    if [ "$i" -eq 60 ]; then
        echo "  ERROR: Airflow didn't come up in 10 minutes"
        exit 1
    fi
done

echo "→ Step 2: Register retail_postgres connection in Airflow..."
docker exec retail_airflow airflow connections delete retail_postgres 2>/dev/null || true
docker exec retail_airflow airflow connections add retail_postgres \
    --conn-type postgres \
    --conn-host retail_postgres \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema retail_warehouse \
    --conn-port 5432 \
    >/dev/null
echo "  Connection registered"

echo "→ Step 3: Ensure local Airflow admin login..."
if docker exec retail_airflow airflow users reset-password \
        --username admin \
        --password admin \
        >/dev/null 2>&1; then
    echo "  Admin password reset (admin / admin)"
else
    docker exec retail_airflow airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin \
        >/dev/null
    echo "  Admin user created (admin / admin)"
fi

echo "→ Step 4: Deploy warehouse schema..."
docker exec -i retail_postgres psql -U airflow -d retail_warehouse \
    < sql/01_schema.sql > /dev/null
echo "  Schema deployed (17 tables across 4 schemas)"

echo "→ Step 5: Verify the constraints..."
docker exec -i retail_postgres psql -U airflow -d retail_warehouse \
    -v ON_ERROR_STOP=0 < sql/02_verify_constraints.sql 2>&1 \
    | tail -3 | head -1
echo "  Constraint verification complete (ERROR messages above are expected)"

echo "→ Step 6: Generate simulation data (if not already present)..."
if [ ! -f scripts/sim_data/day1/transactions.csv ]; then
    python3 scripts/generate_data.py
else
    echo "  Already generated — skipping"
fi

echo "→ Step 7: Switch to day 1 (initial state)..."
bash scripts/switch_to_day.sh 1 > /dev/null
echo "  data/raw/ now has day 1 source files"

echo ""
echo "============================================================"
echo "Setup complete. Next steps:"
echo "  1. Open http://localhost:8080 (admin / admin)"
echo "  2. Find the 'retail_etl' DAG in the list"
echo "  3. Toggle it on (top-left switch)"
echo "  4. Click 'Trigger DAG' (▶ button, top-right)"
echo "  5. Watch tasks turn green"
echo ""
echo "============================================================"
