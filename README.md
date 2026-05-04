# FMCG Pipeline - Inventory & Margin Reconciliation

End-to-end ETL pipeline simulating the data flow at a New Zealand computing hardware retailer. Three source systems - POS transactions, ERP inventory movements, and supplier cost master - flow through a PostgreSQL warehouse with conformed dimensions, facts, an SCD Type 2 product-cost dimension, and analytics-ready marts for margin and stock reconciliation.

The project is built to demonstrate production-style data engineering patterns: SCD2 history tracking, idempotent fact loads, defensive database constraints, dense inventory reconciliation with window functions and forward-fill, Airflow orchestration, audit logging, and portfolio-ready analytical SQL.

## Stack

- PostgreSQL 15 for warehouse, mart, and audit schemas
- Apache Airflow 2.9 for orchestration
- Python, pandas, and SQLAlchemy for transforms
- Docker Compose for local runtime

## Architecture

The schema map is in [Option_A_Schema_Map.png](Option_A_Schema_Map.png).

- `staging`: permissive truncate-and-reload source tables
- `warehouse`: Type 1 dimensions, SCD2 cost dimension, sales and inventory facts, and ERP stock snapshots
- `mart`: daily margin summary, dense stock reconciliation, reconciliation scope, and DQ alerts
- `audit`: SCD2 change log and ETL run log

The deployed schema creates 17 application tables across those four schemas.

## Quick Start

```bash
# First-time setup
docker compose up -d --build

# Register the Airflow connection, deploy schema, generate data, and load day 1 files
bash setup.sh
```

Open `http://localhost:8080`, log in as `admin` / `admin`, enable the `pb_tech_etl` DAG, and trigger it.

To run the day 2 scenario after day 1 has loaded:

```bash
bash scripts/switch_to_day.sh 2
# Trigger the pb_tech_etl DAG again from Airflow
```

To run the portfolio/demo analytics queries after a DAG run:

```bash
docker exec -i pb_postgres psql -U airflow -d pbtech_warehouse < sql/03_analytics_queries.sql
```

## Testing

Local checks:

```bash
python3 -m pip install -r requirements.txt
python3 scripts/test_transforms.py
python3 scripts/verify_data.py
python3 -m compileall dags scripts
bash -n setup.sh
bash -n scripts/switch_to_day.sh
```

Container checks:

```bash
docker compose up -d --build
bash setup.sh
docker exec pb_airflow airflow dags list | grep pb_tech_etl
```

## DAG Flow

`pb_tech_etl` runs these stages:

1. Wait for source files.
2. Load raw CSV/JSON extracts into staging.
3. Run hard and soft data-quality checks.
4. Build Type 1 dimensions and merge the SCD2 product-cost dimension.
5. Load sales, inventory movement, and inventory snapshot warehouse tables.
6. Refresh daily margin and stock reconciliation marts.
7. Write DQ alerts for orphan costs and reconciliation drift.

The pipeline is idempotent: re-running the DAG against the same source files does not duplicate warehouse facts.

## Data Scenario

The simulator creates two days of deterministic source data:

- Day 1 establishes the initial product, cost, sales, movement, and stock snapshot state.
- Day 2 introduces SCD2 scenarios: new SKUs, Type 2 cost/supplier changes, Type 1 UOM updates, discontinued SKUs, unchanged SKUs, and one deliberate orphan-cost transaction.
- Inventory snapshots include normal small variance plus a deliberate stock drift case for reconciliation alerts.

## Phase Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Project skeleton + Docker Compose | Done |
| 2 | Data simulation with two-day source files | Done |
| 3 | Schema deployment | Done |
| 4 | Transform modules | Done |
| 5 | Airflow DAG + minimum viable pipeline | Done |
| 6 | Real SCD2 merge | Done |
| 7 | Dense stock reconciliation logic | Done |
| 8 | Polish + analytics queries | Done |

## Key Files

- [dags/pb_tech_etl.py](dags/pb_tech_etl.py): Airflow DAG and task wrappers
- [dags/transforms/scd2.py](dags/transforms/scd2.py): SCD Type 2 merge logic
- [dags/transforms/inventory.py](dags/transforms/inventory.py): dense running balance and stock reconciliation
- [dags/transforms/marts.py](dags/transforms/marts.py): margin mart builder
- [sql/01_schema.sql](sql/01_schema.sql): full schema deployment
- [sql/02_verify_constraints.sql](sql/02_verify_constraints.sql): defensive constraint probes
- [sql/03_analytics_queries.sql](sql/03_analytics_queries.sql): read-only analytics/demo queries
- [scripts/generate_data.py](scripts/generate_data.py): deterministic data generator
- [scripts/switch_to_day.sh](scripts/switch_to_day.sh): swaps day 1 or day 2 files into `data/raw/`

## Notes

- `warehouse.dim_product_cost` enforces one current cost row per SKU with a partial unique index.
- `warehouse.fact_sales.cost_key` stores the cost row active at sale time, so mart refreshes do not repeat the SCD2 date-range lookup.
- `warehouse.fact_inventory_movements` enforces signed movement conventions with CHECK constraints.
- `mart.stock_reconciliation` uses a scoped product/warehouse date spine, cumulative stock movement, and forward-fill to reconcile days without movement.
