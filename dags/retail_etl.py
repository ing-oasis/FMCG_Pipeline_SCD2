"""
Retail inventory & margin pipeline — complete DAG.

Task structure (matches the design diagram):
    start
       ↓
    [TaskGroup: wait_for_sources]   four FileSensors in parallel
       ↓
    [TaskGroup: load_staging]       four staging loads in parallel
       ↓
    data_quality_check
       ↓
    [TaskGroup: build_dimensions]   3 Type 1 dims in parallel + SCD2 cost
       ↓
    [TaskGroup: build_facts]        fact_sales + fact_inventory_movements
                                    + bridge_inventory_snapshot in parallel
       ↓
    [TaskGroup: build_marts]        margin + reconciliation in parallel
       ↓
    finish

The pipeline is idempotent: re-running the DAG against the same source files
produces the same warehouse state. This is what makes "swap to day 2 and
re-trigger" work cleanly.
"""

import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from sqlalchemy import text

# Import our transform modules
from transforms.cleaning import (
    clean_transactions, clean_inv_movements,
    clean_cost_master, clean_inventory_snapshot,
)
from transforms.dimensions import (
    build_dim_date, build_dim_product_from_transactions,
    build_dim_store_from_master,
)
from transforms.scd2 import scd2_merge, SCD2_DIMENSION_CONFIG
from transforms.inventory import (
    build_reconciliation_scope,
    compute_running_balance,
    reconcile_stock,
)
from transforms.marts import build_daily_margin_summary
from transforms.db import run_query, write_dq_alert

log = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
DAG_ID         = "retail_etl"
DATA_DIR       = "/opt/airflow/data/raw"      # mounted from host
POSTGRES_CONN  = "retail_postgres"
DEFAULT_ARGS = {
    "owner":           "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# =============================================================================
# Task implementations
# =============================================================================

def _get_engine():
    """Convenience wrapper — every task starts with this line."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONN).get_sqlalchemy_engine()


def _df_to_table(engine, df, table_name, schema=None, truncate=False, replace=False):
    """Bulk-load a DataFrame into a database table using parameterised SQL.

    Replaces pandas.to_sql() which breaks under the pandas 2.2 +
    SQLAlchemy 1.4 combination (Airflow 2.9 pins SQLAlchemy < 2.0,
    but pandas 2.2 requires 2.0+ for its engine detection)."""
    full_name = f"{schema}.{table_name}" if schema else table_name
    records = df.where(df.notna(), None).to_dict(orient="records")

    with engine.begin() as conn:
        if replace:
            conn.execute(text(f"DROP TABLE IF EXISTS {full_name}"))
            col_defs = []
            for col in df.columns:
                if pd.api.types.is_bool_dtype(df[col]):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_integer_dtype(df[col]):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(df[col]):
                    sql_type = "DOUBLE PRECISION"
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "TEXT"
                col_defs.append(f"{col} {sql_type}")
            conn.execute(text(
                f"CREATE TABLE {full_name} ({', '.join(col_defs)})"
            ))
        elif truncate:
            conn.execute(text(f"TRUNCATE TABLE {full_name}"))

        if records:
            cols = df.columns.tolist()
            col_list = ", ".join(cols)
            placeholders = ", ".join(f":{c}" for c in cols)
            conn.execute(
                text(f"INSERT INTO {full_name} ({col_list}) VALUES ({placeholders})"),
                records,
            )


# ---------------------------------------------------------------------------
# Staging-load tasks (one per source file)
# ---------------------------------------------------------------------------

def load_stg_transactions():
    engine = _get_engine()
    df = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    log.info(f"Read {len(df):,} raw transaction rows")
    _df_to_table(engine, df, "transactions", schema="staging", truncate=True)
    log.info(f"Loaded {len(df):,} rows into staging.transactions")


def load_stg_inv_movements():
    engine = _get_engine()
    df = pd.read_csv(f"{DATA_DIR}/inv_movements.csv")
    log.info(f"Read {len(df):,} raw movement rows")
    _df_to_table(engine, df, "inv_movements", schema="staging", truncate=True)
    log.info(f"Loaded {len(df):,} rows into staging.inv_movements")


def load_stg_cost_master():
    engine = _get_engine()
    df = pd.read_csv(f"{DATA_DIR}/cost_master.csv")
    log.info(f"Read {len(df):,} raw cost rows")
    _df_to_table(engine, df, "cost_master", schema="staging", truncate=True)
    log.info(f"Loaded {len(df):,} rows into staging.cost_master")


def load_stg_inventory_snapshot():
    engine = _get_engine()
    df = pd.read_csv(f"{DATA_DIR}/inventory_snapshot.csv")
    log.info(f"Read {len(df):,} raw snapshot rows")
    _df_to_table(engine, df, "inventory_snapshot", schema="staging", truncate=True)
    log.info(f"Loaded {len(df):,} rows into staging.inventory_snapshot")


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

def data_quality_check(**context):
    """
    Hard checks raise (pipeline fails). Soft checks log to mart.dq_alerts.
    """
    engine = _get_engine()
    dag_run_id = context["dag_run"].run_id

    with engine.begin() as conn:
        txn_count = conn.execute(
            text("SELECT COUNT(*) FROM staging.transactions")).scalar()
        mov_count = conn.execute(
            text("SELECT COUNT(*) FROM staging.inv_movements")).scalar()
        cost_count = conn.execute(
            text("SELECT COUNT(*) FROM staging.cost_master")).scalar()

    log.info(f"Row counts — transactions={txn_count:,}, movements={mov_count:,}, "
             f"costs={cost_count:,}")

    # HARD: staging tables must have rows
    if txn_count == 0:
        raise ValueError("HARD DQ FAILURE: staging.transactions is empty")
    if mov_count == 0:
        raise ValueError("HARD DQ FAILURE: staging.inv_movements is empty")
    if cost_count == 0:
        raise ValueError("HARD DQ FAILURE: staging.cost_master is empty")

    # SOFT: orphan SKUs in transactions vs cost master
    with engine.begin() as conn:
        orphans = conn.execute(text("""
            SELECT DISTINCT TRIM(UPPER(t.sku)) AS sku
            FROM staging.transactions t
            WHERE TRIM(UPPER(t.sku)) NOT IN (
                SELECT TRIM(UPPER(sku)) FROM staging.cost_master
            )
        """)).fetchall()

    if orphans:
        orphan_skus = [r[0] for r in orphans][:10]
        log.warning(f"Soft DQ: {len(orphans)} SKU(s) in transactions have no cost: "
                    f"{orphan_skus}")
        write_dq_alert(
            engine,
            dag_run_id=dag_run_id,
            severity="WARNING",
            alert_type="orphan_skus_in_transactions",
            affected_table="staging.transactions",
            affected_rows=len(orphans),
            sample_values={"sample_orphan_skus": orphan_skus},
            description=f"{len(orphans)} unique SKUs in transactions have no "
                        f"matching row in cost_master — they will load with "
                        f"cost_key=-1 (sentinel).",
        )


# ---------------------------------------------------------------------------
# Type 1 dimension loads
# ---------------------------------------------------------------------------

def upsert_dim_date():
    engine = _get_engine()
    # Cover Apr 2026 (the simulation's date range) plus generous buffers
    df = build_dim_date("2026-01-01", "2026-12-31")
    with engine.begin() as conn:
        # ON CONFLICT DO NOTHING — date_key is stable, so re-runs are idempotent
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.dim_date
                    (date_key, full_date, year, quarter, month, month_name,
                     day, day_of_week, day_name, is_weekend, is_nz_holiday)
                VALUES
                    (:date_key, :full_date, :year, :quarter, :month, :month_name,
                     :day, :day_of_week, :day_name, :is_weekend, :is_nz_holiday)
                ON CONFLICT (date_key) DO NOTHING
            """), row.to_dict())
    log.info(f"Upserted {len(df):,} rows into warehouse.dim_date")


def upsert_dim_product():
    engine = _get_engine()

    with open(f"{DATA_DIR}/products.json") as f:
        products = pd.DataFrame(json.load(f))
    raw_txns = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    clean_txns = clean_transactions(raw_txns)
    df = build_dim_product_from_transactions(clean_txns, products)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.dim_product
                    (sku, product_name, brand, category, list_price, is_active)
                VALUES
                    (:sku, :product_name, :brand, :category, :list_price, :is_active)
                ON CONFLICT (sku) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    brand        = EXCLUDED.brand,
                    category     = EXCLUDED.category,
                    list_price   = EXCLUDED.list_price,
                    is_active    = EXCLUDED.is_active,
                    updated_at   = CURRENT_TIMESTAMP
            """), row.to_dict())
    log.info(f"Upserted {len(df):,} rows into warehouse.dim_product")


def upsert_dim_store():
    engine = _get_engine()
    stores_df = pd.read_csv(f"{DATA_DIR}/stores.csv")
    df = build_dim_store_from_master(stores_df)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.dim_store
                    (store_id, store_name, region, store_type, is_warehouse,
                     home_warehouse_id, open_date)
                VALUES
                    (:store_id, :store_name, :region, :store_type, :is_warehouse,
                     :home_warehouse_id, :open_date)
                ON CONFLICT (store_id) DO UPDATE SET
                    store_name        = EXCLUDED.store_name,
                    region            = EXCLUDED.region,
                    store_type        = EXCLUDED.store_type,
                    is_warehouse      = EXCLUDED.is_warehouse,
                    home_warehouse_id = EXCLUDED.home_warehouse_id,
                    open_date         = EXCLUDED.open_date,
                    updated_at        = CURRENT_TIMESTAMP
            """), row.to_dict())
    log.info(f"Upserted {len(df):,} rows into warehouse.dim_store")


# ---------------------------------------------------------------------------
# SCD Type 2 cost dimension
# ---------------------------------------------------------------------------

def merge_dim_product_cost(**context):
    """Run the SCD Type 2 merge with five-scenario classification.

    Reads current dim state, compares against today's snapshot, applies
    closes/inserts/updates atomically with audit log entries inside the
    same transaction. The partial unique index on (sku) WHERE is_current
    enforces the 'one current row per SKU' invariant — the merge logic
    cannot accidentally produce two current rows for the same SKU."""
    engine = _get_engine()
    raw = pd.read_csv(f"{DATA_DIR}/cost_master.csv")
    clean = clean_cost_master(raw)
    counts = scd2_merge(
        engine,
        dimension_name="dim_product_cost",
        source_df=clean,
        dag_run_id=context["dag_run"].run_id,
    )
    log.info(f"SCD2 merge counts: {counts}")


# ---------------------------------------------------------------------------
# Fact loads
# ---------------------------------------------------------------------------

def load_fact_sales():
    """Resolve surrogate keys via SQL JOIN, then INSERT into fact_sales.

    The cost_key date-range join is the architectural payoff: every row
    permanently records the cost row that was active when the sale happened.
    LEFT JOIN with COALESCE(c.cost_key, -1) means orphan SKUs get the
    sentinel cost_key (set in schema deployment as cost_key=-1)."""
    engine = _get_engine()

    # Read+clean transactions in pandas (re-using the same cleaner the
    # DQ check used). Then write to a temp table for the JOIN-and-load query.
    raw = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    clean = clean_transactions(raw)

    _df_to_table(engine, clean, "_txn_temp", replace=True)

    insert_sql = """
        INSERT INTO warehouse.fact_sales (
            transaction_id, date_key, product_key, store_key, cost_key,
            quantity, unit_price, total_amount, is_return, is_orphan_cost,
            customer_id, payment_method
        )
        SELECT
            t.transaction_id,
            d.date_key,
            p.product_key,
            s.store_key,
            COALESCE(c.cost_key, -1) AS cost_key,
            t.quantity,
            t.unit_price,
            t.total_amount,
            t.is_return,
            (c.cost_key IS NULL) AS is_orphan_cost,
            t.customer_id,
            t.payment_method
        FROM _txn_temp t
        JOIN warehouse.dim_date    d ON d.full_date = t.transaction_date::date
        JOIN warehouse.dim_product p ON p.sku       = t.sku
        JOIN warehouse.dim_store   s ON s.store_id  = t.store_id
        LEFT JOIN warehouse.dim_product_cost c
            ON  c.sku = t.sku
            AND t.transaction_date::date >= c.effective_from
            AND t.transaction_date::date <  c.effective_to
        ON CONFLICT (transaction_id) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        log.info(f"Inserted {result.rowcount:,} rows into warehouse.fact_sales "
                 f"(after dedup via UNIQUE constraint)")
        # Show how many got the sentinel
        orphan_count = conn.execute(text("""
            SELECT COUNT(*) FROM warehouse.fact_sales WHERE is_orphan_cost = TRUE
        """)).scalar()
        log.info(f"  {orphan_count:,} sales loaded with cost_key=-1 (orphans)")
        conn.execute(text("DROP TABLE IF EXISTS _txn_temp"))


def load_fact_inventory_movements():
    engine = _get_engine()
    raw = pd.read_csv(f"{DATA_DIR}/inv_movements.csv")
    clean = clean_inv_movements(raw)

    _df_to_table(engine, clean, "_mov_temp", replace=True)

    insert_sql = """
        INSERT INTO warehouse.fact_inventory_movements (
            movement_id, date_key, product_key, warehouse_key,
            quantity, movement_type, reference_id, notes
        )
        SELECT
            m.movement_id,
            d.date_key,
            p.product_key,
            s.store_key,
            m.quantity,
            m.movement_type,
            m.reference_id,
            m.notes
        FROM _mov_temp m
        JOIN warehouse.dim_date    d ON d.full_date = m.movement_date::date
        JOIN warehouse.dim_product p ON p.sku       = m.sku
        JOIN warehouse.dim_store   s ON s.store_id  = m.warehouse_id
        ON CONFLICT (movement_id) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        log.info(f"Inserted {result.rowcount:,} rows into "
                 f"warehouse.fact_inventory_movements")
        conn.execute(text("DROP TABLE IF EXISTS _mov_temp"))


def load_bridge_inventory_snapshot():
    engine = _get_engine()
    raw = pd.read_csv(f"{DATA_DIR}/inventory_snapshot.csv")
    clean = clean_inventory_snapshot(raw)

    _df_to_table(engine, clean, "_snap_temp", replace=True)

    insert_sql = """
        INSERT INTO warehouse.bridge_inventory_snapshot (
            date_key, product_key, warehouse_key,
            reported_qty, snapshot_source, snapshot_taken_at
        )
        SELECT
            d.date_key,
            p.product_key,
            s.store_key,
            sp.reported_qty,
            sp.snapshot_source,
            sp.snapshot_taken_at::timestamp
        FROM _snap_temp sp
        JOIN warehouse.dim_date    d ON d.full_date = sp.snapshot_date::date
        JOIN warehouse.dim_product p ON p.sku       = sp.sku
        JOIN warehouse.dim_store   s ON s.store_id  = sp.warehouse_id
        ON CONFLICT (date_key, product_key, warehouse_key, snapshot_source)
            DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        log.info(f"Inserted {result.rowcount:,} rows into "
                 f"warehouse.bridge_inventory_snapshot")
        conn.execute(text("DROP TABLE IF EXISTS _snap_temp"))


# ---------------------------------------------------------------------------
# Mart refreshes
# ---------------------------------------------------------------------------

def refresh_daily_margin():
    engine = _get_engine()
    rowcount = build_daily_margin_summary(engine)
    log.info(f"Refreshed mart.daily_margin_summary with {rowcount:,} rows")


def refresh_stock_reconciliation(**context):
    """Refresh dense stock reconciliation with forward-filled balances."""
    engine = _get_engine()
    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"

    scope_count = build_reconciliation_scope(engine)
    log.info(f"Reconciliation scope contains {scope_count:,} active pairs")

    with engine.begin() as conn:
        bounds = conn.execute(text("""
            WITH reconciliation_dates AS (
                SELECT d.full_date
                FROM warehouse.fact_inventory_movements f
                JOIN warehouse.dim_date d ON d.date_key = f.date_key
                UNION
                SELECT d.full_date
                FROM warehouse.bridge_inventory_snapshot s
                JOIN warehouse.dim_date d ON d.date_key = s.date_key
            )
            SELECT MIN(full_date)::date AS start_date,
                   MAX(full_date)::date AS end_date
            FROM reconciliation_dates
        """)).one()

    start_date, end_date = bounds[0], bounds[1]
    if start_date is None or end_date is None:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE mart.stock_reconciliation"))
        log.info("No movement or snapshot dates found; stock reconciliation cleared")
        return 0

    derived_df = compute_running_balance(engine, start_date, end_date)

    snapshot_query = text("""
        SELECT
            d.full_date AS reconciliation_date,
            s.product_key,
            s.warehouse_key,
            s.reported_qty,
            s.snapshot_source
        FROM warehouse.bridge_inventory_snapshot s
        JOIN warehouse.dim_date d ON d.date_key = s.date_key
        WHERE d.full_date BETWEEN :start_date AND :end_date
    """)
    snapshot_df = run_query(
        engine,
        snapshot_query,
        params={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    reconciled_df = reconcile_stock(derived_df, snapshot_df)

    if reconciled_df.empty:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE mart.stock_reconciliation"))
    else:
        _df_to_table(engine, reconciled_df, "stock_reconciliation",
                     schema="mart", truncate=True)

    rowcount = len(reconciled_df)
    alert_count = int(reconciled_df["alert_flag"].sum()) if rowcount else 0
    if alert_count:
        samples = (
            reconciled_df[reconciled_df["alert_flag"]]
            .head(10)[[
                "reconciliation_date",
                "product_key",
                "warehouse_key",
                "derived_qty",
                "reported_qty",
                "abs_variance",
                "pct_variance",
            ]]
            .assign(reconciliation_date=lambda df: df["reconciliation_date"].astype(str))
            .to_dict(orient="records")
        )
        write_dq_alert(
            engine,
            dag_run_id=dag_run_id,
            severity="WARNING",
            alert_type="stock_reconciliation_variance",
            affected_table="mart.stock_reconciliation",
            affected_rows=alert_count,
            sample_values={"sample_alerts": samples},
            description=(
                f"{alert_count} product/warehouse/day stock positions exceeded "
                "the reconciliation variance threshold."
            ),
        )

    log.info(f"Refreshed mart.stock_reconciliation with {rowcount:,} rows")
    log.info(f"  Stock reconciliation variance alerts: {alert_count:,}")
    return rowcount


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    description="Retail inventory & margin reconciliation pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",          # daily at 02:00 UTC (catchup off, demo manual)
    catchup=False,
    tags=["retail", "etl", "scd2", "reconciliation"],
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish",
                            trigger_rule="none_failed_min_one_success")

    # --- Sensors --------------------------------------------------------------
    with TaskGroup("wait_for_sources") as tg_sensors:
        wait_txns = FileSensor(
            task_id="wait_for_transactions",
            filepath=f"{DATA_DIR}/transactions.csv",
            poke_interval=10, timeout=60 * 60 * 2,        # 2 hours
            mode="reschedule",                             # don't hog a worker
            soft_fail=False, retries=0,
        )
        wait_movs = FileSensor(
            task_id="wait_for_inv_movements",
            filepath=f"{DATA_DIR}/inv_movements.csv",
            poke_interval=10, timeout=60 * 60 * 2,
            mode="reschedule", soft_fail=False, retries=0,
        )
        wait_costs = FileSensor(
            task_id="wait_for_cost_master",
            filepath=f"{DATA_DIR}/cost_master.csv",
            poke_interval=10, timeout=60 * 60 * 2,
            mode="reschedule", soft_fail=False, retries=0,
        )
        wait_snap = FileSensor(
            task_id="wait_for_inventory_snapshot",
            filepath=f"{DATA_DIR}/inventory_snapshot.csv",
            poke_interval=10, timeout=60 * 60 * 2,
            mode="reschedule", soft_fail=False, retries=0,
        )

    # --- Staging --------------------------------------------------------------
    with TaskGroup("load_staging") as tg_staging:
        t_stg_txns = PythonOperator(
            task_id="load_stg_transactions",
            python_callable=load_stg_transactions,
        )
        t_stg_movs = PythonOperator(
            task_id="load_stg_inv_movements",
            python_callable=load_stg_inv_movements,
        )
        t_stg_costs = PythonOperator(
            task_id="load_stg_cost_master",
            python_callable=load_stg_cost_master,
        )
        t_stg_snap = PythonOperator(
            task_id="load_stg_inventory_snapshot",
            python_callable=load_stg_inventory_snapshot,
        )

    # --- DQ -------------------------------------------------------------------
    t_dq = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        retries=0,                  # DQ failures shouldn't retry — fix the data
    )

    # --- Dimensions -----------------------------------------------------------
    with TaskGroup("build_dimensions") as tg_dims:
        t_dim_date = PythonOperator(
            task_id="upsert_dim_date",
            python_callable=upsert_dim_date,
        )
        t_dim_product = PythonOperator(
            task_id="upsert_dim_product",
            python_callable=upsert_dim_product,
        )
        t_dim_store = PythonOperator(
            task_id="upsert_dim_store",
            python_callable=upsert_dim_store,
        )
        t_dim_cost = PythonOperator(
            task_id="merge_dim_product_cost",
            python_callable=merge_dim_product_cost,
        )

    # --- Facts (depend on all dims) -------------------------------------------
    with TaskGroup("build_facts") as tg_facts:
        t_fact_sales = PythonOperator(
            task_id="load_fact_sales",
            python_callable=load_fact_sales,
        )
        t_fact_inv = PythonOperator(
            task_id="load_fact_inventory_movements",
            python_callable=load_fact_inventory_movements,
        )
        t_bridge = PythonOperator(
            task_id="load_bridge_inventory_snapshot",
            python_callable=load_bridge_inventory_snapshot,
        )

    # --- Marts (parallel) -----------------------------------------------------
    with TaskGroup("build_marts") as tg_marts:
        t_margin = PythonOperator(
            task_id="refresh_daily_margin",
            python_callable=refresh_daily_margin,
        )
        t_recon = PythonOperator(
            task_id="refresh_stock_reconciliation",
            python_callable=refresh_stock_reconciliation,
        )

    # --- Dependencies ---------------------------------------------------------
    start >> tg_sensors

    # Each sensor unblocks its corresponding staging load
    wait_txns  >> t_stg_txns
    wait_movs  >> t_stg_movs
    wait_costs >> t_stg_costs
    wait_snap  >> t_stg_snap

    # All staging loads must complete before DQ
    [t_stg_txns, t_stg_movs, t_stg_costs, t_stg_snap] >> t_dq

    # DQ unblocks all dimensions
    t_dq >> [t_dim_date, t_dim_product, t_dim_store, t_dim_cost]

    # Facts depend on the dimensions they reference
    [t_dim_date, t_dim_product, t_dim_store, t_dim_cost] >> t_fact_sales
    [t_dim_date, t_dim_product, t_dim_store] >> t_fact_inv
    [t_dim_date, t_dim_product, t_dim_store] >> t_bridge

    # Marts depend on facts
    t_fact_sales >> t_margin
    [t_fact_inv, t_bridge] >> t_recon

    # Finish
    [t_margin, t_recon] >> finish
