-- =============================================================================
-- PB Tech pipeline — full schema deployment
-- =============================================================================
-- 17 application tables across 4 schemas:
--   staging   (4 tables) — permissive landing for raw data
--   warehouse (7 tables) — dimensions, facts, and the SCD Type 2 cost dimension
--   mart      (4 tables) — analytics-ready outputs + DQ alerts + recon scope
--   audit     (2 tables) — operational observability
--
-- Idempotent: DROP SCHEMA ... CASCADE at the top wipes any prior state.
-- Run with:
--   docker exec -i pb_postgres psql -U airflow -d pbtech_warehouse < sql/01_schema.sql
--
-- After running, sql/02_verify_constraints.sql probes the defensive properties
-- (partial unique index, CHECK constraints, sign convention).
-- =============================================================================

-- ============================================================================
-- Wipe any prior state (safe re-run)
-- ============================================================================
DROP SCHEMA IF EXISTS staging   CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;
DROP SCHEMA IF EXISTS mart      CASCADE;
DROP SCHEMA IF EXISTS audit     CASCADE;

CREATE SCHEMA staging;
CREATE SCHEMA warehouse;
CREATE SCHEMA mart;
CREATE SCHEMA audit;

-- ============================================================================
-- STAGING SCHEMA — permissive types, no constraints, truncate-and-reload
-- ============================================================================

-- Transactions: TEXT for transaction_date because the source has mixed formats.
-- Cleaning happens in the transform step, not at load time.
CREATE TABLE staging.transactions (
    transaction_id   TEXT,
    transaction_date TEXT,
    store_id         TEXT,
    sku              TEXT,
    quantity         INTEGER,
    unit_price       NUMERIC(10, 2),
    total_amount     NUMERIC(12, 2),
    customer_id      TEXT,
    payment_method   TEXT,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory movements: signed quantity convention is enforced in warehouse layer,
-- not staging. Staging accepts whatever the source gives us.
CREATE TABLE staging.inv_movements (
    movement_id      TEXT,
    movement_date    TEXT,
    sku              TEXT,
    warehouse_id     TEXT,
    movement_type    TEXT,
    quantity         INTEGER,
    reference_id     TEXT,
    notes            TEXT,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cost master: 4-decimal precision because cost margins matter at the cent level.
-- This is the source of truth that feeds the SCD2 merge into dim_product_cost.
CREATE TABLE staging.cost_master (
    sku              TEXT,
    supplier_id      TEXT,
    unit_cost        NUMERIC(10, 4),
    currency         TEXT DEFAULT 'NZD',
    uom              TEXT DEFAULT 'each',
    extracted_at     TIMESTAMP,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory snapshot: end-of-day ERP-reported stock, used for reconciliation.
CREATE TABLE staging.inventory_snapshot (
    snapshot_date     TEXT,
    sku               TEXT,
    warehouse_id      TEXT,
    reported_qty      INTEGER,
    snapshot_source   TEXT,
    snapshot_taken_at TEXT,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- WAREHOUSE SCHEMA — Type 1 dimensions
-- ============================================================================

CREATE TABLE warehouse.dim_product (
    product_key   SERIAL PRIMARY KEY,
    sku           TEXT NOT NULL UNIQUE,
    product_name  TEXT,
    brand         TEXT,
    category      TEXT,
    list_price    NUMERIC(10, 2),
    is_active     BOOLEAN DEFAULT TRUE,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_dim_product_category ON warehouse.dim_product(category);
CREATE INDEX idx_dim_product_brand    ON warehouse.dim_product(brand);

CREATE TABLE warehouse.dim_store (
    store_key      SERIAL PRIMARY KEY,
    store_id       TEXT NOT NULL UNIQUE,
    store_name     TEXT,
    region         TEXT,
    store_type     TEXT,
    is_warehouse   BOOLEAN DEFAULT FALSE,
    home_warehouse_id TEXT,   -- which warehouse fulfils this store
    open_date      DATE,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (store_type IN ('Physical', 'Online'))
);
CREATE INDEX idx_dim_store_region    ON warehouse.dim_store(region);
CREATE INDEX idx_dim_store_warehouse ON warehouse.dim_store(is_warehouse) WHERE is_warehouse = TRUE;

CREATE TABLE warehouse.dim_date (
    date_key      INTEGER PRIMARY KEY,           -- YYYYMMDD as integer
    full_date     DATE NOT NULL UNIQUE,
    year          INTEGER NOT NULL,
    quarter       INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT NOT NULL,
    day           INTEGER NOT NULL,
    day_of_week   INTEGER NOT NULL,
    day_name      TEXT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_nz_holiday BOOLEAN DEFAULT FALSE
);

-- ============================================================================
-- WAREHOUSE SCHEMA — SCD Type 2 dimension (the centrepiece)
-- ============================================================================
-- Every constraint on this table prevents a real production bug class.
-- See sql/02_verify_constraints.sql for proof that they fire correctly.

CREATE TABLE warehouse.dim_product_cost (
    cost_key         SERIAL PRIMARY KEY,
    sku              TEXT NOT NULL,
    -- Type 2 attributes (changes create new history rows)
    unit_cost        NUMERIC(10, 4) NOT NULL,
    supplier_id      TEXT NOT NULL,
    -- Type 1 attributes (changes overwrite in place)
    currency         TEXT DEFAULT 'NZD',
    uom              TEXT DEFAULT 'each',
    -- SCD2 metadata
    effective_from   DATE NOT NULL,
    effective_to     DATE NOT NULL DEFAULT '9999-12-31',
    is_current       BOOLEAN NOT NULL DEFAULT TRUE,
    -- Audit
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Defensive constraints
    CHECK (effective_from < effective_to),
    CHECK (NOT is_current OR effective_to = '9999-12-31')
);

-- THE crucial constraint: at most one current row per SKU. If the merge logic
-- ever fails to close a row before opening a new one, this fires at insert time.
CREATE UNIQUE INDEX idx_dim_product_cost_one_current
    ON warehouse.dim_product_cost (sku)
    WHERE is_current = TRUE;

-- Lookup index for the fact-load date-range join.
CREATE INDEX idx_dim_product_cost_lookup
    ON warehouse.dim_product_cost (sku, effective_from, effective_to);

-- Sentinel row for orphan transactions (cost_key = -1).
-- The fact-load uses LEFT JOIN + COALESCE(c.cost_key, -1), so transactions
-- referencing SKUs without cost data attach to this row instead of being dropped.
-- We disable the auto-increment for this insert so cost_key really is -1.
INSERT INTO warehouse.dim_product_cost
    (cost_key, sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES
    (-1, '__UNKNOWN__', 0.00, '__UNKNOWN__', '2000-01-01', '9999-12-31', FALSE);

-- ============================================================================
-- WAREHOUSE SCHEMA — Fact tables
-- ============================================================================

CREATE TABLE warehouse.fact_sales (
    sale_key         BIGSERIAL PRIMARY KEY,
    transaction_id   TEXT NOT NULL UNIQUE,    -- idempotency key
    -- Foreign keys
    date_key         INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    product_key      INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    store_key        INTEGER NOT NULL REFERENCES warehouse.dim_store(store_key),
    cost_key         INTEGER NOT NULL REFERENCES warehouse.dim_product_cost(cost_key),
    -- Measures
    quantity         INTEGER NOT NULL,
    unit_price       NUMERIC(10, 2) NOT NULL,
    total_amount     NUMERIC(12, 2) NOT NULL,
    -- Derived flags
    is_return        BOOLEAN NOT NULL,
    is_orphan_cost   BOOLEAN NOT NULL DEFAULT FALSE,
    -- Degenerate dimensions
    customer_id      TEXT,
    payment_method   TEXT,
    -- Audit
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CHECK (quantity != 0)
);
CREATE INDEX idx_fact_sales_date    ON warehouse.fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON warehouse.fact_sales(product_key);
CREATE INDEX idx_fact_sales_store   ON warehouse.fact_sales(store_key);
CREATE INDEX idx_fact_sales_cost    ON warehouse.fact_sales(cost_key);
CREATE INDEX idx_fact_sales_orphan
    ON warehouse.fact_sales(is_orphan_cost) WHERE is_orphan_cost = TRUE;

CREATE TABLE warehouse.fact_inventory_movements (
    movement_key     BIGSERIAL PRIMARY KEY,
    movement_id      TEXT NOT NULL UNIQUE,
    -- Foreign keys
    date_key         INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    product_key      INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    warehouse_key    INTEGER NOT NULL REFERENCES warehouse.dim_store(store_key),
    -- Measure (signed)
    quantity         INTEGER NOT NULL,
    -- Type taxonomy
    movement_type    TEXT NOT NULL,
    -- Reference data
    reference_id     TEXT,
    notes            TEXT,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints — the sign convention is enforced here so the running-balance
    -- window function can trust the signs without defensive logic
    CHECK (movement_type IN ('receipt', 'sale_out', 'transfer_in',
                             'transfer_out', 'adjustment')),
    CHECK (quantity != 0),
    CHECK (
        (movement_type IN ('receipt', 'transfer_in')   AND quantity > 0) OR
        (movement_type IN ('sale_out', 'transfer_out') AND quantity < 0) OR
        (movement_type = 'adjustment')   -- adjustments can go either way
    )
);
-- Composite index matches the running-balance window function's
-- PARTITION BY (product_key, warehouse_key) ORDER BY date_key
CREATE INDEX idx_fact_inv_sku_wh_date
    ON warehouse.fact_inventory_movements(product_key, warehouse_key, date_key);
CREATE INDEX idx_fact_inv_date ON warehouse.fact_inventory_movements(date_key);
CREATE INDEX idx_fact_inv_type ON warehouse.fact_inventory_movements(movement_type);

-- ============================================================================
-- WAREHOUSE SCHEMA — Bridge table for ERP-reported snapshots
-- ============================================================================

CREATE TABLE warehouse.bridge_inventory_snapshot (
    snapshot_key      BIGSERIAL PRIMARY KEY,
    date_key          INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    product_key       INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    warehouse_key     INTEGER NOT NULL REFERENCES warehouse.dim_store(store_key),
    reported_qty      INTEGER NOT NULL,
    snapshot_source   TEXT NOT NULL DEFAULT 'ERP',
    snapshot_taken_at TIMESTAMP NOT NULL,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date_key, product_key, warehouse_key, snapshot_source)
);
CREATE INDEX idx_inv_snapshot_lookup
    ON warehouse.bridge_inventory_snapshot(product_key, warehouse_key, date_key);

-- ============================================================================
-- MART SCHEMA — Analytics-ready outputs
-- ============================================================================

CREATE TABLE mart.daily_margin_summary (
    summary_date         DATE NOT NULL,
    region               TEXT NOT NULL,
    store_type           TEXT NOT NULL,
    category             TEXT NOT NULL,
    -- Volume measures
    transaction_count    INTEGER NOT NULL,
    units_sold           INTEGER NOT NULL,
    return_count         INTEGER NOT NULL,
    -- Revenue measures (2 decimals — display precision)
    gross_revenue        NUMERIC(14, 2) NOT NULL,
    return_value         NUMERIC(14, 2) NOT NULL,
    net_revenue          NUMERIC(14, 2) NOT NULL,
    -- Cost measures (4 decimals — preserve precision through aggregation)
    total_cost           NUMERIC(14, 4) NOT NULL,
    return_cost          NUMERIC(14, 4) NOT NULL,
    -- Margin
    gross_margin         NUMERIC(14, 4) NOT NULL,
    margin_pct           NUMERIC(7, 4),       -- nullable: div by zero on no-revenue days
    -- Quality flags
    orphan_txn_count     INTEGER NOT NULL DEFAULT 0,
    orphan_value         NUMERIC(14, 2) NOT NULL DEFAULT 0,
    -- Audit
    refreshed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_date, region, store_type, category)
);
CREATE INDEX idx_margin_date     ON mart.daily_margin_summary(summary_date);
CREATE INDEX idx_margin_category ON mart.daily_margin_summary(category);

CREATE TABLE mart.stock_reconciliation (
    reconciliation_date DATE NOT NULL,
    product_key         INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    warehouse_key       INTEGER NOT NULL REFERENCES warehouse.dim_store(store_key),
    -- The two competing measurements
    derived_qty         INTEGER NOT NULL,
    reported_qty        INTEGER,
    snapshot_source     TEXT,
    -- Reconciliation outputs
    abs_variance        INTEGER NOT NULL,
    pct_variance        NUMERIC(7, 4),
    alert_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    alert_reason        TEXT,
    -- Audit
    refreshed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (reconciliation_date, product_key, warehouse_key)
);
CREATE INDEX idx_recon_date ON mart.stock_reconciliation(reconciliation_date);
CREATE INDEX idx_recon_alerts
    ON mart.stock_reconciliation(reconciliation_date, alert_flag)
    WHERE alert_flag = TRUE;

CREATE TABLE mart.dim_reconciliation_scope (
    product_key      INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    warehouse_key    INTEGER NOT NULL REFERENCES warehouse.dim_store(store_key),
    is_in_scope      BOOLEAN NOT NULL DEFAULT TRUE,
    added_at         DATE NOT NULL DEFAULT CURRENT_DATE,
    removed_at       DATE,
    notes            TEXT,
    PRIMARY KEY (product_key, warehouse_key)
);

CREATE TABLE mart.dq_alerts (
    alert_id         BIGSERIAL PRIMARY KEY,
    dag_run_id       TEXT NOT NULL,
    detected_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    alert_severity   TEXT NOT NULL,
    alert_type       TEXT NOT NULL,
    affected_table   TEXT NOT NULL,
    affected_rows    INTEGER,
    sample_values    JSONB,
    description      TEXT NOT NULL,
    resolved_at      TIMESTAMP,
    resolution_notes TEXT,
    CHECK (alert_severity IN ('INFO', 'WARNING', 'ERROR'))
);
CREATE INDEX idx_dq_alerts_unresolved
    ON mart.dq_alerts(detected_at, alert_severity)
    WHERE resolved_at IS NULL;
CREATE INDEX idx_dq_alerts_dag_run ON mart.dq_alerts(dag_run_id);

-- ============================================================================
-- AUDIT SCHEMA — operational observability
-- ============================================================================

CREATE TABLE audit.scd2_change_log (
    log_id            BIGSERIAL PRIMARY KEY,
    change_timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dag_run_id        TEXT,
    dimension_table   TEXT NOT NULL,
    natural_key       TEXT NOT NULL,
    change_type       TEXT NOT NULL,
    closed_cost_key   INTEGER,
    new_cost_key      INTEGER,
    old_attributes    JSONB,
    new_attributes    JSONB,
    notes             TEXT,
    CHECK (change_type IN ('insert_initial', 'type2_change',
                           'type1_update', 'discontinued'))
);
CREATE INDEX idx_scd2_log_natural_key ON audit.scd2_change_log(natural_key);
CREATE INDEX idx_scd2_log_timestamp   ON audit.scd2_change_log(change_timestamp);
CREATE INDEX idx_scd2_log_dag_run     ON audit.scd2_change_log(dag_run_id);

CREATE TABLE audit.etl_run_log (
    run_id           BIGSERIAL PRIMARY KEY,
    dag_id           TEXT NOT NULL,
    dag_run_id       TEXT NOT NULL,
    task_id          TEXT NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    -- Generated column: Postgres computes from started_at and finished_at automatically.
    -- SQL Server equivalent: a computed column with the same expression.
    duration_seconds NUMERIC(10, 2)
        GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finished_at - started_at)))
        STORED,
    status           TEXT NOT NULL,
    rows_in          INTEGER,
    rows_out         INTEGER,
    rows_changed     INTEGER,
    notes            TEXT,
    error_message    TEXT,
    CHECK (status IN ('SUCCESS', 'FAILED', 'RUNNING', 'SKIPPED'))
);
CREATE INDEX idx_etl_log_dag_run ON audit.etl_run_log(dag_id, dag_run_id);
CREATE INDEX idx_etl_log_task    ON audit.etl_run_log(task_id, started_at);
CREATE INDEX idx_etl_log_failures
    ON audit.etl_run_log(status, started_at) WHERE status = 'FAILED';

-- ============================================================================
-- Inventory of what was created (run after deployment to verify)
-- ============================================================================
-- Expected output:
--   staging:    4 tables  (transactions, inv_movements, cost_master, inventory_snapshot)
--   warehouse:  7 tables  (3 Type1 dims + dim_product_cost + 2 facts + bridge)
--   mart:       4 tables  (margin, reconciliation, scope, dq_alerts)
--   audit:      2 tables  (scd2_change_log, etl_run_log)
--   = 17 application tables (more than 13 because staging.inventory_snapshot
--     was added late and dim_reconciliation_scope and dq_alerts each count too)

\echo '\n=== Schema deployment summary ==='
SELECT table_schema, COUNT(*) AS table_count
FROM information_schema.tables
WHERE table_schema IN ('staging', 'warehouse', 'mart', 'audit')
  AND table_type = 'BASE TABLE'
GROUP BY table_schema
ORDER BY table_schema;
