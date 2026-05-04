-- =============================================================================
-- PB Tech pipeline - analytics and demo queries
-- =============================================================================
-- Run after at least one successful pb_tech_etl DAG run:
--   docker exec -i pb_postgres psql -U airflow -d pbtech_warehouse < sql/03_analytics_queries.sql
--
-- These queries are read-only. They are designed for portfolio walkthroughs:
-- margin performance, SCD2 cost history, reconciliation drift, orphan-cost
-- exposure, and data-quality observability.
-- =============================================================================

\echo '\n=== 1. Daily margin by region and store type ==='
SELECT
    summary_date,
    region,
    store_type,
    SUM(transaction_count) AS transactions,
    SUM(units_sold) AS units_sold,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(SUM(total_cost), 4) AS total_cost,
    ROUND(SUM(gross_margin), 4) AS gross_margin,
    ROUND(
        SUM(gross_margin) / NULLIF(SUM(net_revenue), 0),
        4
    ) AS margin_pct
FROM mart.daily_margin_summary
GROUP BY summary_date, region, store_type
ORDER BY summary_date, gross_margin DESC;

\echo '\n=== 2. Category margin leaderboard ==='
SELECT
    category,
    SUM(transaction_count) AS transactions,
    SUM(units_sold) AS units_sold,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(SUM(gross_margin), 4) AS gross_margin,
    ROUND(
        SUM(gross_margin) / NULLIF(SUM(net_revenue), 0),
        4
    ) AS margin_pct,
    SUM(orphan_txn_count) AS orphan_txns
FROM mart.daily_margin_summary
GROUP BY category
ORDER BY gross_margin DESC;

\echo '\n=== 3. Orphan-cost exposure ==='
SELECT
    summary_date,
    category,
    region,
    store_type,
    orphan_txn_count,
    ROUND(orphan_value, 2) AS orphan_value,
    ROUND(net_revenue, 2) AS net_revenue,
    ROUND(orphan_value / NULLIF(net_revenue, 0), 4) AS orphan_revenue_pct
FROM mart.daily_margin_summary
WHERE orphan_txn_count > 0
ORDER BY summary_date, orphan_value DESC;

\echo '\n=== 4. SCD2 change counts by DAG run ==='
SELECT
    dag_run_id,
    change_type,
    COUNT(*) AS changes,
    MIN(change_timestamp) AS first_logged_at,
    MAX(change_timestamp) AS last_logged_at
FROM audit.scd2_change_log
GROUP BY dag_run_id, change_type
ORDER BY first_logged_at, change_type;

\echo '\n=== 5. Product cost history for changed SKUs ==='
WITH changed_skus AS (
    SELECT DISTINCT natural_key AS sku
    FROM audit.scd2_change_log
    WHERE LEFT(natural_key, 2) <> '__'
)
SELECT
    c.sku,
    p.product_name,
    p.category,
    c.cost_key,
    c.unit_cost,
    c.supplier_id,
    c.currency,
    c.uom,
    c.effective_from,
    c.effective_to,
    c.is_current
FROM warehouse.dim_product_cost c
LEFT JOIN warehouse.dim_product p ON p.sku = c.sku
JOIN changed_skus s ON s.sku = c.sku
ORDER BY c.sku, c.effective_from, c.cost_key;

\echo '\n=== 6. Stock reconciliation alerts ==='
SELECT
    r.reconciliation_date,
    p.sku,
    p.product_name,
    p.category,
    wh.store_id AS warehouse_id,
    wh.store_name AS warehouse_name,
    r.derived_qty,
    r.reported_qty,
    r.abs_variance,
    r.pct_variance,
    r.alert_reason
FROM mart.stock_reconciliation r
JOIN warehouse.dim_product p ON p.product_key = r.product_key
JOIN warehouse.dim_store wh ON wh.store_key = r.warehouse_key
WHERE r.alert_flag = TRUE
ORDER BY r.reconciliation_date, r.abs_variance DESC, p.sku;

\echo '\n=== 7. Stock reconciliation daily summary ==='
SELECT
    reconciliation_date,
    COUNT(*) AS scoped_positions,
    COUNT(*) FILTER (WHERE snapshot_source IS NULL) AS missing_snapshots,
    COUNT(*) FILTER (WHERE alert_flag = TRUE) AS alert_positions,
    ROUND(AVG(abs_variance), 2) AS avg_abs_variance,
    MAX(abs_variance) AS max_abs_variance
FROM mart.stock_reconciliation
GROUP BY reconciliation_date
ORDER BY reconciliation_date;

\echo '\n=== 8. Active reconciliation scope by warehouse ==='
SELECT
    wh.store_id AS warehouse_id,
    wh.store_name AS warehouse_name,
    COUNT(*) AS scoped_sku_count,
    MIN(scope.added_at) AS first_added_at,
    MAX(scope.added_at) AS last_added_at
FROM mart.dim_reconciliation_scope scope
JOIN warehouse.dim_store wh ON wh.store_key = scope.warehouse_key
WHERE scope.is_in_scope = TRUE
GROUP BY wh.store_id, wh.store_name
ORDER BY warehouse_id;

\echo '\n=== 9. Unresolved data-quality alerts ==='
SELECT
    detected_at,
    dag_run_id,
    alert_severity,
    alert_type,
    affected_table,
    affected_rows,
    sample_values,
    description
FROM mart.dq_alerts
WHERE resolved_at IS NULL
ORDER BY detected_at DESC, alert_severity DESC;

\echo '\n=== 10. Current cost coverage by product category ==='
SELECT
    p.category,
    COUNT(*) AS products,
    COUNT(c.cost_key) FILTER (WHERE c.is_current = TRUE) AS products_with_current_cost,
    COUNT(*) - COUNT(c.cost_key) FILTER (WHERE c.is_current = TRUE) AS products_missing_current_cost
FROM warehouse.dim_product p
LEFT JOIN warehouse.dim_product_cost c
    ON c.sku = p.sku
   AND c.is_current = TRUE
GROUP BY p.category
ORDER BY products_missing_current_cost DESC, p.category;
