-- =============================================================================
-- Schema verification probes
-- =============================================================================
-- After deploying 01_schema.sql, run this to PROVE the defensive constraints
-- actually fire. Silent constraints that don't work are worse than no constraints
-- at all — discovering they're broken at production-incident time is the worst
-- possible time.
--
-- Each probe expects a specific outcome (SUCCESS or specific ERROR).
-- The script uses BEGIN ... ROLLBACK so nothing persists after verification.
--
-- Run with:
--   docker exec -i retail_postgres psql -U airflow -d retail_warehouse -v ON_ERROR_STOP=0 < sql/02_verify_constraints.sql
--
-- IMPORTANT: -v ON_ERROR_STOP=0 lets the script continue after expected errors.
-- Without that flag, the script halts on the first probe (which is supposed to fail).
-- =============================================================================

\echo '\n=== PROBE 1: Sentinel row exists ==='
-- Should return one row with cost_key = -1
SELECT cost_key, sku, unit_cost, is_current
FROM warehouse.dim_product_cost
WHERE cost_key = -1;

\echo '\n=== PROBE 2: Insert valid current row ==='
-- Expected: SUCCESS (1 row inserted)
BEGIN;
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_A', 100.0000, 'SUP100', '2026-01-01', '9999-12-31', TRUE);
SELECT 'PROBE 2 result:' AS label, sku, is_current FROM warehouse.dim_product_cost
WHERE sku = 'PROBE_SKU_A';
ROLLBACK;

\echo '\n=== PROBE 3: Partial unique index — second current row should FAIL ==='
-- Expected: first INSERT succeeds, second fails with "duplicate key value violates
-- unique constraint idx_dim_product_cost_one_current"
BEGIN;
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_B', 100.0000, 'SUP100', '2026-01-01', '9999-12-31', TRUE);
-- Now try to insert a second current row for the same SKU. This MUST fail.
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_B', 110.0000, 'SUP100', '2026-02-01', '9999-12-31', TRUE);
ROLLBACK;

\echo '\n=== PROBE 4: Partial unique index — historical rows allowed ==='
-- Expected: SUCCESS — multiple non-current rows for the same SKU coexist
BEGIN;
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_C', 100, 'SUP100', '2025-01-01', '2025-06-01', FALSE);
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_C', 110, 'SUP100', '2025-06-01', '2025-12-01', FALSE);
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_C', 120, 'SUP100', '2025-12-01', '9999-12-31', TRUE);
SELECT 'PROBE 4 result:' AS label, COUNT(*) AS rows_for_sku
FROM warehouse.dim_product_cost
WHERE sku = 'PROBE_SKU_C';
-- Should print 3 rows
ROLLBACK;

\echo '\n=== PROBE 5: CHECK constraint — effective_from < effective_to ==='
-- Expected: FAIL with check constraint violation
BEGIN;
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_D', 100, 'SUP100', '2026-01-01', '2026-01-01', FALSE);
ROLLBACK;

\echo '\n=== PROBE 6: CHECK constraint — is_current implies effective_to is sentinel ==='
-- Expected: FAIL — can't have is_current=TRUE with a closed effective_to
BEGIN;
INSERT INTO warehouse.dim_product_cost
    (sku, unit_cost, supplier_id, effective_from, effective_to, is_current)
VALUES ('PROBE_SKU_E', 100, 'SUP100', '2026-01-01', '2026-12-31', TRUE);
ROLLBACK;

\echo '\n=== PROBE 7: Sign convention — sale_out with positive qty must fail ==='
-- We need at least one row in each FK target before we can insert the fact.
BEGIN;
-- Set up FK targets
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month,
                                month_name, day, day_of_week, day_name, is_weekend)
VALUES (20260101, '2026-01-01', 2026, 1, 1, 'January', 1, 4, 'Thursday', FALSE);
INSERT INTO warehouse.dim_product (sku) VALUES ('PROBE_PRODUCT');
INSERT INTO warehouse.dim_store (store_id, store_type, is_warehouse)
VALUES ('PROBE_WH', 'Physical', TRUE);

-- Now the test: sale_out with positive qty must fail the CHECK
INSERT INTO warehouse.fact_inventory_movements
    (movement_id, date_key, product_key, warehouse_key, quantity, movement_type)
SELECT 'PROBE_MOV_1', 20260101, p.product_key, s.store_key, 5, 'sale_out'
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT' AND s.store_id = 'PROBE_WH';
ROLLBACK;

\echo '\n=== PROBE 8: Sign convention — receipt with negative qty must fail ==='
BEGIN;
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month,
                                month_name, day, day_of_week, day_name, is_weekend)
VALUES (20260101, '2026-01-01', 2026, 1, 1, 'January', 1, 4, 'Thursday', FALSE);
INSERT INTO warehouse.dim_product (sku) VALUES ('PROBE_PRODUCT_2');
INSERT INTO warehouse.dim_store (store_id, store_type, is_warehouse)
VALUES ('PROBE_WH_2', 'Physical', TRUE);

INSERT INTO warehouse.fact_inventory_movements
    (movement_id, date_key, product_key, warehouse_key, quantity, movement_type)
SELECT 'PROBE_MOV_2', 20260101, p.product_key, s.store_key, -3, 'receipt'
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_2' AND s.store_id = 'PROBE_WH_2';
ROLLBACK;

\echo '\n=== PROBE 9: Sign convention — adjustment with either sign should succeed ==='
-- Adjustments can go either way; this should succeed
BEGIN;
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month,
                                month_name, day, day_of_week, day_name, is_weekend)
VALUES (20260101, '2026-01-01', 2026, 1, 1, 'January', 1, 4, 'Thursday', FALSE);
INSERT INTO warehouse.dim_product (sku) VALUES ('PROBE_PRODUCT_3');
INSERT INTO warehouse.dim_store (store_id, store_type, is_warehouse)
VALUES ('PROBE_WH_3', 'Physical', TRUE);

INSERT INTO warehouse.fact_inventory_movements
    (movement_id, date_key, product_key, warehouse_key, quantity, movement_type)
SELECT 'PROBE_MOV_3', 20260101, p.product_key, s.store_key, -2, 'adjustment'
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_3' AND s.store_id = 'PROBE_WH_3';

INSERT INTO warehouse.fact_inventory_movements
    (movement_id, date_key, product_key, warehouse_key, quantity, movement_type)
SELECT 'PROBE_MOV_4', 20260101, p.product_key, s.store_key, 5, 'adjustment'
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_3' AND s.store_id = 'PROBE_WH_3';

SELECT 'PROBE 9 result:' AS label, COUNT(*) AS adjustment_rows
FROM warehouse.fact_inventory_movements
WHERE movement_id IN ('PROBE_MOV_3', 'PROBE_MOV_4');
-- Should print 2
ROLLBACK;

\echo '\n=== PROBE 10: fact_sales — quantity = 0 must fail ==='
BEGIN;
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month,
                                month_name, day, day_of_week, day_name, is_weekend)
VALUES (20260101, '2026-01-01', 2026, 1, 1, 'January', 1, 4, 'Thursday', FALSE);
INSERT INTO warehouse.dim_product (sku) VALUES ('PROBE_PRODUCT_4');
INSERT INTO warehouse.dim_store (store_id, store_type, is_warehouse)
VALUES ('PROBE_ST_4', 'Physical', FALSE);

INSERT INTO warehouse.fact_sales
    (transaction_id, date_key, product_key, store_key, cost_key,
     quantity, unit_price, total_amount, is_return)
SELECT 'PROBE_TXN_1', 20260101, p.product_key, s.store_key, -1,
       0, 100, 0, FALSE
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_4' AND s.store_id = 'PROBE_ST_4';
ROLLBACK;

\echo '\n=== PROBE 11: fact_sales — duplicate transaction_id must fail (idempotency) ==='
BEGIN;
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month,
                                month_name, day, day_of_week, day_name, is_weekend)
VALUES (20260101, '2026-01-01', 2026, 1, 1, 'January', 1, 4, 'Thursday', FALSE);
INSERT INTO warehouse.dim_product (sku) VALUES ('PROBE_PRODUCT_5');
INSERT INTO warehouse.dim_store (store_id, store_type, is_warehouse)
VALUES ('PROBE_ST_5', 'Physical', FALSE);

-- First insert succeeds
INSERT INTO warehouse.fact_sales
    (transaction_id, date_key, product_key, store_key, cost_key,
     quantity, unit_price, total_amount, is_return)
SELECT 'PROBE_TXN_DUP', 20260101, p.product_key, s.store_key, -1,
       1, 100, 100, FALSE
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_5' AND s.store_id = 'PROBE_ST_5';

-- Second with same transaction_id must fail
INSERT INTO warehouse.fact_sales
    (transaction_id, date_key, product_key, store_key, cost_key,
     quantity, unit_price, total_amount, is_return)
SELECT 'PROBE_TXN_DUP', 20260101, p.product_key, s.store_key, -1,
       1, 100, 100, FALSE
FROM warehouse.dim_product p, warehouse.dim_store s
WHERE p.sku = 'PROBE_PRODUCT_5' AND s.store_id = 'PROBE_ST_5';
ROLLBACK;

\echo '\n=== Verification complete ==='
\echo 'If you saw ERROR messages on probes 3, 5, 6, 7, 8, 10, 11 (those were SUPPOSED to fail),'
\echo 'and probes 1, 2, 4, 9 succeeded with the expected output, the schema is correctly defended.'
