"""
Transform smoke test for the retail ETL pipeline.

Verifies:
  1. The transforms package imports cleanly
  2. Cleaning and Type 1 dimension builders work on generated data
  3. SCD2 change detection classifies the five expected scenarios
  4. Stock reconciliation pure logic applies the hybrid alert threshold

Run from the project root:
    python3 scripts/test_transforms.py

This script intentionally avoids database-dependent functions such as
compute_running_balance() and build_reconciliation_scope(); those are covered
by the Airflow/Docker flow and SQL probes.
"""

import json
import sys
from pathlib import Path

import pandas as pd

# Make sure we can import from dags/
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "dags"))

from transforms import (
    clean_transactions,
    clean_inv_movements,
    clean_cost_master,
    clean_inventory_snapshot,
    build_dim_date,
    build_dim_product_from_transactions,
    build_dim_store_from_master,
    SCD2_DIMENSION_CONFIG,
    detect_changes,
    compute_running_balance,
    reconcile_stock,
    build_reconciliation_scope,
    build_daily_margin_summary,
)

print("[1/7] Imports succeeded")

# ---------------------------------------------------------------------------
# Test cleaning functions on real generated data
# ---------------------------------------------------------------------------
RAW_DIR = ROOT / "data" / "raw"
if not (RAW_DIR / "transactions.csv").exists():
    print(f"\nNo data in {RAW_DIR}. Run: bash scripts/switch_to_day.sh 1")
    sys.exit(1)

raw_txns = pd.read_csv(RAW_DIR / "transactions.csv")
clean_txns = clean_transactions(raw_txns)

assert len(clean_txns) > 0, "clean_transactions returned empty"
assert "is_return" in clean_txns.columns, "missing is_return"
assert clean_txns["transaction_date"].dtype.name.startswith("datetime"), (
    "transaction_date should be datetime"
)
assert clean_txns["sku"].str.match(r"^[A-Z0-9]+$").all(), (
    "SKU should be normalised"
)
print(f"[2/7] clean_transactions: {len(raw_txns):,} to {len(clean_txns):,} rows")

raw_movs = pd.read_csv(RAW_DIR / "inv_movements.csv")
clean_movs = clean_inv_movements(raw_movs)
assert len(clean_movs) > 0, "clean_inv_movements returned empty"
print(f"[3/7] clean_inv_movements: {len(raw_movs):,} to {len(clean_movs):,} rows")

raw_costs = pd.read_csv(RAW_DIR / "cost_master.csv")
clean_costs = clean_cost_master(raw_costs)
assert (clean_costs["unit_cost"] > 0).all(), "all costs should be positive"
print(f"[4/7] clean_cost_master: {len(raw_costs):,} to {len(clean_costs):,} rows")

raw_snapshots = pd.read_csv(RAW_DIR / "inventory_snapshot.csv")
clean_snapshots = clean_inventory_snapshot(raw_snapshots)
assert len(clean_snapshots) > 0, "clean_inventory_snapshot returned empty"

# ---------------------------------------------------------------------------
# Test dimension builders
# ---------------------------------------------------------------------------
dim_date = build_dim_date("2026-04-01", "2026-04-30")
assert len(dim_date) == 30, f"expected 30 dates, got {len(dim_date)}"
assert dim_date["date_key"].dtype == int, "date_key should be int"

with open(RAW_DIR / "products.json") as f:
    products_df = pd.DataFrame(json.load(f))

dim_product = build_dim_product_from_transactions(clean_txns, products_df)
assert len(dim_product) == len(products_df), "should produce one row per product"

stores_df = pd.read_csv(RAW_DIR / "stores.csv")
dim_store = build_dim_store_from_master(stores_df)
assert len(dim_store) == len(stores_df), "should produce one row per store"
assert dim_store["is_warehouse"].sum() == 4, "should have 4 warehouses"
print(
    f"[5/7] dimensions: {len(dim_date):,} dates, "
    f"{len(dim_product):,} products, {len(dim_store):,} stores"
)

# ---------------------------------------------------------------------------
# Verify SCD2 change detection as a pure function
# ---------------------------------------------------------------------------
config = SCD2_DIMENSION_CONFIG["dim_product_cost"]
current_dim = pd.DataFrame([
    {
        "cost_key": 1,
        "sku": "SKU_SAME",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
    {
        "cost_key": 2,
        "sku": "SKU_COST_CHANGE",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
    {
        "cost_key": 3,
        "sku": "SKU_UOM_CHANGE",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
    {
        "cost_key": 4,
        "sku": "SKU_DISCONTINUED",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
])
source_costs = pd.DataFrame([
    {
        "sku": "SKU_SAME",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
    {
        "sku": "SKU_COST_CHANGE",
        "unit_cost": 110.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "each",
    },
    {
        "sku": "SKU_UOM_CHANGE",
        "unit_cost": 100.0,
        "supplier_id": "SUP001",
        "currency": "NZD",
        "uom": "carton",
    },
    {
        "sku": "SKU_NEW",
        "unit_cost": 75.0,
        "supplier_id": "SUP002",
        "currency": "NZD",
        "uom": "each",
    },
])

changes = detect_changes(source_costs, current_dim, config)
assert len(changes["new"]) == 1, "expected one new SKU"
assert changes["unchanged"] == ["SKU_SAME"], "expected one unchanged SKU"
assert len(changes["type_2_changes"]) == 1, "expected one Type 2 change"
assert len(changes["type_1_updates"]) == 1, "expected one Type 1 update"
assert len(changes["discontinued"]) == 1, "expected one discontinued SKU"
print("[6/7] SCD2 detect_changes classified all five scenarios")

# ---------------------------------------------------------------------------
# Verify pure reconciliation threshold logic
# ---------------------------------------------------------------------------
derived = pd.DataFrame([
    {
        "reconciliation_date": "2026-04-01",
        "product_key": 1,
        "warehouse_key": 10,
        "derived_qty": 100,
    },
    {
        "reconciliation_date": "2026-04-01",
        "product_key": 2,
        "warehouse_key": 10,
        "derived_qty": 100,
    },
    {
        "reconciliation_date": "2026-04-01",
        "product_key": 3,
        "warehouse_key": 10,
        "derived_qty": 50,
    },
])
snapshots = pd.DataFrame([
    {
        "reconciliation_date": "2026-04-01",
        "product_key": 1,
        "warehouse_key": 10,
        "reported_qty": 100,
        "snapshot_source": "ERP",
    },
    {
        "reconciliation_date": "2026-04-01",
        "product_key": 2,
        "warehouse_key": 10,
        "reported_qty": 80,
        "snapshot_source": "ERP",
    },
])

reconciled = reconcile_stock(derived, snapshots)
reason_by_product = dict(zip(reconciled["product_key"], reconciled["alert_reason"]))
assert reason_by_product[1] == "reconciled"
assert reason_by_product[2] == "variance_above_threshold"
assert reason_by_product[3] == "no_erp_snapshot"
print("[7/7] reconcile_stock applied threshold and missing-snapshot logic")

assert callable(compute_running_balance)
assert callable(build_reconciliation_scope)
assert callable(build_daily_margin_summary)

print("\nTransform smoke test PASSED")
