"""
Transformation logic for the PB Tech pipeline.

Organised into modules by concern:
  - cleaning   : staging-layer cleaning (dirty-data handling)
  - dimensions : Type 1 dimension builders (product, store, date)
  - scd2       : the SCD Type 2 merge for dim_product_cost
  - inventory  : running-balance computation + reconciliation
  - marts      : aggregate mart builders (margin summary, etc.)
  - db         : shared database helpers (connections, audit log writes)

The DAG imports from this package, not from individual files. The submodule
boundary is for our reading; consumers see a flat namespace.

Two function categories:
  PURE FUNCTIONS take and return pandas DataFrames. No database access.
  DATABASE FUNCTIONS take an SQLAlchemy engine and read/write the warehouse.

The DAG's task wrappers handle the glue (read source -> call pure transform
-> write result via database function).
"""

# Cleaning
from .cleaning import (
    clean_transactions,
    clean_inv_movements,
    clean_cost_master,
    clean_inventory_snapshot,
)

# Dimension builders (Type 1)
from .dimensions import (
    build_dim_date,
    build_dim_product_from_transactions,
    build_dim_store_from_master,
)

# SCD Type 2
from .scd2 import (
    SCD2_DIMENSION_CONFIG,
    detect_changes,
    scd2_merge,
)

# Inventory & reconciliation
from .inventory import (
    compute_running_balance,
    reconcile_stock,
    build_reconciliation_scope,
)

# Marts
from .marts import (
    build_daily_margin_summary,
)

# Database helpers
from .db import (
    get_engine,
    run_query,
    write_etl_log,
    write_dq_alert,
)

__all__ = [
    # cleaning
    "clean_transactions", "clean_inv_movements",
    "clean_cost_master", "clean_inventory_snapshot",
    # dimensions
    "build_dim_date", "build_dim_product_from_transactions",
    "build_dim_store_from_master",
    # scd2
    "SCD2_DIMENSION_CONFIG", "detect_changes", "scd2_merge",
    # inventory
    "compute_running_balance", "reconcile_stock", "build_reconciliation_scope",
    # marts
    "build_daily_margin_summary",
    # db
    "get_engine", "run_query", "write_etl_log", "write_dq_alert",
]
