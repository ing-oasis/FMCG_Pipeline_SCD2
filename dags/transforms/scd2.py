"""
SCD Type 2 merge logic for warehouse.dim_product_cost.

This module is the architectural centrepiece of the pipeline. The merge:
  - Detects which cost rows have changed since the last load
  - Closes out historical rows (sets effective_to + is_current=FALSE)
  - Opens new current rows (effective_to='9999-12-31', is_current=TRUE)
  - Logs every change to audit.scd2_change_log within the same transaction
  - Handles five distinct scenarios: insert_initial, type2_change,
    type1_update, discontinued, unchanged

Configuration is the lever that makes adding tracked attributes a one-line
change rather than a code change. See SCD2_DIMENSION_CONFIG.

This is the production path used by the DAG.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import run_query, write_scd2_audit

log = logging.getLogger(__name__)


# =============================================================================
# Configuration — declares which attributes are tracked Type 2 vs Type 1
# =============================================================================
# Decision 1A: config-driven SCD2 attribute tracking.
#
# Adding a new tracked attribute is one line below — no code changes to
# detect_changes() or scd2_merge() required.

SCD2_DIMENSION_CONFIG = {
    "dim_product_cost": {
        "table_schema":      "warehouse",
        "table_name":        "dim_product_cost",
        "natural_key":       "sku",
        "type_2_attributes": ["unit_cost", "supplier_id"],
        "type_1_attributes": ["currency", "uom"],
        "ignored_attributes": ["extracted_at", "loaded_at"],
        "audit_table":       "audit.scd2_change_log",
        "initial_effective_from": "2000-01-01",
    },
}


# =============================================================================
# Helper — value comparison that handles type coercion safely
# =============================================================================

def _values_equal(a, b) -> bool:
    """
    Compare two values for SCD2 equality.

    Subtleties handled:
      - NaN/None on either side: treated equal only if BOTH are missing
      - Numeric comparison: cast both sides to float for unit_cost (Decimal
        from the DB vs float from pandas), then compare with epsilon for
        floating-point safety
      - String comparison: stripped + case-preserving
    """
    # Both missing → equal
    a_missing = a is None or (isinstance(a, float) and pd.isna(a))
    b_missing = b is None or (isinstance(b, float) and pd.isna(b))
    if a_missing and b_missing:
        return True
    if a_missing or b_missing:
        return False

    # Numeric — Decimal vs float coexistence is the main hazard
    if isinstance(a, (int, float, Decimal)) or isinstance(b, (int, float, Decimal)):
        return abs(float(a) - float(b)) < 1e-6

    # Strings — defensive strip
    if isinstance(a, str) or isinstance(b, str):
        return str(a).strip() == str(b).strip()

    return a == b


# =============================================================================
# Pure function — change detection (no DB, just compares two DataFrames)
# =============================================================================

def detect_changes(
    source_df: pd.DataFrame,
    current_dim_df: pd.DataFrame,
    config: dict,
) -> dict:
    """
    Pure function: classify each SKU into one of five scenarios by comparing
    today's source snapshot against the current state of the dimension.

    Args:
        source_df:      cleaned cost master (one row per SKU, current snapshot)
        current_dim_df: current rows from dim_product_cost (is_current=TRUE,
                        excluding the sentinel row)
        config:         entry from SCD2_DIMENSION_CONFIG

    Returns:
        dict with five keys:
          'new'            : list of source rows (dicts) — insert_initial path
          'unchanged'      : list of natural_key values
          'type_2_changes' : list of (current_row_dict, source_row_dict) pairs
          'type_1_updates' : list of (current_row_dict, source_row_dict) pairs
          'discontinued'   : list of current_row_dicts (no replacement source)
    """
    natural_key = config["natural_key"]
    type_2_attrs = config["type_2_attributes"]
    type_1_attrs = config["type_1_attributes"]

    # Index both sides by natural_key for O(1) lookup
    source_by_key = {
        row[natural_key]: row.to_dict()
        for _, row in source_df.iterrows()
    }
    current_by_key = {
        row[natural_key]: row.to_dict()
        for _, row in current_dim_df.iterrows()
    }

    new_skus = set(source_by_key) - set(current_by_key)
    discontinued_skus = set(current_by_key) - set(source_by_key)
    common_skus = set(source_by_key) & set(current_by_key)

    new           = [source_by_key[k] for k in sorted(new_skus)]
    discontinued  = [current_by_key[k] for k in sorted(discontinued_skus)]
    unchanged     = []
    type_2_changes = []
    type_1_updates = []

    for key in sorted(common_skus):
        source_row = source_by_key[key]
        current_row = current_by_key[key]

        type_2_diff = any(
            not _values_equal(source_row.get(attr), current_row.get(attr))
            for attr in type_2_attrs
        )
        type_1_diff = any(
            not _values_equal(source_row.get(attr), current_row.get(attr))
            for attr in type_1_attrs
        )

        if type_2_diff:
            # Type 2 dominates: even if Type 1 also changed, the close-and-open
            # operation will write the new Type 1 values into the new row
            type_2_changes.append((current_row, source_row))
        elif type_1_diff:
            type_1_updates.append((current_row, source_row))
        else:
            unchanged.append(key)

    log.info(
        f"SCD2 change detection: "
        f"new={len(new)}, unchanged={len(unchanged)}, "
        f"type_2={len(type_2_changes)}, type_1={len(type_1_updates)}, "
        f"discontinued={len(discontinued)}"
    )

    return {
        "new":            new,
        "unchanged":      unchanged,
        "type_2_changes": type_2_changes,
        "type_1_updates": type_1_updates,
        "discontinued":   discontinued,
    }


# =============================================================================
# Database function — the merge itself (transactional)
# =============================================================================

def scd2_merge(
    engine: Engine,
    dimension_name: str,
    source_df: pd.DataFrame,
    today: Optional[date] = None,
    dag_run_id: str = "manual",
) -> dict:
    """
    Apply SCD Type 2 merge to a dimension table.

    Reads the current dimension state, classifies SKUs (via detect_changes),
    then applies all changes (insert/close/update) atomically inside a single
    transaction. Audit log inserts go in the same transaction — either all
    changes commit or none do.

    Args:
        engine:         SQLAlchemy engine for the warehouse database
        dimension_name: key into SCD2_DIMENSION_CONFIG (e.g., 'dim_product_cost')
        source_df:      cleaned source snapshot (e.g., from clean_cost_master)
        today:          effective_from date for new rows. Defaults to the
                        source snapshot's extracted_at date, then actual today.
        dag_run_id:     for audit log linkage

    Returns:
        dict of counts: {
            'inserted_new':    int,
            'closed':          int,
            'opened':          int,
            'updated_t1':      int,
            'discontinued':    int,
            'unchanged':       int,
            'audit_logged':    int,
        }
    """
    if today is None:
        today = _infer_effective_date(source_df)
    elif isinstance(today, datetime):
        today = today.date()
    elif not isinstance(today, date):
        today = pd.to_datetime(today).date()

    config = SCD2_DIMENSION_CONFIG[dimension_name]
    schema = config["table_schema"]
    table = config["table_name"]
    full_table = f"{schema}.{table}"
    natural_key = config["natural_key"]
    type_2_attrs = config["type_2_attributes"]
    type_1_attrs = config["type_1_attributes"]

    log.info(f"SCD2 merge starting against {full_table}, "
             f"source_rows={len(source_df):,}, today={today}")

    # ----------------------------------------------------------------------
    # Step 1: Read current rows (excluding the sentinel)
    # ----------------------------------------------------------------------
    current_query = text(f"""
        SELECT cost_key, sku, unit_cost, supplier_id, currency, uom,
               effective_from, effective_to, is_current
        FROM {full_table}
        WHERE is_current = TRUE
          AND cost_key != -1
    """)
    current_dim_df = run_query(engine, current_query)
    log.info(f"  Current dim state: {len(current_dim_df):,} active rows")

    # ----------------------------------------------------------------------
    # Step 2: Classify changes (pure function)
    # ----------------------------------------------------------------------
    classification = detect_changes(source_df, current_dim_df, config)

    # ----------------------------------------------------------------------
    # Step 3: Apply changes inside a single transaction
    # ----------------------------------------------------------------------
    counts = {
        "inserted_new":  0,
        "closed":        0,
        "opened":        0,
        "updated_t1":    0,
        "discontinued":  0,
        "unchanged":     len(classification["unchanged"]),
        "audit_logged":  0,
    }

    is_first_load = len(current_dim_df) == 0
    initial_effective_from = config["initial_effective_from"] if is_first_load else today.isoformat()

    with engine.begin() as conn:

        # --- Scenario A: insert_initial -----------------------------------
        # New SKUs: insert one current row each
        for source_row in classification["new"]:
            new_cost_key = _insert_current_row(
                conn, full_table, source_row,
                effective_from=initial_effective_from if is_first_load else today.isoformat(),
                config=config,
            )
            counts["inserted_new"] += 1

            write_scd2_audit(
                conn,
                dag_run_id=dag_run_id,
                dimension_table=table,
                natural_key=str(source_row[natural_key]),
                change_type="insert_initial",
                new_cost_key=new_cost_key,
                new_attributes=_audit_attrs(source_row, type_2_attrs + type_1_attrs),
                notes=("first-load bulk insert" if is_first_load else "new SKU"),
            )
            counts["audit_logged"] += 1

        # --- Scenario B: type_2_change ------------------------------------
        # Close the current row, then insert the new current row
        for current_row, source_row in classification["type_2_changes"]:
            old_cost_key = int(current_row["cost_key"])

            # Close: set effective_to to today, is_current=FALSE
            conn.execute(text(f"""
                UPDATE {full_table}
                SET effective_to = :today, is_current = FALSE
                WHERE cost_key = :cost_key
            """), {"today": today.isoformat(), "cost_key": old_cost_key})
            counts["closed"] += 1

            # Insert new current row (with today as effective_from — Decision 4A)
            new_cost_key = _insert_current_row(
                conn, full_table, source_row,
                effective_from=today.isoformat(),
                config=config,
            )
            counts["opened"] += 1

            write_scd2_audit(
                conn,
                dag_run_id=dag_run_id,
                dimension_table=table,
                natural_key=str(source_row[natural_key]),
                change_type="type2_change",
                closed_cost_key=old_cost_key,
                new_cost_key=new_cost_key,
                old_attributes=_audit_attrs(current_row, type_2_attrs + type_1_attrs),
                new_attributes=_audit_attrs(source_row, type_2_attrs + type_1_attrs),
                notes=_describe_diff(current_row, source_row, type_2_attrs),
            )
            counts["audit_logged"] += 1

        # --- Scenario C: type_1_update -----------------------------------
        # Update the current row in place — Type 1 attrs only
        for current_row, source_row in classification["type_1_updates"]:
            cost_key = int(current_row["cost_key"])
            update_cols = ", ".join(f"{a} = :{a}" for a in type_1_attrs)
            params = {a: source_row.get(a) for a in type_1_attrs}
            params["cost_key"] = cost_key

            conn.execute(
                text(f"UPDATE {full_table} SET {update_cols} WHERE cost_key = :cost_key"),
                params,
            )
            counts["updated_t1"] += 1

            write_scd2_audit(
                conn,
                dag_run_id=dag_run_id,
                dimension_table=table,
                natural_key=str(source_row[natural_key]),
                change_type="type1_update",
                closed_cost_key=cost_key,           # same key — updated in place
                new_cost_key=cost_key,
                old_attributes=_audit_attrs(current_row, type_1_attrs),
                new_attributes=_audit_attrs(source_row, type_1_attrs),
                notes=_describe_diff(current_row, source_row, type_1_attrs),
            )
            counts["audit_logged"] += 1

        # --- Scenario D: discontinued -------------------------------------
        # Close the current row, no replacement
        for current_row in classification["discontinued"]:
            cost_key = int(current_row["cost_key"])
            conn.execute(text(f"""
                UPDATE {full_table}
                SET effective_to = :today, is_current = FALSE
                WHERE cost_key = :cost_key
            """), {"today": today.isoformat(), "cost_key": cost_key})
            counts["discontinued"] += 1

            write_scd2_audit(
                conn,
                dag_run_id=dag_run_id,
                dimension_table=table,
                natural_key=str(current_row[natural_key]),
                change_type="discontinued",
                closed_cost_key=cost_key,
                old_attributes=_audit_attrs(current_row, type_2_attrs + type_1_attrs),
                notes="SKU disappeared from source",
            )
            counts["audit_logged"] += 1

    log.info(f"SCD2 merge committed. Counts: {counts}")
    return counts


# =============================================================================
# Helpers (private to this module)
# =============================================================================

def _insert_current_row(conn, full_table, source_row, effective_from, config) -> int:
    """
    Insert one current row. Returns the new cost_key.

    The partial unique index on (sku) WHERE is_current=TRUE will reject
    this insert if there's already a current row for the same SKU. In a
    type_2_change, the close-row UPDATE must run before this INSERT.
    """
    sql = text(f"""
        INSERT INTO {full_table}
            (sku, unit_cost, supplier_id, currency, uom,
             effective_from, effective_to, is_current)
        VALUES
            (:sku, :unit_cost, :supplier_id, :currency, :uom,
             :effective_from, '9999-12-31', TRUE)
        RETURNING cost_key
    """)
    result = conn.execute(sql, {
        "sku":             source_row[config["natural_key"]],
        "unit_cost":       float(source_row.get("unit_cost", 0)),
        "supplier_id":     source_row.get("supplier_id"),
        "currency":        source_row.get("currency", "NZD"),
        "uom":             source_row.get("uom", "each"),
        "effective_from":  effective_from,
    })
    return int(result.scalar_one())


def _infer_effective_date(source_df: pd.DataFrame) -> date:
    """
    Infer the SCD2 effective date from the source snapshot.

    The simulator's cost master includes extracted_at. Using that date keeps
    the Type 2 timeline aligned with the business event date instead of the
    machine date when the DAG happened to run.
    """
    if "extracted_at" in source_df.columns:
        extracted_at = pd.to_datetime(source_df["extracted_at"], errors="coerce")
        if extracted_at.notna().any():
            return extracted_at.max().date()

    return date.today()


def _audit_attrs(row: dict, attrs: list) -> dict:
    """Extract a subset of attributes from a row, with type-safe values for JSONB."""
    out = {}
    for a in attrs:
        v = row.get(a)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out[a] = None
        elif isinstance(v, Decimal):
            out[a] = float(v)
        elif isinstance(v, (datetime, date)):
            out[a] = v.isoformat()
        else:
            out[a] = v
    return out


def _describe_diff(old: dict, new: dict, attrs: list) -> str:
    """Human-readable summary of which attributes changed and how."""
    diffs = []
    for a in attrs:
        if not _values_equal(old.get(a), new.get(a)):
            diffs.append(f"{a}: {old.get(a)!r} → {new.get(a)!r}")
    return "; ".join(diffs) if diffs else "no diff (unexpected)"
