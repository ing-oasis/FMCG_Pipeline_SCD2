"""
Staging-layer cleaning functions.

Each function takes a raw DataFrame (whatever was loaded from the source CSV)
and returns a cleaned DataFrame ready for warehouse load. Cleaning includes:
  - Date format normalisation (mixed YYYY-MM-DD and DD/MM/YYYY in source)
  - SKU normalisation (strip whitespace, uppercase)
  - Deduplication by natural key
  - Quarantine of rows missing critical fields

These functions are pure — they don't touch the database. The DAG's task
wrappers handle the database round-trip.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def _parse_mixed_dates(series: pd.Series) -> pd.Series:
    """
    Source data has dates in two formats: ISO (YYYY-MM-DD) and NZ-style (DD/MM/YYYY).
    Try ISO first; for rows that fail, try NZ-style. Anything that fails both
    becomes NaT (caught by downstream null-checks).
    """
    iso = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    # Where ISO parsing failed, retry with NZ format
    nz_format_mask = iso.isna() & series.notna()
    if nz_format_mask.any():
        iso.loc[nz_format_mask] = pd.to_datetime(
            series[nz_format_mask], format="%d/%m/%Y", errors="coerce"
        )
    return iso


def _normalise_sku(series: pd.Series) -> pd.Series:
    """Strip whitespace and upper-case. Source has dirty patterns from
    OCR-style errors and mixed-case data entry."""
    return series.astype(str).str.strip().str.upper()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a raw transactions DataFrame.

    Steps:
      1. Parse mixed date formats
      2. Normalise SKU (strip + upper)
      3. Drop duplicate transaction_ids (keep first)
      4. Drop rows with NULL transaction_id, sku, or store_id (critical fields)
      5. Recompute total_amount = quantity * unit_price (defensive — source can be wrong)
      6. Add is_return flag (quantity < 0)

    Returns a clean DataFrame with the same column set plus 'is_return'.
    """
    log.info(f"Cleaning {len(df):,} raw transactions")
    df = df.copy()

    # 1. Dates
    df["transaction_date"] = _parse_mixed_dates(df["transaction_date"])

    # 2. SKU
    df["sku"] = _normalise_sku(df["sku"])

    # 3. Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    log.info(f"  Dropped {before - len(df):,} duplicate transactions")

    # 4. Drop rows missing critical fields
    before = len(df)
    df = df.dropna(subset=["transaction_id", "sku", "store_id", "transaction_date"])
    log.info(f"  Dropped {before - len(df):,} rows with NULL critical fields")

    # 5. Recompute total_amount (source can have rounding errors)
    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)

    # 6. Derived flag
    df["is_return"] = df["quantity"] < 0

    log.info(f"  Output: {len(df):,} clean transactions")
    return df


def clean_inv_movements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean inventory movements.

    Steps:
      1. Parse mixed date formats
      2. Normalise SKU
      3. Drop duplicate movement_ids
      4. Drop rows with NULL critical fields
      5. Validate sign convention (quantities must match movement_type)
         — rows with bad signs are LOGGED but not dropped here; the
         schema's CHECK constraint will reject them at insert time
         (loud failure preferred over silent drop)
    """
    log.info(f"Cleaning {len(df):,} raw inventory movements")
    df = df.copy()

    df["movement_date"] = _parse_mixed_dates(df["movement_date"])
    df["sku"]           = _normalise_sku(df["sku"])

    before = len(df)
    df = df.drop_duplicates(subset=["movement_id"], keep="first")
    log.info(f"  Dropped {before - len(df):,} duplicate movements")

    before = len(df)
    df = df.dropna(subset=["movement_id", "sku", "warehouse_id",
                            "movement_type", "quantity", "movement_date"])
    log.info(f"  Dropped {before - len(df):,} rows with NULL critical fields")

    # Validate sign convention but don't drop — let the DB constraint fail loudly
    bad_signs = df[
        ((df["movement_type"] == "sale_out")    & (df["quantity"] > 0)) |
        ((df["movement_type"] == "transfer_out") & (df["quantity"] > 0)) |
        ((df["movement_type"] == "receipt")      & (df["quantity"] < 0)) |
        ((df["movement_type"] == "transfer_in")  & (df["quantity"] < 0))
    ]
    if len(bad_signs) > 0:
        log.warning(
            f"  ⚠ {len(bad_signs)} rows violate sign convention. "
            f"Database CHECK constraint will reject these at insert time."
        )

    log.info(f"  Output: {len(df):,} clean movements")
    return df


def clean_cost_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean cost master snapshot.

    Steps:
      1. Normalise SKU
      2. Drop duplicate SKUs (keep first — log if duplicates found)
      3. Drop rows with NULL critical fields
      4. Validate cost > 0 (quarantine zero/negative costs as DQ alerts)
    """
    log.info(f"Cleaning {len(df):,} raw cost master rows")
    df = df.copy()

    df["sku"] = _normalise_sku(df["sku"])

    before = len(df)
    df = df.drop_duplicates(subset=["sku"], keep="first")
    if len(df) < before:
        log.warning(f"  ⚠ Dropped {before - len(df):,} duplicate SKUs in cost master")

    before = len(df)
    df = df.dropna(subset=["sku", "supplier_id", "unit_cost"])
    log.info(f"  Dropped {before - len(df):,} rows with NULL critical fields")

    # Validate cost values
    bad_costs = df[df["unit_cost"] <= 0]
    if len(bad_costs) > 0:
        log.warning(
            f"  ⚠ {len(bad_costs)} rows with non-positive unit_cost. "
            f"These will be excluded from cost master load."
        )
        df = df[df["unit_cost"] > 0]

    log.info(f"  Output: {len(df):,} clean cost rows")
    return df


def clean_inventory_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean ERP inventory snapshot.

    Steps:
      1. Parse snapshot_date
      2. Normalise SKU
      3. Drop NULLs on critical fields
      4. Allow reported_qty to be 0 or negative (legitimate edge cases —
         negative ERP-reported stock indicates an over-allocation that
         the reconciliation should flag, not silently drop)
    """
    log.info(f"Cleaning {len(df):,} raw snapshot rows")
    df = df.copy()

    df["snapshot_date"] = _parse_mixed_dates(df["snapshot_date"])
    df["sku"]           = _normalise_sku(df["sku"])

    before = len(df)
    df = df.dropna(subset=["snapshot_date", "sku", "warehouse_id", "reported_qty"])
    log.info(f"  Dropped {before - len(df):,} rows with NULL critical fields")

    log.info(f"  Output: {len(df):,} clean snapshot rows")
    return df
