"""
Type 1 dimension builders.

Type 1 = overwrite history. When an attribute changes, the dimension row is
updated in place and the previous value is lost. Suitable for attributes
where history doesn't matter (display name corrections, category renames).

Contrast with Type 2 (in scd2.py): tracked attributes get new history rows
on change, preserving the full timeline.

These builders are pure pandas — they take cleaned DataFrames and return
DataFrames ready for upsert. The DAG's task wrappers do the actual upsert.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def build_dim_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Generate the date dimension table.

    Each row covers one day from start_date to end_date inclusive.
    Pre-computes attributes that would otherwise require date functions
    in every reporting query.

    Args:
        start_date: ISO format string, e.g. "2026-01-01"
        end_date:   ISO format string, e.g. "2026-12-31"

    Returns:
        DataFrame with one row per day, columns matching dim_date schema.
    """
    log.info(f"Building dim_date from {start_date} to {end_date}")
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": dates})

    # Surrogate key as YYYYMMDD integer
    df["date_key"]    = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]        = df["full_date"].dt.year
    df["quarter"]     = df["full_date"].dt.quarter
    df["month"]       = df["full_date"].dt.month
    df["month_name"]  = df["full_date"].dt.strftime("%B")
    df["day"]         = df["full_date"].dt.day
    df["day_of_week"] = df["full_date"].dt.dayofweek + 1   # 1=Monday, 7=Sunday
    df["day_name"]    = df["full_date"].dt.strftime("%A")
    df["is_weekend"]  = df["day_of_week"].isin([6, 7])
    df["is_nz_holiday"] = _flag_nz_holidays(df["full_date"])

    log.info(f"  Output: {len(df):,} date rows")
    return df[[
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "day", "day_of_week", "day_name", "is_weekend", "is_nz_holiday",
    ]]


def _flag_nz_holidays(dates: pd.Series) -> pd.Series:
    """
    Flag NZ public holidays. Lightweight implementation — covers the
    fixed-date major holidays. Easter and Queen's Birthday vary; in
    production we'd use a proper holiday library or load from a table.

    For the project this is enough to demonstrate the column exists and
    produces sensible non-trivial values.
    """
    flags = pd.Series(False, index=dates.index)
    for d in dates:
        # Fixed-date NZ public holidays
        if (d.month, d.day) in {
            (1, 1),   # New Year's Day
            (1, 2),   # Day after New Year's
            (2, 6),   # Waitangi Day
            (4, 25),  # ANZAC Day
            (12, 25), # Christmas Day
            (12, 26), # Boxing Day
        }:
            flags.loc[d == dates] = True
    return flags


def build_dim_product_from_transactions(
    transactions_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the product dimension from the products catalog (products.json).

    The transactions DataFrame is included as a parameter for traceability —
    in production we might use it to flag inactive SKUs (no sales for 90+ days)
    but for this version we just take what the catalog says.

    Args:
        transactions_df: cleaned transactions (used for activity flagging if needed)
        products_df:     loaded from products.json

    Returns:
        DataFrame with columns matching dim_product schema (no product_key —
        SERIAL is assigned at insert time).
    """
    log.info(f"Building dim_product from {len(products_df):,} catalog entries")
    df = products_df.copy()

    df["sku"] = df["sku"].astype(str).str.strip().str.upper()

    # Defensive: deduplicate
    df = df.drop_duplicates(subset=["sku"], keep="first")

    # Match warehouse.dim_product schema. Catalog provides 'active' as boolean;
    # warehouse expects 'is_active'.
    out = pd.DataFrame({
        "sku":           df["sku"],
        "product_name":  df.get("product_name", df["sku"]),
        "brand":         df.get("brand", "Unknown"),
        "category":      df.get("category", "Uncategorised"),
        "list_price":    df.get("list_price", 0).round(2),
        "is_active":     df.get("active", True),
    })

    log.info(f"  Output: {len(out):,} product rows")
    return out


def build_dim_store_from_master(stores_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the store dimension from the stores master CSV.

    The stores master includes both selling stores and warehouses (distinguished
    by is_warehouse). Both go in the same dim — see Decision: stores and
    warehouses share dim_store, with is_warehouse flag.

    Args:
        stores_df: loaded from stores.csv

    Returns:
        DataFrame matching dim_store schema (no store_key).
    """
    log.info(f"Building dim_store from {len(stores_df):,} master rows")
    df = stores_df.copy()

    # Booleans may have been read as strings — coerce
    if df["is_warehouse"].dtype == object:
        df["is_warehouse"] = df["is_warehouse"].astype(str).str.lower().isin(
            ["true", "1", "yes"]
        )

    out = pd.DataFrame({
        "store_id":          df["store_id"],
        "store_name":        df["store_name"],
        "region":            df["region"],
        "store_type":        df["store_type"],
        "is_warehouse":      df["is_warehouse"],
        "home_warehouse_id": df.get("home_warehouse_id"),
        "open_date":         pd.to_datetime(df["open_date"]).dt.date,
    })

    log.info(f"  Output: {len(out):,} store rows")
    return out
