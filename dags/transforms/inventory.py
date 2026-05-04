"""
Inventory reconciliation logic.

Two algorithmic pieces:
  1. compute_running_balance — produces dense end-of-day stock per
     (sku, warehouse, date) using a window function on
     fact_inventory_movements + a lateral-join forward-fill
  2. reconcile_stock — joins derived stock against ERP-reported stock,
     computes variance, applies the hybrid threshold (abs > 5 AND pct > 3%),
     produces alerts

Plus a helper:
  3. build_reconciliation_scope — populates mart.dim_reconciliation_scope
     with (active SKUs × warehouses) pairs that have had recent activity

The database-heavy balance computation is paired with a pure pandas
reconciliation function so threshold behavior can be tested without Postgres.
"""

import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import run_query

log = logging.getLogger(__name__)


# Default thresholds for reconciliation alerts (Decision 10C — hybrid)
DEFAULT_ABS_THRESHOLD = 5      # units
DEFAULT_PCT_THRESHOLD = 0.03   # 3%

RECONCILIATION_COLUMNS = [
    "reconciliation_date",
    "product_key",
    "warehouse_key",
    "derived_qty",
    "reported_qty",
    "snapshot_source",
    "abs_variance",
    "pct_variance",
    "alert_flag",
    "alert_reason",
]


def _as_date(value) -> date:
    """Coerce strings, pandas timestamps, and datetimes to a plain date."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.to_datetime(value).date()


def compute_running_balance(
    engine: Engine,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Produce dense end-of-day stock per (sku, warehouse, date).

    Algorithm (executed as a single SQL query for performance):
      1. Build dense scaffold: cross-join (active SKUs × warehouses × dates)
      2. Compute cumulative balance per (sku, warehouse) by date:
         SUM(quantity) OVER (PARTITION BY product_key, warehouse_key
                             ORDER BY date_key
                             ROWS UNBOUNDED PRECEDING)
      3. Forward-fill the balance into dates with no movement using a
         LATERAL JOIN that finds the most recent balance at-or-before
         each scaffold date

    Args:
        engine:     SQLAlchemy engine
        start_date: first date in the analysis window
        end_date:   last date (inclusive)

    Returns:
        DataFrame with columns: reconciliation_date, product_key,
        warehouse_key, derived_qty.

    Implementation note:
        The cumulative balance is calculated for all movement history up to
        end_date, then forward-filled onto the requested date scaffold. That
        means the first requested day has the true stock-on-hand position even
        when the opening movements happened before start_date.
    """
    start = _as_date(start_date)
    end = _as_date(end_date)
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    log.info("Computing dense running balance from %s to %s", start, end)

    query = text("""
        WITH params AS (
            SELECT
                CAST(:start_date AS date) AS start_date,
                CAST(:end_date AS date)   AS end_date
        ),
        date_spine AS (
            SELECT d.date_key, d.full_date
            FROM warehouse.dim_date d
            CROSS JOIN params p
            WHERE d.full_date BETWEEN p.start_date AND p.end_date
        ),
        active_scope AS (
            SELECT product_key, warehouse_key
            FROM mart.dim_reconciliation_scope
            CROSS JOIN params p
            WHERE is_in_scope = TRUE
              AND (removed_at IS NULL OR removed_at >= p.start_date)
        ),
        fallback_scope AS (
            SELECT DISTINCT product_key, warehouse_key
            FROM warehouse.fact_inventory_movements
            WHERE NOT EXISTS (SELECT 1 FROM active_scope)
        ),
        scope AS (
            SELECT product_key, warehouse_key FROM active_scope
            UNION
            SELECT product_key, warehouse_key FROM fallback_scope
        ),
        daily_movements AS (
            SELECT
                f.product_key,
                f.warehouse_key,
                d.date_key,
                SUM(f.quantity) AS daily_qty
            FROM warehouse.fact_inventory_movements f
            JOIN warehouse.dim_date d ON d.date_key = f.date_key
            CROSS JOIN params p
            WHERE d.full_date <= p.end_date
            GROUP BY f.product_key, f.warehouse_key, d.date_key
        ),
        balances_on_movement_days AS (
            SELECT
                product_key,
                warehouse_key,
                date_key,
                SUM(daily_qty) OVER (
                    PARTITION BY product_key, warehouse_key
                    ORDER BY date_key
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_balance
            FROM daily_movements
        ),
        scaffold AS (
            SELECT
                ds.date_key,
                ds.full_date,
                s.product_key,
                s.warehouse_key
            FROM date_spine ds
            CROSS JOIN scope s
        )
        SELECT
            sc.full_date AS reconciliation_date,
            sc.product_key,
            sc.warehouse_key,
            COALESCE(ff.running_balance, 0)::INTEGER AS derived_qty
        FROM scaffold sc
        LEFT JOIN LATERAL (
            SELECT b.running_balance
            FROM balances_on_movement_days b
            WHERE b.product_key = sc.product_key
              AND b.warehouse_key = sc.warehouse_key
              AND b.date_key <= sc.date_key
            ORDER BY b.date_key DESC
            LIMIT 1
        ) ff ON TRUE
        ORDER BY sc.full_date, sc.product_key, sc.warehouse_key
    """)

    df = run_query(
        engine,
        query,
        params={"start_date": start.isoformat(), "end_date": end.isoformat()},
    )
    if df.empty:
        return pd.DataFrame(columns=[
            "reconciliation_date", "product_key", "warehouse_key", "derived_qty",
        ])

    df["reconciliation_date"] = pd.to_datetime(df["reconciliation_date"]).dt.date
    df["product_key"] = df["product_key"].astype(int)
    df["warehouse_key"] = df["warehouse_key"].astype(int)
    df["derived_qty"] = df["derived_qty"].astype(int)

    log.info("  Produced %s dense stock positions", f"{len(df):,}")
    return df


def reconcile_stock(
    derived_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    abs_threshold: int = DEFAULT_ABS_THRESHOLD,
    pct_threshold: float = DEFAULT_PCT_THRESHOLD,
) -> pd.DataFrame:
    """
    Pure function: compare derived stock to ERP-reported stock,
    flag drift above thresholds.

    Three-state output (Decision 11A — flag, don't pick a winner):
      - 'reconciled'              : variance within thresholds
      - 'variance_above_threshold': abs and pct both exceed thresholds
      - 'no_erp_snapshot'         : derived has data, snapshot is missing

    Args:
        derived_df:    output of compute_running_balance
        snapshot_df:   from warehouse.bridge_inventory_snapshot
        abs_threshold: alert if |derived - reported| > this
        pct_threshold: ALSO alert if |derived - reported| / reported > this
                       (both must be true to alert — hybrid threshold)

    Returns:
        DataFrame matching mart.stock_reconciliation schema:
        reconciliation_date, product_key, warehouse_key,
        derived_qty, reported_qty, snapshot_source,
        abs_variance, pct_variance, alert_flag, alert_reason.
    """
    log.info(
        "Reconciling derived stock against ERP snapshots "
        "(abs_threshold=%s, pct_threshold=%s)",
        abs_threshold,
        pct_threshold,
    )

    if derived_df.empty:
        return pd.DataFrame(columns=RECONCILIATION_COLUMNS)

    required_derived = {
        "reconciliation_date", "product_key", "warehouse_key", "derived_qty",
    }
    required_snapshot = {
        "reconciliation_date", "product_key", "warehouse_key",
        "reported_qty", "snapshot_source",
    }
    missing_derived = required_derived - set(derived_df.columns)
    missing_snapshot = required_snapshot - set(snapshot_df.columns)
    if missing_derived:
        raise ValueError(f"derived_df missing columns: {sorted(missing_derived)}")
    if missing_snapshot:
        raise ValueError(f"snapshot_df missing columns: {sorted(missing_snapshot)}")

    derived = derived_df.copy()
    snapshots = snapshot_df.copy()

    key_cols = ["reconciliation_date", "product_key", "warehouse_key"]
    derived["reconciliation_date"] = pd.to_datetime(
        derived["reconciliation_date"]
    ).dt.date
    snapshots["reconciliation_date"] = pd.to_datetime(
        snapshots["reconciliation_date"]
    ).dt.date

    derived["product_key"] = derived["product_key"].astype(int)
    derived["warehouse_key"] = derived["warehouse_key"].astype(int)
    derived["derived_qty"] = derived["derived_qty"].astype(int)

    snapshots = snapshots.sort_values(key_cols).drop_duplicates(
        subset=key_cols,
        keep="last",
    )
    snapshots["product_key"] = snapshots["product_key"].astype(int)
    snapshots["warehouse_key"] = snapshots["warehouse_key"].astype(int)
    snapshots["reported_qty"] = pd.to_numeric(
        snapshots["reported_qty"],
        errors="coerce",
    )

    snapshot_cols = key_cols + ["reported_qty", "snapshot_source"]
    merged = derived.merge(
        snapshots[snapshot_cols],
        how="left",
        on=key_cols,
    )

    has_snapshot = merged["reported_qty"].notna()
    derived_qty = merged["derived_qty"].astype(float)
    reported_qty = merged["reported_qty"].astype(float)
    abs_diff = (derived_qty - reported_qty).abs()

    merged["abs_variance"] = (
        abs_diff.where(has_snapshot, 0)
        .fillna(0)
        .round()
        .astype(int)
    )

    denominator = reported_qty.abs()
    pct_variance = abs_diff / denominator
    pct_variance = pct_variance.where(has_snapshot & denominator.ne(0))
    merged["pct_variance"] = pct_variance.round(4)

    over_threshold = (
        has_snapshot
        & merged["abs_variance"].gt(abs_threshold)
        & merged["pct_variance"].notna()
        & merged["pct_variance"].gt(pct_threshold)
    )

    merged["alert_flag"] = over_threshold
    merged["alert_reason"] = "reconciled"
    merged.loc[~has_snapshot, "alert_reason"] = "no_erp_snapshot"
    merged.loc[over_threshold, "alert_reason"] = "variance_above_threshold"

    out = merged[RECONCILIATION_COLUMNS].copy()
    out["reported_qty"] = pd.Series(
        [None if pd.isna(value) else int(value) for value in out["reported_qty"]],
        index=out.index,
        dtype=object,
    )

    log.info(
        "  Reconciliation output: %s rows, %s variance alerts, %s missing snapshots",
        f"{len(out):,}",
        f"{int(out['alert_flag'].sum()):,}",
        f"{int((out['alert_reason'] == 'no_erp_snapshot').sum()):,}",
    )
    return out


def build_reconciliation_scope(engine: Engine) -> int:
    """
    Populate mart.dim_reconciliation_scope with (sku, warehouse) pairs
    that have had movement activity in the last 90 days.

    This keeps the dense reconciliation cross-join sane — if we crossed
    every SKU with every warehouse, we'd produce stock positions for
    SKUs that have never been at that warehouse.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Count of (sku, warehouse) pairs in scope.
    """
    log.info("Refreshing mart.dim_reconciliation_scope from recent movements")

    close_stale_sql = text("""
        WITH latest_fact_date AS (
            SELECT MAX(d.full_date)::date AS as_of_date
            FROM warehouse.fact_inventory_movements f
            JOIN warehouse.dim_date d ON d.date_key = f.date_key
        )
        UPDATE mart.dim_reconciliation_scope scope
        SET
            is_in_scope = FALSE,
            removed_at = latest_fact_date.as_of_date,
            notes = 'Automatically removed: no movement activity in the last 90 days'
        FROM latest_fact_date
        WHERE latest_fact_date.as_of_date IS NOT NULL
          AND scope.is_in_scope = TRUE
          AND NOT EXISTS (
              SELECT 1
              FROM warehouse.fact_inventory_movements f
              JOIN warehouse.dim_date d ON d.date_key = f.date_key
              WHERE f.product_key = scope.product_key
                AND f.warehouse_key = scope.warehouse_key
                AND d.full_date >= latest_fact_date.as_of_date - INTERVAL '90 days'
          )
    """)

    upsert_recent_sql = text("""
        WITH latest_fact_date AS (
            SELECT MAX(d.full_date)::date AS as_of_date
            FROM warehouse.fact_inventory_movements f
            JOIN warehouse.dim_date d ON d.date_key = f.date_key
        ),
        recent_activity AS (
            SELECT DISTINCT
                f.product_key,
                f.warehouse_key,
                latest_fact_date.as_of_date
            FROM warehouse.fact_inventory_movements f
            JOIN warehouse.dim_date d ON d.date_key = f.date_key
            CROSS JOIN latest_fact_date
            WHERE latest_fact_date.as_of_date IS NOT NULL
              AND d.full_date >= latest_fact_date.as_of_date - INTERVAL '90 days'
        )
        INSERT INTO mart.dim_reconciliation_scope (
            product_key,
            warehouse_key,
            is_in_scope,
            added_at,
            removed_at,
            notes
        )
        SELECT
            product_key,
            warehouse_key,
            TRUE,
            as_of_date,
            NULL,
            'Automatically scoped from movement activity in the last 90 days'
        FROM recent_activity
        ON CONFLICT (product_key, warehouse_key) DO UPDATE SET
            is_in_scope = TRUE,
            removed_at = NULL,
            notes = EXCLUDED.notes
    """)

    count_sql = text("""
        SELECT COUNT(*)
        FROM mart.dim_reconciliation_scope
        WHERE is_in_scope = TRUE
          AND removed_at IS NULL
    """)

    with engine.begin() as conn:
        conn.execute(close_stale_sql)
        conn.execute(upsert_recent_sql)
        active_count = int(conn.execute(count_sql).scalar() or 0)

    log.info("  Active reconciliation scope pairs: %s", f"{active_count:,}")
    return active_count
