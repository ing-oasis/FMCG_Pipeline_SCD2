"""
Mart-layer aggregation builders.

Produces analytics-ready outputs from warehouse facts. Each builder is a
SQL query (sometimes wrapped in a thin pandas layer for the merge logic).

Marts use grain-as-PK pattern: the composite of the grouping columns
(date + region + store_type + category) IS the primary key. Refreshes
use ON CONFLICT DO UPDATE so re-running is idempotent.
"""

import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def build_daily_margin_summary(engine: Engine) -> int:
    """
    Refresh mart.daily_margin_summary from current fact_sales + dimensions.

    The query joins fact_sales to dim_product_cost on cost_key (a simple
    integer FK — the date-range work was already done at fact-load time).
    This is the architectural payoff: mart refreshes are fast because the
    expensive SCD2 lookup happened once at fact load.

    Returns:
        Row count inserted into the mart.

    Implementation note: truncate-and-reload is fine at this scale.
    For larger windows we'd switch to incremental upsert by summary_date.
    """
    log.info("Refreshing mart.daily_margin_summary")

    truncate_sql = "TRUNCATE TABLE mart.daily_margin_summary"
    insert_sql = """
        INSERT INTO mart.daily_margin_summary (
            summary_date, region, store_type, category,
            transaction_count, units_sold, return_count,
            gross_revenue, return_value, net_revenue,
            total_cost, return_cost,
            gross_margin, margin_pct,
            orphan_txn_count, orphan_value
        )
        SELECT
            d.full_date  AS summary_date,
            s.region,
            s.store_type,
            p.category,

            -- Volume: count distinct transactions (handles dedup) for non-returns
            COUNT(DISTINCT f.transaction_id) FILTER (WHERE NOT f.is_return)
                AS transaction_count,
            COALESCE(SUM(f.quantity) FILTER (WHERE NOT f.is_return), 0)
                AS units_sold,
            COUNT(*) FILTER (WHERE f.is_return)
                AS return_count,

            -- Revenue (2dp display precision)
            COALESCE(SUM(f.total_amount) FILTER (WHERE NOT f.is_return), 0)
                AS gross_revenue,
            COALESCE(SUM(f.total_amount) FILTER (WHERE f.is_return), 0)
                AS return_value,
            COALESCE(SUM(f.total_amount), 0)
                AS net_revenue,

            -- Cost (4dp precision preserved through aggregation)
            -- For orphans, c.unit_cost is 0 (sentinel row) — they contribute
            -- 0 to total_cost, which is wrong for margin but flagged via
            -- orphan_txn_count.
            COALESCE(SUM(f.quantity * c.unit_cost) FILTER (WHERE NOT f.is_return), 0)
                AS total_cost,
            COALESCE(SUM(f.quantity * c.unit_cost) FILTER (WHERE f.is_return), 0)
                AS return_cost,

            -- Margin = net_revenue - total_cost
            -- (returns reduce both, so this naturally handles them)
            (COALESCE(SUM(f.total_amount), 0)
             - COALESCE(SUM(f.quantity * c.unit_cost), 0))
                AS gross_margin,

            -- Margin %  — nullable (div by zero on no-revenue days)
            CASE
                WHEN COALESCE(SUM(f.total_amount), 0) = 0 THEN NULL
                ELSE ROUND(
                    ((COALESCE(SUM(f.total_amount), 0)
                      - COALESCE(SUM(f.quantity * c.unit_cost), 0))
                     / NULLIF(SUM(f.total_amount), 0))::NUMERIC,
                    4
                )
            END AS margin_pct,

            -- Quality flags
            COUNT(*) FILTER (WHERE f.is_orphan_cost)
                AS orphan_txn_count,
            COALESCE(SUM(f.total_amount) FILTER (WHERE f.is_orphan_cost), 0)
                AS orphan_value

        FROM warehouse.fact_sales        f
        JOIN warehouse.dim_date          d ON f.date_key    = d.date_key
        JOIN warehouse.dim_product       p ON f.product_key = p.product_key
        JOIN warehouse.dim_store         s ON f.store_key   = s.store_key
        JOIN warehouse.dim_product_cost  c ON f.cost_key    = c.cost_key

        GROUP BY d.full_date, s.region, s.store_type, p.category
    """

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))
        result = conn.execute(text(insert_sql))
        rowcount = result.rowcount

    log.info(f"  Inserted {rowcount:,} rows into mart.daily_margin_summary")
    return rowcount
