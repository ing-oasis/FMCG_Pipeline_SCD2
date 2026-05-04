"""
Database helpers for the PB Tech pipeline.

Two purposes:
  1. Engine factory — single source of truth for how we connect to Postgres.
     The DAG's tasks call get_engine() rather than constructing connections
     ad-hoc; this means changing the connection string is a one-line edit.
  2. Audit log writers — write_etl_log() and write_dq_alert() are called
     from inside transform tasks to populate the audit/observability tables.
"""

import json
import logging
from typing import Any, Optional

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def run_query(engine: Engine, query, params=None) -> pd.DataFrame:
    """Execute a SQL query and return the result as a DataFrame.

    Replaces pd.read_sql() which breaks under the pandas 2.2 +
    SQLAlchemy 1.4 combination that Airflow 2.9 requires."""
    with engine.connect() as conn:
        result = conn.execute(query if isinstance(query, sqlalchemy.sql.expression.TextClause) else text(query),
                              params or {})
        rows = result.fetchall()
        columns = list(result.keys())
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

def get_engine(conn_id: str = "pb_postgres") -> Engine:
    """
    Return a SQLAlchemy engine connected to the warehouse database.

    Two paths:
      - In Airflow context: use the registered connection (PostgresHook)
      - Outside Airflow (e.g., notebooks, smoke tests): use a hardcoded
        connection string with credentials that match docker-compose.yml.

    The Airflow path is preferred because credentials live in Airflow's
    metadata DB, not in code.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_sqlalchemy_engine()
    except (ImportError, Exception):
        # Fallback for local development / smoke tests.
        # Connection string mirrors docker-compose.yml.
        return create_engine(
            "postgresql+psycopg2://airflow:airflow@localhost:5432/pbtech_warehouse",
            future=True,
        )


# ---------------------------------------------------------------------------
# Audit log writers
# ---------------------------------------------------------------------------

def write_etl_log(
    engine: Engine,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    status: str,
    started_at,
    finished_at=None,
    rows_in: Optional[int] = None,
    rows_out: Optional[int] = None,
    rows_changed: Optional[int] = None,
    notes: Optional[str] = None,
    error_message: Optional[str] = None,
) -> int:
    """
    Insert a row into audit.etl_run_log. Returns the new run_id.

    Tasks call this twice in their lifecycle: once with status='RUNNING' at
    start, then UPDATE the row with status='SUCCESS' or 'FAILED' at end.
    For simplicity, this version inserts a single row at task end.
    """
    sql = text("""
        INSERT INTO audit.etl_run_log
            (dag_id, dag_run_id, task_id, started_at, finished_at, status,
             rows_in, rows_out, rows_changed, notes, error_message)
        VALUES
            (:dag_id, :dag_run_id, :task_id, :started_at, :finished_at, :status,
             :rows_in, :rows_out, :rows_changed, :notes, :error_message)
        RETURNING run_id
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_changed": rows_changed,
            "notes": notes,
            "error_message": error_message,
        })
        return result.scalar_one()


def write_dq_alert(
    engine: Engine,
    dag_run_id: str,
    severity: str,
    alert_type: str,
    affected_table: str,
    description: str,
    affected_rows: Optional[int] = None,
    sample_values: Optional[dict] = None,
) -> int:
    """
    Insert a row into mart.dq_alerts.

    Soft DQ failures call this; the pipeline continues running.
    Hard DQ failures raise an exception instead.
    """
    sql = text("""
        INSERT INTO mart.dq_alerts
            (dag_run_id, alert_severity, alert_type, affected_table,
             affected_rows, sample_values, description)
        VALUES
            (:dag_run_id, :severity, :alert_type, :affected_table,
             :affected_rows, :sample_values, :description)
        RETURNING alert_id
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, {
            "dag_run_id": dag_run_id,
            "severity": severity,
            "alert_type": alert_type,
            "affected_table": affected_table,
            "affected_rows": affected_rows,
            "sample_values": json.dumps(sample_values) if sample_values else None,
            "description": description,
        })
        return result.scalar_one()


def write_scd2_audit(
    conn,                      # SQLAlchemy connection — must be in same txn
    dag_run_id: str,
    dimension_table: str,
    natural_key: str,
    change_type: str,
    closed_cost_key: Optional[int] = None,
    new_cost_key: Optional[int] = None,
    old_attributes: Optional[dict] = None,
    new_attributes: Optional[dict] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Insert a row into audit.scd2_change_log within an existing transaction.

    CRITICAL: This function takes a connection (not an engine) because it
    must run inside the same transaction as the dim table change. If the
    dim insert/update commits but the audit log insert fails (or vice versa),
    you have unaudited changes — which breaks the "audit explains the dim"
    invariant. The two MUST commit or rollback together.
    """
    sql = text("""
        INSERT INTO audit.scd2_change_log
            (dag_run_id, dimension_table, natural_key, change_type,
             closed_cost_key, new_cost_key, old_attributes, new_attributes, notes)
        VALUES
            (:dag_run_id, :dim_table, :natural_key, :change_type,
             :closed_cost_key, :new_cost_key, :old_attrs, :new_attrs, :notes)
    """)
    conn.execute(sql, {
        "dag_run_id": dag_run_id,
        "dim_table": dimension_table,
        "natural_key": natural_key,
        "change_type": change_type,
        "closed_cost_key": closed_cost_key,
        "new_cost_key": new_cost_key,
        "old_attrs": json.dumps(old_attributes) if old_attributes else None,
        "new_attrs": json.dumps(new_attributes) if new_attributes else None,
        "notes": notes,
    })
