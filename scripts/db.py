""""
Finance Platform — Database Connection & Utilities
Single point of DB access. Swap to PostgreSQL by changing get_engine() only.
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from contextlib import contextmanager

log = logging.getLogger(__name__)


def get_connection():
    """
    SQLite connection for direct SQL operations.
    To migrate to PostgreSQL:
      → replace with: psycopg2.connect(host=..., dbname=..., user=..., password=...)
    """
    from scripts.config import DB_PATH
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_conn():
    """Context manager — auto commits and closes."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql: str, params=()) -> pd.DataFrame:
    """Run a SELECT query and return a DataFrame."""
    with get_conn() as conn:
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except Exception as e:
            log.error(f"Query failed: {e}\nSQL: {sql}")
            return pd.DataFrame()


def execute(sql: str, params=()):
    """Run a single INSERT/UPDATE/DELETE."""
    with get_conn() as conn:
        conn.execute(sql, params)


def executemany(sql: str, data: list):
    """Run a batch INSERT/UPDATE."""
    with get_conn() as conn:
        conn.executemany(sql, data)


def init_db():
    """Create all tables from schema.sql if they don't exist."""
    from scripts.config import SCHEMA_PATH
    with get_conn() as conn:
        with open(SCHEMA_PATH, "r") as f:
            conn.executescript(f.read())
    log.info("Database initialised")


def seed_entities():
    """Seed the entities table from config."""
    from scripts.config import ENTITIES
    with get_conn() as conn:
        for code, info in ENTITIES.items():
            conn.execute("""
                INSERT OR REPLACE INTO entities
                (company_code, entity_name, currency, active)
                VALUES (?, ?, ?, ?)
            """, (code, info["name"], info["currency"], 1 if info["active"] else 0))
    log.info(f"Seeded {len(ENTITIES)} entities")


def table_counts() -> dict:
    """Return row counts for all main tables — useful for health check."""
    tables = [
        "entities", "org_hierarchy", "budget_rates",
        "ad50_lines", "gl_transactions", "plan_data",
        "fte_data", "sac_detail", "account_master",
        "validation_log", "load_history",
    ]
    counts = {}
    for t in tables:
        df = query(f"SELECT COUNT(*) as cnt FROM {t}")
        counts[t] = int(df["cnt"].iloc[0]) if not df.empty else 0
    return counts


def delete_period_data(company: str, fiscal_year: int,
                       fiscal_period: int, table: str = "gl_transactions"):
    """
    Delete existing data for a period before reload.
    Core of the delete-and-replace strategy.
    """
    with get_conn() as conn:
        result = conn.execute(f"""
            DELETE FROM {table}
            WHERE company = ?
              AND fiscal_year = ?
              AND fiscal_period = ?
        """, (company, fiscal_year, fiscal_period))
        deleted = result.rowcount
    log.info(f"Deleted {deleted} rows from {table} "
             f"for {company} {fiscal_year}/{fiscal_period:02d}")
    return deleted


def delete_plan_period(company: str, fiscal_year: int,
                       fiscal_period: int, plan_type: str, version: str):
    """Delete plan_data for a specific version before reload."""
    with get_conn() as conn:
        result = conn.execute("""
            DELETE FROM plan_data
            WHERE company = ?
              AND fiscal_year = ?
              AND fiscal_period = ?
              AND plan_type = ?
              AND version = ?
        """, (company, fiscal_year, fiscal_period, plan_type, version))
        deleted = result.rowcount
    log.info(f"Deleted {deleted} plan rows for {company} "
             f"{fiscal_year}/{fiscal_period:02d} {version}")
    return deleted
