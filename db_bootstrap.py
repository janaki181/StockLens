import logging
import os
from logging.handlers import TimedRotatingFileHandler

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()


def setup_logging() -> None:
    """Configure rotating file handler: keeps 8 weeks of history, rotates weekly."""
    root_logger = logging.getLogger()
    if any(isinstance(handler, TimedRotatingFileHandler) for handler in root_logger.handlers):
        return

    handler = TimedRotatingFileHandler(
        filename="pipeline.log",
        when="W0",          # rotate every Monday
        backupCount=8,      # keep 8 weeks of history
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    ))
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE", "postgres"),
    )


def get_engine():
    return create_engine(get_database_url())


def ensure_database_exists() -> None:
    target_db = os.getenv("PGDATABASE", "postgres")

    if target_db.lower() == "postgres":
        return

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = cur.fetchone() is not None

            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
                logging.info(f"Database created: {target_db}")
                print(f"✅ Database '{target_db}' created")
            else:
                logging.info(f"Database exists: {target_db}")
    finally:
        conn.close()


def migrate_price_history_schema(engine) -> None:
    """Add any missing columns to price_history without dropping data."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("price_history"):
        return  # table doesn't exist yet — nothing to migrate

    # columns we expect to exist and their types
    expected_columns = {
        "nifty_index_close": "FLOAT",
        "vs_nifty_cumulative": "FLOAT",
        # add future new columns here as you build the project
    }

    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'price_history'
            """)).fetchall()
        }

    with engine.begin() as conn:
        if "nifty_close" in existing and "nifty_index_close" not in existing:
            conn.execute(text('ALTER TABLE price_history RENAME COLUMN "nifty_close" TO "nifty_index_close"'))
            print("✅ Renamed column: price_history.nifty_close -> nifty_index_close")
            logging.info("Migrated price_history: renamed nifty_close to nifty_index_close")
            existing.remove("nifty_close")
            existing.add("nifty_index_close")

        for col, dtype in expected_columns.items():
            if col not in existing:
                conn.execute(text(f'ALTER TABLE price_history ADD COLUMN "{col}" {dtype}'))
                print(f"✅ Added missing column: price_history.{col}")
                logging.info(f"Migrated price_history: added {col}")


def migrate_stock_table_schema(engine) -> None:
    """Add any missing columns to stock_data without dropping data."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("stock_data"):
        return  # table doesn't exist yet — nothing to migrate

    with engine.connect() as conn:
        existing_cols = {
            row[0] for row in conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'stock_data'
            """)).fetchall()
        }

    with engine.begin() as conn:
        if 'revenue_signal' not in existing_cols:
            conn.execute(text("ALTER TABLE stock_data ADD COLUMN revenue_signal VARCHAR(20)"))
            print("✅ Added missing column: stock_data.revenue_signal")
            logging.info("Migrated stock_data: added revenue_signal column")


def clear_price_history_snapshot_columns(engine) -> None:
    """Null out snapshot-only columns from price_history so it stays historical-only."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("price_history"):
        return

    snapshot_columns = [
        "market_cap_cr",
        "pe_ratio",
        "roe_pr",
        "profit_margin_pr",
        "debt_to_equity",
        "week52_high",
        "week52_low",
        "revenue_signal",
    ]

    with engine.connect() as conn:
        existing = {
            row[0]
            for row in conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'price_history'
            """)).fetchall()
        }

    columns_to_clear = [column for column in snapshot_columns if column in existing]
    if not columns_to_clear:
        return

    set_clause = ", ".join(f'"{column}" = NULL' for column in columns_to_clear)

    with engine.begin() as conn:
        conn.execute(text(f"UPDATE price_history SET {set_clause}"))

    logging.info("Cleared snapshot-only columns from price_history")


def backfill_vs_nifty_cumulative(engine) -> None:
    """Backfill missing cumulative outperformance values using existing daily vs_nifty_pct."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("price_history"):
        return

    with engine.begin() as conn:
        missing = conn.execute(text("""
            SELECT COUNT(*)
            FROM price_history
            WHERE vs_nifty_cumulative IS NULL
        """)).scalar()

        if not missing:
            return

        conn.execute(text("""
            WITH calc AS (
                SELECT
                    ticker,
                    date,
                    ROUND(
                        SUM(COALESCE(vs_nifty_pct, 0))
                        OVER (
                            PARTITION BY ticker
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )::numeric,
                        2
                    ) AS cumulative_value
                FROM price_history
            )
            UPDATE price_history p
            SET vs_nifty_cumulative = c.cumulative_value
            FROM calc c
            WHERE p.ticker = c.ticker
              AND p.date = c.date
              AND p.vs_nifty_cumulative IS NULL
        """))

    print(f"✅ Backfilled vs_nifty_cumulative for {missing} existing rows")
    logging.info(f"Backfilled vs_nifty_cumulative for {missing} rows")
