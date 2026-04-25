import pandas as pd
from sqlalchemy import text

try:
    from db_bootstrap import get_engine
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from db_bootstrap import get_engine

_engine = None


def get_engine_cached():
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


def load_stock_data() -> pd.DataFrame:
    """Latest snapshot with one row per company from stock_data."""
    query = """
        WITH latest_day AS (
            SELECT MAX(DATE(date)) AS max_day
            FROM stock_data
        ),
        ranked AS (
            SELECT
                sd.*,
                ROW_NUMBER() OVER (
                    PARTITION BY sd.ticker
                    ORDER BY sd.date DESC
                ) AS rn
            FROM stock_data sd
            JOIN latest_day ld ON DATE(sd.date) = ld.max_day
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker
    """
    return pd.read_sql(query, get_engine_cached())


def load_price_history() -> pd.DataFrame:
    """Full price history ordered by company and date."""
    query = """
        SELECT *
        FROM price_history
        ORDER BY ticker, date
    """
    return pd.read_sql(query, get_engine_cached())


def load_price_history_ticker(ticker: str) -> pd.DataFrame:
    """Price history for one company."""
    query = text(
        """
        SELECT *
        FROM price_history
        WHERE ticker = :ticker
        ORDER BY date
        """
    )
    return pd.read_sql(query, get_engine_cached(), params={"ticker": ticker})


def get_tickers(stock_df: pd.DataFrame) -> list[str]:
    if stock_df.empty or "ticker" not in stock_df.columns:
        return []
    return sorted(stock_df["ticker"].dropna().astype(str).unique().tolist())


def get_sectors(stock_df: pd.DataFrame) -> list[str]:
    if stock_df.empty or "sector" not in stock_df.columns:
        return []
    return sorted(stock_df["sector"].dropna().astype(str).unique().tolist())
