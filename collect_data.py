# collect_data.py
# FLOW:
# 1. Fetch live NIFTY 50 ticker list from NSE CSV  ← only used for ticker names
# 2. Convert to Yahoo Finance tickers (.NS suffix)
# 3. Fetch 35 days of price history per company    ← needed for 30d volume avg
# 4. Fetch fundamentals from yfinance info dict
# 5. Sector comes from yfinance automatically      ← no hardcoding
# 6. Return single DataFrame → save raw to PostgreSQL first → then clean_data.py
import yfinance as yf
import pandas as pd
import requests
import time
import logging
from datetime import datetime

from sqlalchemy import inspect, text

from db_bootstrap import ensure_database_exists, get_engine, setup_logging

setup_logging()

def migrate_raw_table_schema(engine) -> None:
    if not inspect(engine).has_table("raw_stock_data"):
        return

    columns_query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'raw_stock_data'
        """
    )
    with engine.connect() as conn:
        current_columns = {
            row[0] for row in conn.execute(columns_query).fetchall()
        }

    rename_map = {
        "market_cap": "market_cap_cr",
        "roe": "roe_pr",
        "profit_margin": "profit_margin_pr",
        "qoq_revenue_growth": "qoq_revenue_growth_pr",
    }

    with engine.begin() as conn:
        for old_name, new_name in rename_map.items():
            if old_name in current_columns and new_name not in current_columns:
                conn.execute(
                    text(f'ALTER TABLE raw_stock_data RENAME COLUMN "{old_name}" TO "{new_name}"')
                )
                logging.info(f"Renamed raw_stock_data.{old_name} to {new_name}")


def save_raw_data(df: pd.DataFrame) -> None:
    if df.empty:
        logging.warning("No raw rows to save")
        return

    engine = get_engine()
    migrate_raw_table_schema(engine)

    run_date = pd.to_datetime(df["date"].iloc[0]).date()
    if inspect(engine).has_table("raw_stock_data"):
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM raw_stock_data WHERE DATE(date) = :run_date"),
                {"run_date": run_date},
            )

    df.to_sql("raw_stock_data", con=engine, if_exists="append", index=False)
    logging.info(f"Saved {len(df)} raw rows to raw_stock_data for {run_date}")
    print(f"✅ Saved {len(df)} raw rows for {run_date} to PostgreSQL table raw_stock_data")


def _latest_numeric_value(frame: pd.DataFrame, row_names: list[str]) -> float | None:
    for row_name in row_names:
        if row_name not in frame.index:
            continue

        values = pd.to_numeric(frame.loc[row_name], errors="coerce").dropna()
        if not values.empty:
            try:
                return float(values.iloc[0])
            except Exception:
                continue

    return None


def get_roe_manual(stock: yf.Ticker) -> float | None:
    try:
        balance = stock.quarterly_balance_sheet
        income = stock.quarterly_financials

        if balance is None or balance.empty or income is None or income.empty:
            return None

        equity = _latest_numeric_value(balance, [
            "Stockholders Equity",
            "Total Stockholder Equity",
            "Total Equity Gross Minority Interest",
            "Common Stock Equity",
        ])
        net_income = _latest_numeric_value(income, [
            "Net Income",
            "Net Income Common Stockholders",
            "Net Income Applicable To Common Shares",
        ])

        if equity is None or equity == 0 or net_income is None:
            return None

        return round((net_income / equity) * 100, 2)
    except Exception:
        return None


def get_de_manual(stock: yf.Ticker) -> float | None:
    try:
        balance = stock.quarterly_balance_sheet
        if balance is None or balance.empty:
            return None

        debt = _latest_numeric_value(balance, [
            "Total Debt",
            "Long Term Debt",
            "Short Long Term Debt",
            "Long Term Debt And Capital Lease Obligation",
        ])
        equity = _latest_numeric_value(balance, [
            "Stockholders Equity",
            "Total Stockholder Equity",
            "Total Equity Gross Minority Interest",
            "Common Stock Equity",
        ])

        if debt is None or equity is None or equity == 0:
            return None

        return round(debt / equity, 3)
    except Exception:
        return None


# ─────────────────────────────────────────────
# STEP 1: FETCH LIVE NIFTY 50 TICKER LIST
#
# WHY CSV: NSE CSV only gives us the 50 ticker
# symbols (e.g. "TCS", "INFY"). Nothing else.
# No prices, no financials. We just need the names
# so we know which companies to ask yfinance about.
# The CSV itself never goes to PostgreSQL.
# ─────────────────────────────────────────────
def get_nifty50_tickers() -> list[str]:
    url = "https://archives.nseindia.com/content/indices/ind_nifty50list.csv"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.nseindia.com/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        # CHANGE: removed print(response.raise_for_status())
        # raise_for_status() returns None on success — printing None is misleading
        # It still raises an exception if the request failed, so error handling works

        df = pd.read_csv(pd.io.common.StringIO(response.text))
        symbols = df["Symbol"].str.strip().tolist()

        # NSE: "TCS" → Yahoo Finance: "TCS.NS"
        tickers = [symbol + ".NS" for symbol in symbols]

        logging.info(f"Fetched {len(tickers)} tickers from NSE NIFTY 50 list")
        print(f"✅ Got {len(tickers)} tickers from NSE — will now fetch data from yfinance")
        return tickers

    except Exception as e:
        logging.error(f"Failed to fetch NIFTY 50 list from NSE: {e}")
        print(f"❌ NSE fetch failed: {e}")
        return []


# ─────────────────────────────────────────────
# STEP 2: FETCH DATA FOR ONE COMPANY
#
# CHANGE 1: period="1d" → period="35d"
# Reason: volume_ratio = today's volume ÷ 30-day avg volume
# To calculate a 30-day average you need 30 rows of history.
# "35d" gives a small buffer in case of market holidays.
# We still only store TODAY's row — but we need the history
# to compute the ratio before storing.
#
# CHANGE 2: removed stock.items() and info.items() debug prints
# Reason: stock.items() doesn't exist on a Ticker object —
# this would throw an AttributeError at runtime.
# info.items() would dump hundreds of raw key-value pairs
# to your terminal every single run — unusable noise.
# Logging handles any errors cleanly instead.
#
# CHANGE 3: added volume_30d_avg and volume_ratio to result dict
# Reason: these are raw computed values from price history,
# not indicators — so they belong in collect_data, not indicators.py
# volume_ratio is used directly in the alerts system later.
# ─────────────────────────────────────────────
def fetch_company_data(ticker: str) -> dict | None:
    result = {
        "date":               datetime.today().date(),
        "ticker":             ticker.replace(".NS", ""),

        # Identity — from yfinance, never hardcoded
        "company_name":       None,
        "sector":             None,
        "industry":           None,

        # Price — today's values only (from last row of 35d history)
        "open":               None,
        "high":               None,
        "low":                None,
        "close":              None,

        # Volume — raw + ratio
        "volume":             None,       # today's raw share count
        "volume_30d_avg":     None,       # average daily volume over last 30 days
        "volume_ratio":       None,       # today ÷ 30d avg (e.g. 2.3 = spike)

        # Fundamentals
        "market_cap_cr":      None,
        "pe_ratio":           None,
        "roe_pr":             None,
        "profit_margin_pr":   None,
        "debt_to_equity":     None,
        "week52_high":        None,
        "week52_low":         None,

        # Revenue growth
        "qoq_revenue_growth_pr": None,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        # CHANGE: removed debug prints here
        # stock.items() → AttributeError (Ticker has no .items())
        # info.items()  → floods terminal with 100+ raw fields every run

        # ── Identity ──────────────────────────
        result["company_name"] = info.get("longName")
        result["sector"]       = info.get("sector")
        result["industry"]     = info.get("industry")

        # ── Price + Volume ────────────────────
        # CHANGE: period="35d" instead of "1d"
        # We need 30+ rows to compute volume_30d_avg
        # iloc[-1] still gives today's price — last row of the range
        hist = stock.history(period="35d", interval="1d")

        if not hist.empty:
            # Today's price — last row
            result["open"]   = round(float(hist["Open"].iloc[-1]),  2)
            result["high"]   = round(float(hist["High"].iloc[-1]),  2)
            result["low"]    = round(float(hist["Low"].iloc[-1]),   2)
            result["close"]  = round(float(hist["Close"].iloc[-1]), 2)

            # Today's raw volume
            today_vol = int(hist["Volume"].iloc[-1])
            result["volume"] = today_vol

            # 30-day average volume (exclude today = iloc[:-1], take last 30)
            # We exclude today so today's spike doesn't inflate its own average
            avg_vol = hist["Volume"].iloc[:-1].tail(30).mean()
            result["volume_30d_avg"] = round(avg_vol, 0)

            # Volume ratio — how many times today's volume vs normal
            # e.g. 2.3 means today had 2.3x the usual trading activity
            if avg_vol and avg_vol > 0:
                result["volume_ratio"] = round(today_vol / avg_vol, 2)

        # ── Fundamentals ──────────────────────
        market_cap_raw = info.get("marketCap")
        if market_cap_raw:
            result["market_cap_cr"] = round(market_cap_raw / 1e7, 2)

        result["pe_ratio"]       = info.get("trailingPE")
        result["week52_high"]    = info.get("fiftyTwoWeekHigh")
        result["week52_low"]     = info.get("fiftyTwoWeekLow")

        debt_to_equity = info.get("debtToEquity")
        if debt_to_equity is None:
            debt_to_equity = get_de_manual(stock)
        result["debt_to_equity"] = debt_to_equity

        roe    = info.get("returnOnEquity")
        margin = info.get("profitMargins")
        result["roe_pr"] = round(roe * 100, 2) if roe is not None else get_roe_manual(stock)
        result["profit_margin_pr"] = round(margin * 100, 2) if margin is not None else None

        # ── Revenue growth ────────────────────
        result["qoq_revenue_growth_pr"] = get_qoq_growth(stock)

        logging.info(f"  ✅ {result['company_name']} | {result['sector']} | vol_ratio={result['volume_ratio']}")

    except Exception as e:
        logging.warning(f"  ⚠️  {ticker} failed: {e}")
        return None

    return result


# ─────────────────────────────────────────────
# HELPER: QoQ REVENUE GROWTH — unchanged
# ─────────────────────────────────────────────
def get_qoq_growth(stock: yf.Ticker) -> float | None:
    try:
        q = stock.quarterly_financials
        if q is None or q.empty or "Total Revenue" not in q.index:
            return None
        rev = q.loc["Total Revenue"]
        if len(rev) < 2 or rev.iloc[1] == 0:
            return None
        return round(((rev.iloc[0] - rev.iloc[1]) / rev.iloc[1]) * 100, 2)
    except Exception:
        return None


# ─────────────────────────────────────────────
# STEP 3: COLLECT ALL 50 COMPANIES
# ─────────────────────────────────────────────
def collect_all_data() -> pd.DataFrame:
    logging.info("=" * 50)
    logging.info("Pipeline started — collect_data.py")

    tickers = get_nifty50_tickers()

    if not tickers:
        logging.error("No tickers — pipeline stopped")
        raise Exception("Could not fetch NIFTY 50 list. Check internet connection.")

    all_rows = []

    for i, ticker in enumerate(tickers, start=1):
        print(f"[{i}/{len(tickers)}] Fetching {ticker}...")
        row = fetch_company_data(ticker)

        if row is not None:
            all_rows.append(row)

        time.sleep(1)   # 1s pause — prevents Yahoo Finance throttling

    df = pd.DataFrame(all_rows)

    logging.info(f"collect_data.py done — {len(df)} rows collected")
    return df


# ─────────────────────────────────────────────
# RUN STANDALONE TO TEST
# ─────────────────────────────────────────────
if __name__ == "__main__":
    ensure_database_exists()
    df = collect_all_data()
    save_raw_data(df)

    print("\n── PREVIEW ──")
    print(df[[
        "company_name", "sector",
        "close", "market_cap_cr", "volume", "volume_30d_avg", "volume_ratio",
        "pe_ratio", "roe_pr", "profit_margin_pr", "qoq_revenue_growth_pr"
    ]].to_string(index=False))

    print(f"\nTotal rows collected : {len(df)}")
    print(f"\nSectors (from yfinance automatically):\n{df['sector'].value_counts()}")
    print(f"\nNull counts:\n{df.isnull().sum()}")