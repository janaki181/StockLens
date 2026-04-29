import logging
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import inspect, text

from collect_data import get_nifty50_tickers
from db_bootstrap import ensure_database_exists, get_engine, setup_logging

setup_logging()

PRICE_HISTORY_COLUMNS = [
    "date", "ticker", "company_name", "sector", "industry",
    "open", "high", "low", "close", "volume",
    "volume_30d_avg", "volume_ratio",
    "nifty_index_close", "sma_20", "sma_50", "sma_200", "ema_20",
    "rsi_14", "macd", "macd_signal", "macd_hist",
    "bb_upper", "bb_lower", "bb_mid",
    "vs_nifty_pct", "vs_nifty_cumulative",
]

# 420 calendar days gives enough traded sessions to compute stable SMA-200 values.
HISTORY_WARMUP_DAYS = 420
# First load keeps only the most recent 200 rows per company for dashboard focus.
ROWS_TO_STORE = 200
CONTEXT_LIMIT = 250


def fetch_ticker_info(engine, ticker: str) -> dict:
    defaults = {
        "company_name": ticker.replace(".NS", ""),
        "sector": None, "industry": None,
        "market_cap_cr": None, "pe_ratio": None,
        "roe_pr": None, "profit_margin_pr": None,
        "debt_to_equity": None, "week52_high": None, "week52_low": None,
        "volume_ratio": None, "revenue_signal": None,
    }

    ticker_key = ticker.replace(".NS", "")

    for table_name in ("stock_data", "raw_stock_data"):
        if not inspect(engine).has_table(table_name):
            print(f"    ⚠️  Table {table_name} does not exist — skipping")
            continue

        with engine.connect() as conn:
            table_columns = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = :table_name
                        """
                    ),
                    {"table_name": table_name},
                ).fetchall()
            }

        selected = [col for col in defaults.keys() if col in table_columns]
        if not selected:
            print(f"    ⚠️  No usable metadata columns found in {table_name}")
            continue

        selected_columns = ", ".join(selected)

        query = text(f"""
            SELECT {selected_columns}
            FROM {table_name}
            WHERE ticker = :ticker
            ORDER BY date DESC
            LIMIT 1
        """)
        with engine.connect() as conn:
            row = conn.execute(query, {"ticker": ticker_key}).fetchone()

        if row:
            print(f"    ✅ Metadata loaded from {table_name}")
            values = dict(row._mapping)
            for key, default_value in defaults.items():
                if values.get(key) is None:
                    values[key] = default_value
            return values
        else:
            print(f"⚠️ No metadata row found in {table_name} for ticker={ticker_key}")

    print(f"    ❌ No metadata found anywhere — using defaults for {ticker_key}")
    return defaults


def _normalize_history(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history

    frame = history.reset_index().copy()
    date_column = frame.columns[0]
    frame = frame.rename(columns={date_column: "date"})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame = frame.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    frame = frame[["date", "open", "high", "low", "close", "volume"]]
    frame = frame[frame["date"].notna()]
    frame = frame[frame["close"].notna() & (frame["close"] > 0)]
    return frame.sort_values("date").reset_index(drop=True)


def fetch_price_history(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    print(f"    🔍 Fetching price history: {ticker} from {start_date} to {end_date}")
    try:
        history = yf.Ticker(ticker).history(
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
        )
    except Exception as exc:
        logging.warning(f"History fetch failed for {ticker}: {exc}")
        print(f"    ❌ yfinance exception: {exc}")
        return pd.DataFrame()

    result = _normalize_history(history)
    print(f"    📊 Rows returned from yfinance: {len(result)}")
    return result


def fetch_nifty_history(start_date: date, end_date: date) -> pd.DataFrame:
    history = fetch_price_history("^NSEI", start_date, end_date)
    if history.empty:
        return history
    return history[["date", "close"]].rename(columns={"close": "nifty_index_close"})


def get_last_trading_day() -> date:
    today = date.today()
    lookback_start = today - timedelta(days=14)

    # Fetch NIFTY history to find the actual last trading day
    # This handles weekends, Indian public holidays automatically
    print(f"    🗓️  Checking last trading day (today={today})")
    nifty_history = fetch_nifty_history(lookback_start, today)

    if not nifty_history.empty:
        last_day = max(nifty_history["date"])
        print(f"    🗓️  Last trading day confirmed: {last_day}")
        return last_day

    # Fallback — roll back to Friday if weekend
    if today.weekday() >= 5:
        fallback = today - timedelta(days=today.weekday() - 4)
        print(f"    🗓️  Fallback to Friday: {fallback}")
        return fallback

    print(f"    🗓️  Using today as trading day: {today}")
    return today


def load_existing_last_dates(engine) -> dict[str, date]:
    if not inspect(engine).has_table("price_history"):
        print("    ℹ️  price_history table does not exist yet — first run")
        return {}

    query = text("""
        SELECT ticker, MAX(date) AS last_date
        FROM price_history
        GROUP BY ticker
    """)
    last_dates = pd.read_sql_query(query, con=engine)
    if last_dates.empty:
        return {}

    last_dates["last_date"] = pd.to_datetime(last_dates["last_date"], errors="coerce").dt.date
    return dict(zip(last_dates["ticker"], last_dates["last_date"]))


def load_context_history(engine, ticker: str) -> pd.DataFrame:
    if not inspect(engine).has_table("price_history"):
        return pd.DataFrame(columns=["date", "close", "volume"])

    query = text(f"""
        SELECT date, close, volume
        FROM price_history
        WHERE ticker = :ticker
        ORDER BY date DESC
        LIMIT {CONTEXT_LIMIT}
    """)
    context = pd.read_sql_query(query, con=engine, params={"ticker": ticker})
    if context.empty:
        return context

    context["date"] = pd.to_datetime(context["date"], errors="coerce").dt.date
    context = context.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return context


def compute_indicators(price_frame: pd.DataFrame, benchmark_frame: pd.DataFrame) -> pd.DataFrame:
    frame = price_frame.sort_values("date").reset_index(drop=True).copy()
    if frame.empty:
        return frame

    frame["volume_30d_avg"] = frame["volume"].shift(1).rolling(window=30, min_periods=1).mean()
    frame["volume_ratio"] = frame["volume"] / frame["volume_30d_avg"]
    frame["sma_20"]  = frame["close"].rolling(window=20,  min_periods=20).mean()
    frame["sma_50"]  = frame["close"].rolling(window=50,  min_periods=50).mean()
    frame["sma_200"] = frame["close"].rolling(window=200, min_periods=200).mean()
    frame["ema_20"]  = frame["close"].ewm(span=20, adjust=False, min_periods=20).mean()

    delta    = frame["close"].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    rs       = avg_gain / avg_loss.replace(0, pd.NA)
    frame["rsi_14"] = 100 - (100 / (1 + rs))

    ema_12 = frame["close"].ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = frame["close"].ewm(span=26, adjust=False, min_periods=26).mean()
    frame["macd"]        = ema_12 - ema_26
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False, min_periods=9).mean()
    frame["macd_hist"]   = frame["macd"] - frame["macd_signal"]

    rolling_std    = frame["close"].rolling(window=20, min_periods=20).std()
    frame["bb_mid"]   = frame["sma_20"]
    frame["bb_upper"] = frame["bb_mid"] + (2 * rolling_std)
    frame["bb_lower"] = frame["bb_mid"] - (2 * rolling_std)

    benchmark = benchmark_frame.sort_values("date").reset_index(drop=True).copy()
    if not benchmark.empty:
        benchmark["nifty_index_close"] = benchmark["nifty_index_close"].ffill()
        benchmark["nifty_return"] = benchmark["nifty_index_close"].pct_change()
        frame = frame.merge(benchmark, on="date", how="left")
        frame["nifty_index_close"] = frame["nifty_index_close"].ffill()
        frame["company_return"]      = frame["close"].pct_change()
        frame["vs_nifty_pct"]        = (frame["company_return"] - frame["nifty_return"]) * 100
        # Cumulative outperformance is chained in build_output_rows using the last DB value.
        frame["vs_nifty_cumulative"] = pd.NA
        frame = frame.drop(columns=["company_return", "nifty_return"])
    else:
        frame["nifty_index_close"]   = pd.NA
        frame["vs_nifty_pct"]        = pd.NA
        frame["vs_nifty_cumulative"] = pd.NA

    cols_to_round = [
        "volume_30d_avg", "volume_ratio",
        "sma_20", "sma_50", "sma_200", "ema_20",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_lower", "bb_mid",
        "vs_nifty_pct", "vs_nifty_cumulative",
    ]
    for col in cols_to_round:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").round(2)

    return frame


def build_output_rows(
    engine,
    ticker: str,
    metadata: dict,
    price_frame: pd.DataFrame,
    enriched_frame: pd.DataFrame,
    last_stored_date: date | None,
) -> pd.DataFrame:
    if price_frame.empty or enriched_frame.empty:
        print(f"    ❌ build_output_rows: price_frame empty={price_frame.empty}, enriched_frame empty={enriched_frame.empty}")
        return pd.DataFrame()

    indicator_columns = [
        "date", "volume_30d_avg", "volume_ratio", "nifty_index_close",
        "sma_20", "sma_50", "sma_200", "ema_20", "rsi_14",
        "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_lower", "bb_mid",
        "vs_nifty_pct", "vs_nifty_cumulative",
    ]

    # Only keep indicator columns that actually exist in enriched_frame
    available = [c for c in indicator_columns if c in enriched_frame.columns]
    merged = price_frame.merge(enriched_frame[available], on="date", how="left")
    merged["nifty_index_close"] = merged["nifty_index_close"].ffill()

    merged["ticker"]          = ticker.replace(".NS", "")
    merged["company_name"]    = metadata["company_name"]
    merged["sector"]          = metadata["sector"]
    merged["industry"]        = metadata["industry"]

    print(f"    📋 Merged rows before date filter: {len(merged)}")
    print(f"    📋 last_stored_date={last_stored_date}")

    if last_stored_date is not None:
        before = len(merged)
        merged = merged[merged["date"] > last_stored_date]
        print(f"    📋 After date filter (> {last_stored_date}): {len(merged)} rows (was {before})")
    else:
        merged = merged.tail(ROWS_TO_STORE)
        print(f"    📋 First load — keeping last {ROWS_TO_STORE} rows: {len(merged)}")

    ticker_key = ticker.replace(".NS", "")
    last_cumulative = 0.0
    if last_stored_date is not None and inspect(engine).has_table("price_history"):
        last_cumulative_query = text("""
            SELECT vs_nifty_cumulative
            FROM price_history
            WHERE ticker = :ticker AND vs_nifty_cumulative IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
        """)
        with engine.connect() as conn:
            result = conn.execute(last_cumulative_query, {"ticker": ticker_key}).fetchone()
        if result and result[0] is not None:
            last_cumulative = float(result[0])

    pct = pd.to_numeric(merged["vs_nifty_pct"], errors="coerce")
    merged["vs_nifty_cumulative"] = (last_cumulative + pct.fillna(0).cumsum()).round(2)

    # Align latest inserted row with today's cleaned snapshot from stock_data.
    latest_idx = merged["date"].idxmax() if not merged.empty else None
    if latest_idx is not None:
        stock_volume_ratio = pd.to_numeric(metadata.get("volume_ratio"), errors="coerce")
        if pd.notna(stock_volume_ratio):
            merged.loc[latest_idx, "volume_ratio"] = round(float(stock_volume_ratio), 2)

    merged = merged.sort_values("date").reset_index(drop=True)
    # Enforce SMA nulling until sufficient historical rows exist.
    # Calculate current DB count for this ticker and mark SMA columns as NA
    try:
        existing_count = 0
        if inspect(engine).has_table("price_history"):
            with engine.connect() as conn:
                cnt = conn.execute(
                    text("SELECT COUNT(*) FROM price_history WHERE ticker = :ticker"),
                    {"ticker": ticker.replace(".NS", "")},
                ).scalar()
                existing_count = int(cnt) if cnt is not None else 0

        # overall index for each new row when appended to existing history
        merged["_overall_idx"] = (merged.reset_index().index + 1) + existing_count

        for w in (20, 50, 200):
            col = f"sma_{w}"
            if col in merged.columns:
                merged.loc[merged["_overall_idx"] < w, col] = pd.NA

        merged = merged.drop(columns=[c for c in ("_overall_idx",) if c in merged.columns])
    except Exception:
        # Non-fatal: if DB query fails, leave computed SMAs as-is
        pass
    return merged


def save_price_history(df: pd.DataFrame, engine, ticker: str) -> None:
    if df.empty:
        return

    output = df.copy()
    output["date"] = pd.to_datetime(output["date"], errors="coerce").dt.date

    if inspect(engine).has_table("price_history"):
        with engine.begin() as conn:
            for run_date in output["date"].dropna().unique():
                conn.execute(
                    text("DELETE FROM price_history WHERE ticker = :ticker AND DATE(date) = :run_date"),
                    {"ticker": ticker.replace(".NS", ""), "run_date": run_date},
                )

    output = output.reindex(columns=PRICE_HISTORY_COLUMNS)
    output.to_sql("price_history", con=engine, if_exists="append", index=False)


def process_ticker(ticker: str, engine, last_dates: dict[str, date], target_date: date) -> int:
    ticker_key      = ticker.replace(".NS", "")
    metadata        = fetch_ticker_info(engine, ticker)
    last_stored_date = last_dates.get(ticker_key)

    print(f"  → target_date={target_date} | last_stored={last_stored_date}")

    # ── CHECKPOINT 1: already have today's data ──────────────
    if last_stored_date == target_date:
        print(f"  ♻️  Already have data for {target_date} — re-fetching to refresh")
        if inspect(engine).has_table("price_history"):
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM price_history WHERE ticker = :ticker AND DATE(date) = :run_date"),
                    {"ticker": ticker_key, "run_date": target_date},
                )
        last_stored_date = target_date - timedelta(days=1)

    # ── CHECKPOINT 2: stored date is somehow in the future ───
    if last_stored_date is not None and last_stored_date > target_date:
        print(f"  ⏭️  Skipping {ticker_key} — stored date {last_stored_date} is ahead of target {target_date}")
        return 0

    # ── CHECKPOINT 3: decide fetch range ─────────────────────
    if last_stored_date is None:
        fetch_start    = target_date - timedelta(days=HISTORY_WARMUP_DAYS)
        context_frame  = pd.DataFrame(columns=["date", "close", "volume"])
        print(f"  🆕 First load — fetching from {fetch_start}")
    else:
        fetch_start    = last_stored_date + timedelta(days=1)
        context_frame  = load_context_history(engine, ticker_key)
        print(f"  🔄 Incremental — fetching from {fetch_start} | context rows: {len(context_frame)}")

    # ── CHECKPOINT 4: fetch price history ────────────────────
    price_frame = fetch_price_history(ticker, fetch_start, target_date)
    if price_frame.empty:
        print(f"  ❌ STOPPED: price_frame is empty for {ticker_key}")
        print(f"     fetch_start={fetch_start}, target_date={target_date}")
        print(f"     Check if market was open on those dates or if yfinance returned data")
        return 0

    print(f"  ✅ price_frame: {len(price_frame)} rows | {price_frame['date'].min()} → {price_frame['date'].max()}")

    # ── CHECKPOINT 5: fetch NIFTY benchmark ──────────────────
    benchmark_start = context_frame["date"].min() if not context_frame.empty else price_frame["date"].min()
    benchmark_frame = fetch_nifty_history(benchmark_start, target_date)
    print(f"  📈 benchmark_frame: {len(benchmark_frame)} rows")

    # ── CHECKPOINT 6: combine context + new prices ───────────
    close_context = (
        context_frame[["date", "close", "volume"]].copy()
        if not context_frame.empty
        else pd.DataFrame(columns=["date", "close", "volume"])
    )
    combined_prices = pd.concat(
        [close_context, price_frame[["date", "close", "volume"]]],
        ignore_index=True,
    ).drop_duplicates(subset=["date"], keep="last")
    combined_prices = combined_prices.sort_values("date").reset_index(drop=True)
    print(f"  🔗 combined_prices: {len(combined_prices)} rows")

    # ── CHECKPOINT 7: compute indicators ─────────────────────
    enriched = compute_indicators(combined_prices, benchmark_frame)
    print(f"  🧮 enriched frame: {len(enriched)} rows")

    # ── CHECKPOINT 8: build output rows ──────────────────────
    output = build_output_rows(engine, ticker, metadata, price_frame, enriched, last_stored_date)
    if output.empty:
        print(f"  ❌ STOPPED: output is empty after build_output_rows for {ticker_key}")
        return 0

    # ── CHECKPOINT 9: save ────────────────────────────────────
    print(f"  💾 Saving {len(output)} rows for {ticker_key}")
    save_price_history(output, engine, ticker)
    logging.info(f"Saved {len(output)} price history rows for {ticker_key}")
    print(f"  ✅ Saved {len(output)} price rows for {ticker_key}")
    return len(output)


def main() -> None:
    ensure_database_exists()
    engine     = get_engine()

    # run schema migration before anything else
    from db_bootstrap import backfill_vs_nifty_cumulative, clear_price_history_snapshot_columns, migrate_price_history_schema
    migrate_price_history_schema(engine)
    backfill_vs_nifty_cumulative(engine)
    clear_price_history_snapshot_columns(engine)

    tickers    = get_nifty50_tickers()

    if not tickers:
        raise RuntimeError("Could not fetch NIFTY 50 tickers")

    print(f"\n📅 Determining last trading day...")
    target_date = get_last_trading_day()
    print(f"📅 Last trading day: {target_date}")

    last_dates = load_existing_last_dates(engine)
    print(f"📦 Existing tickers in price_history: {len(last_dates)}")

    total_rows = 0

    for index, ticker in enumerate(tickers, start=1):
        print(f"\n[{index}/{len(tickers)}] ── {ticker} ──────────────────")
        total_rows += process_ticker(ticker, engine, last_dates, target_date)

    print(f"\n{'='*50}")
    print(f"Total price history rows saved: {total_rows}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()