import logging

import pandas as pd
from sqlalchemy import inspect, text

from db_bootstrap import ensure_database_exists, get_engine, migrate_stock_table_schema, setup_logging

setup_logging()


NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "volume_30d_avg",
    "volume_ratio",
    "market_cap_cr",
    "pe_ratio",
    "roe_pr",
    "profit_margin_pr",
    "debt_to_equity",
    "week52_high",
    "week52_low",
    "qoq_revenue_growth_pr",
]

SECTOR_MEDIAN_COLUMNS = ["roe_pr", "debt_to_equity", "qoq_revenue_growth_pr"]

LEGACY_COLUMN_RENAMES = {
    "market_cap": "market_cap_cr",
    "roe": "roe_pr",
    "profit_margin": "profit_margin_pr",
    "qoq_revenue_growth": "qoq_revenue_growth_pr",
}

def load_latest_raw_data(engine) -> pd.DataFrame:
    if not inspect(engine).has_table("raw_stock_data"):
        return pd.DataFrame()

    raw_df = pd.read_sql_query(
        text(
            """
            SELECT *
            FROM raw_stock_data
            WHERE DATE(date) = (SELECT MAX(DATE(date)) FROM raw_stock_data)
            """
        ),
        con=engine,
    )

    for old_name, new_name in LEGACY_COLUMN_RENAMES.items():
        if old_name in raw_df.columns and new_name not in raw_df.columns:
            raw_df = raw_df.rename(columns={old_name: new_name})

    return raw_df


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["ticker"] = cleaned["ticker"].astype("string").str.upper().str.strip()
    cleaned["company_name"] = cleaned["company_name"].astype("string").str.strip()
    cleaned["sector"] = cleaned["sector"].astype("string").str.strip()
    cleaned["industry"] = cleaned["industry"].astype("string").str.strip()

    for column in NUMERIC_COLUMNS:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned = cleaned[cleaned["close"].notna() & (cleaned["close"] > 0)]
    cleaned = cleaned[cleaned["date"].notna()]
    cleaned = cleaned[cleaned["ticker"].notna() & (cleaned["ticker"] != "")]

    return cleaned.reset_index(drop=True)


def fill_sector_medians(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    for column in SECTOR_MEDIAN_COLUMNS:
        if column not in cleaned.columns:
            continue

        sector_median = cleaned.groupby("sector")[column].transform("median")
        cleaned[column] = cleaned[column].fillna(sector_median)

    return cleaned


def add_revenue_signal(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    def classify_revenue_growth(value):
        if pd.isna(value):
            return "UNKNOWN"
        if value > 0:
            return "GROWING"
        if value == 0:
            return "FLAT"
        return "DECLINING"

    cleaned["revenue_signal"] = cleaned["qoq_revenue_growth_pr"].apply(classify_revenue_growth)

    return cleaned


def clean_stock_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_types(raw_df)
    cleaned = fill_sector_medians(cleaned)
    cleaned = add_revenue_signal(cleaned)
    return cleaned


def save_clean_data(df: pd.DataFrame, engine) -> None:
    if df.empty:
        logging.warning("No cleaned rows to save")
        return

    migrate_stock_table_schema(engine)

    run_dates = [pd.to_datetime(value).date() for value in df["date"].dropna().unique()]
    if inspect(engine).has_table("stock_data"):
        with engine.begin() as conn:
            for run_date in run_dates:
                conn.execute(
                    text("DELETE FROM stock_data WHERE DATE(date) = :run_date"),
                    {"run_date": run_date},
                )

    df.to_sql("stock_data", con=engine, if_exists="append", index=False)
    if len(run_dates) == 1:
        logging.info(f"Saved {len(df)} cleaned rows to stock_data for {run_dates[0]}")
        print(f"✅ Saved {len(df)} cleaned rows for {run_dates[0]} to PostgreSQL table stock_data")
    else:
        logging.info(f"Saved {len(df)} cleaned rows to stock_data for {len(run_dates)} dates")
        print(f"✅ Saved {len(df)} cleaned rows to PostgreSQL table stock_data")


def main() -> None:
    ensure_database_exists()
    engine = get_engine()
    raw_df = load_latest_raw_data(engine)

    if raw_df.empty:
        print("No rows found in raw_stock_data.")
        return

    cleaned_df = clean_stock_data(raw_df)
    save_clean_data(cleaned_df, engine)

    print("\n── CLEAN PREVIEW ──")
    preview_columns = [
        "company_name",
        "sector",
        "close",
        "market_cap_cr",
        "volume_ratio",
        "pe_ratio",
        "revenue_signal",
        "roe_pr",
        "debt_to_equity",
        "qoq_revenue_growth_pr",
    ]
    available_columns = [column for column in preview_columns if column in cleaned_df.columns]
    print(cleaned_df[available_columns].to_string(index=False))
    print(f"\nTotal cleaned rows : {len(cleaned_df)}")
    print(f"\nNull counts after clean:\n{cleaned_df.isnull().sum()}")


if __name__ == "__main__":
    main()
