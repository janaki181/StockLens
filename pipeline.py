import logging
import subprocess
import sys
from pathlib import Path

from sqlalchemy import text

from db_bootstrap import get_engine
from db_bootstrap import setup_logging

setup_logging()


def verify_price_history_consistency() -> bool:
    """Ensure latest stock_data snapshot date exists in price_history for all tickers."""
    engine = get_engine()

    with engine.connect() as conn:
        latest_snapshot_date = conn.execute(
            text("SELECT MAX(DATE(date)) FROM stock_data")
        ).scalar()

        if latest_snapshot_date is None:
            logging.warning("Consistency check skipped: stock_data is empty")
            print("⚠️  Consistency check skipped: stock_data is empty")
            return True

        missing_rows = conn.execute(
            text(
                """
                SELECT s.ticker
                FROM stock_data s
                WHERE DATE(s.date) = :run_date
                  AND NOT EXISTS (
                      SELECT 1
                      FROM price_history p
                      WHERE p.ticker = s.ticker
                        AND DATE(p.date) = :run_date
                  )
                ORDER BY s.ticker
                """
            ),
            {"run_date": latest_snapshot_date},
        ).fetchall()

    if missing_rows:
        missing_tickers = [row[0] for row in missing_rows]
        preview = ", ".join(missing_tickers[:8])
        suffix = "..." if len(missing_tickers) > 8 else ""
        logging.error(
            f"Consistency check failed for {latest_snapshot_date}: "
            f"{len(missing_tickers)} missing tickers in price_history ({preview}{suffix})"
        )
        print(
            f"❌ Consistency check failed for {latest_snapshot_date}: "
            f"{len(missing_tickers)} tickers missing in price_history"
        )
        return False

    logging.info(f"Consistency check passed for {latest_snapshot_date}")
    print(f"✅ Consistency check passed for {latest_snapshot_date}")
    return True


def run_step(script_name: str) -> None:
    project_root = Path(__file__).resolve().parent
    module_name = Path(script_name).with_suffix("").as_posix().replace("/", ".").replace("\\", ".")

    print(f"\n=== Running {script_name} ===")
    logging.info(f"Starting {script_name}")

    completed = subprocess.run([sys.executable, "-m", module_name], cwd=project_root, check=False)
    if completed.returncode != 0:
        logging.error(f"{script_name} failed with exit code {completed.returncode}")
        raise SystemExit(completed.returncode)

    logging.info(f"Completed {script_name}")
    print(f"=== Finished {script_name} ===")


def main() -> None:
    run_step(r"data_pipeline\collect_data.py")
    run_step(r"data_pipeline\clean_data.py")
    run_step(r"data_pipeline\price_history_builder.py")

    # If latest snapshot is not in price_history for some tickers, retry indicators once.
    if not verify_price_history_consistency():
        print("↻ Retrying data_pipeline/price_history_builder.py once to recover missing price_history rows...")
        run_step(r"data_pipeline\price_history_builder.py")
        if not verify_price_history_consistency():
            logging.error("Pipeline failed: latest snapshot not fully reflected in price_history")
            raise SystemExit(1)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
