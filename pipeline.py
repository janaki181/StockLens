import logging
import subprocess
import sys
from pathlib import Path

from db_bootstrap import setup_logging

setup_logging()


def run_step(script_name: str) -> None:
    project_root = Path(__file__).resolve().parent
    script_path = project_root / script_name

    print(f"\n=== Running {script_name} ===")
    logging.info(f"Starting {script_name}")

    completed = subprocess.run([sys.executable, str(script_path)], check=False)
    if completed.returncode != 0:
        logging.error(f"{script_name} failed with exit code {completed.returncode}")
        raise SystemExit(completed.returncode)

    logging.info(f"Completed {script_name}")
    print(f"=== Finished {script_name} ===")


def main() -> None:
    run_step("collect_data.py")
    run_step("clean_data.py")
    run_step("indicators.py")
    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
