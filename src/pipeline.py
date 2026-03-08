"""Pipeline module: Orchestrate ETL with structured logging."""

import logging
import sys
from datetime import datetime
from pathlib import Path

from src.database import get_connection, init_schema
from src.extractor import load_config, extract_all
from src.transformer import transform_all
from src.loader import load_all, print_db_summary


def setup_logging(config: dict) -> logging.Logger:
    """Configure file + console logging.
    
    Log format: 2026-03-08 13:25:00 | INFO | Loading 3 symbols...
    File logs go to logs/pipeline_YYYY-MM-DD.log (one per day).
    """
    log_cfg = config.get("logging", {})
    log_dir = log_cfg.get("directory", "logs/")
    log_level = log_cfg.get("level", "INFO")

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_file = Path(log_dir) / f"pipeline_{today}.log"

    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Avoid duplicate handlers on re-run
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler (append mode – keeps full history per day)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def run_pipeline(config_path: str = "config.yaml") -> None:
    """Execute the full ETL pipeline with logging."""
    config = load_config(config_path)
    logger = setup_logging(config)

    logger.info("=" * 50)
    logger.info("ETL Pipeline started")
    logger.info("=" * 50)

    # --- Extract ---
    logger.info("Phase 1/3: Extract")
    try:
        raw_data = extract_all(config)
        logger.info(f"Extracted {len(raw_data)} symbols")
    except Exception as e:
        logger.error(f"Extract failed: {e}")
        return

    # --- Transform ---
    logger.info("Phase 2/3: Transform")
    try:
        clean_data = transform_all(raw_data)
        logger.info(f"Transformed {len(clean_data)}/{len(raw_data)} symbols")
    except Exception as e:
        logger.error(f"Transform failed: {e}")
        return

    if not clean_data:
        logger.warning("No data survived transformation. Aborting load.")
        return

    # --- Load ---
    logger.info("Phase 3/3: Load")
    try:
        db_path = config["database"]["path"]
        conn = get_connection(db_path)
        init_schema(conn)

        summary = load_all(conn, clean_data)

        total_inserted = sum(
            s.get("inserted", 0) for s in summary.values() if "error" not in s
        )
        total_skipped = sum(
            s.get("skipped", 0) for s in summary.values() if "error" not in s
        )

        logger.info(f"Load complete: +{total_inserted} inserted, "
                     f"{total_skipped} skipped")

        print_db_summary(conn)
        conn.close()

    except Exception as e:
        logger.error(f"Load failed: {e}")
        return

    logger.info("ETL Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()
