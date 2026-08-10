from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

from src.database import (
    get_connection,
    init_schema,
)
from src.extractor import (
    ConfigError,
    ExtractionError,
    extract_all,
    load_config,
)
from src.loader import load_all
from src.transformer import (
    TransformError,
    transform_all,
)


def setup_logging(
    config: dict,
) -> logging.Logger:
    logging_cfg = config.get(
        "logging",
        {},
    )

    level_name = str(
        logging_cfg.get(
            "level",
            "INFO",
        )
    ).upper()

    level = getattr(
        logging,
        level_name,
        logging.INFO,
    )

    logger = logging.getLogger("auto_etl_pipeline")

    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)

    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    logger.addHandler(handler)

    log_dir = logging_cfg.get("directory")

    if log_dir:
        path = Path(str(log_dir))

        path.mkdir(
            parents=True,
            exist_ok=True,
        )

        file_handler = logging.FileHandler(path / "pipeline.log")

        file_handler.setLevel(level)

        file_handler.setFormatter(handler.formatter)

        logger.addHandler(file_handler)

    return logger


def run_pipeline(
    config_path: str | Path = "config.yaml",
) -> int:
    try:
        config = load_config(config_path)

        logger = setup_logging(config)

        logger.info("ETL pipeline started")

        raw = extract_all(config)

        clean = transform_all(raw)

        conn = get_connection(config["database"]["path"])

        try:
            init_schema(conn)

            summary = load_all(
                conn,
                clean,
                source="yfinance",
            )

        finally:
            conn.close()

        inserted = sum(item["inserted"] for item in summary.values())

        skipped = sum(item["skipped"] for item in summary.values())

        logger.info(
            "ETL pipeline completed: inserted=%s skipped=%s",
            inserted,
            skipped,
        )

        return 0

    except (
        ConfigError,
        ExtractionError,
        TransformError,
        OSError,
        sqlite3.Error,
    ) as exc:
        logging.getLogger("auto_etl_pipeline").error(
            "ETL pipeline failed: %s",
            exc,
        )

        print(
            f"error: {exc}",
            file=sys.stderr,
        )

        return 1


def parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("Run the yfinance-to-SQLite ETL pipeline."))

    parser.add_argument(
        "--config",
        default="config.yaml",
        help=("Path to YAML configuration"),
    )

    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
) -> int:
    args = parse_args(argv)

    return run_pipeline(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
