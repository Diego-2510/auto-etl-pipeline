from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


class ConfigError(ValueError):
    """Raised when pipeline configuration is invalid."""


class ExtractionError(RuntimeError):
    """Raised when extraction fails."""


def load_config(
    config_path: str | Path = "config.yaml",
) -> dict[str, Any]:
    path = Path(config_path)

    if not path.is_file():
        raise ConfigError(f"config not found: {path}")

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc

    if not isinstance(
        data,
        dict,
    ):
        raise ConfigError("config root must be a mapping")

    validate_config(data)

    return data


def validate_config(
    config: dict[str, Any],
) -> None:
    extract = config.get("extract")
    database = config.get("database")

    if not isinstance(
        extract,
        dict,
    ):
        raise ConfigError("missing 'extract' mapping")

    if not isinstance(
        database,
        dict,
    ):
        raise ConfigError("missing 'database' mapping")

    symbols = extract.get("symbols")

    if (
        not isinstance(symbols, list)
        or not symbols
        or not all(isinstance(symbol, str) and symbol.strip() for symbol in symbols)
    ):
        raise ConfigError("extract.symbols must be a non-empty list of strings")

    if len(set(symbols)) != len(symbols):
        raise ConfigError("extract.symbols must not contain duplicates")

    source = extract.get(
        "source",
        "yfinance",
    )

    if source != "yfinance":
        raise ConfigError("extract.source must be 'yfinance'; no other source is implemented")

    db_path = database.get("path")

    if not isinstance(db_path, str) or not db_path.strip():
        raise ConfigError("database.path must be a non-empty string")


def _cache_path(
    symbol: str,
    cache_dir: str | Path,
    interval: str,
) -> Path:
    safe_symbol = symbol.replace("/", "-").replace(":", "-")

    safe_interval = interval.replace("/", "-").replace(":", "-")

    return Path(cache_dir) / f"{safe_symbol}_{safe_interval}.csv"


def _cache_is_valid(
    path: Path,
    max_age_hours: float,
) -> bool:
    if max_age_hours < 0:
        raise ConfigError("cache.max_age_hours must be non-negative")

    if not path.is_file():
        return False

    age_seconds = time.time() - path.stat().st_mtime

    return age_seconds <= max_age_hours * 3600


def _fetch_from_api(
    symbol: str,
    period: str,
    interval: str,
    attempts: int = 3,
) -> pd.DataFrame:
    import yfinance as yf

    last_error: Exception | None = None

    for attempt in range(attempts):
        try:
            frame = yf.Ticker(symbol).history(
                period=period,
                interval=interval,
            )

            if frame.empty:
                raise ValueError("provider returned no rows")

            frame = frame.copy()

            if (
                isinstance(
                    frame.index,
                    pd.DatetimeIndex,
                )
                and frame.index.tz is not None
            ):
                frame.index = frame.index.tz_localize(None)

            frame.index.name = "date"

            frame.columns = [str(column).lower().replace(" ", "_") for column in frame.columns]

            required = [
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]

            missing = [column for column in required if column not in frame.columns]

            if missing:
                raise ValueError(f"provider response missing columns: {missing}")

            return frame[required]

        except Exception as exc:
            last_error = exc

            if attempt + 1 < attempts:
                time.sleep(2**attempt)

    raise ExtractionError(f"failed to fetch {symbol} after {attempts} attempts: {last_error}")


def extract_symbol(
    symbol: str,
    config: dict[str, Any],
) -> pd.DataFrame:
    extract = config["extract"]
    cache = config.get(
        "cache",
        {},
    )

    interval = str(
        extract.get(
            "interval",
            "1d",
        )
    )

    cache_enabled = bool(
        cache.get(
            "enabled",
            True,
        )
    )

    cache_dir = Path(
        str(
            cache.get(
                "directory",
                "data",
            )
        )
    )

    cache_path = _cache_path(
        symbol,
        cache_dir,
        interval,
    )

    if cache_enabled and _cache_is_valid(
        cache_path,
        float(
            cache.get(
                "max_age_hours",
                24,
            )
        ),
    ):
        try:
            frame = pd.read_csv(
                cache_path,
                index_col=0,
                parse_dates=True,
            )
        except (
            OSError,
            pd.errors.ParserError,
        ) as exc:
            raise ExtractionError(f"failed to read cache for {symbol}: {exc}") from exc

        if frame.empty:
            raise ExtractionError(f"cache for {symbol} is empty")

        return frame

    frame = _fetch_from_api(
        symbol,
        str(
            extract.get(
                "period",
                "1y",
            )
        ),
        interval,
    )

    if cache_enabled:
        cache_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        frame.to_csv(cache_path)

    return frame


def extract_all(
    config: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    results: dict[
        str,
        pd.DataFrame,
    ] = {}

    failures: list[str] = []

    for symbol in config["extract"]["symbols"]:
        try:
            results[symbol] = extract_symbol(
                symbol,
                config,
            )
        except ExtractionError as exc:
            failures.append(str(exc))

    if failures:
        raise ExtractionError("; ".join(failures))

    return results
