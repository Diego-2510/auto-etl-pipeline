from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src import extractor
from src.extractor import (
    ConfigError,
    validate_config,
)


def base_config() -> dict:
    return {
        "database": {
            "path": "data/test.db",
        },
        "extract": {
            "source": "yfinance",
            "symbols": [
                "AAPL",
            ],
        },
    }


def test_validate_config_rejects_unimplemented_source() -> None:
    config = base_config()

    config["extract"]["source"] = "ccxt"

    with pytest.raises(
        ConfigError,
        match="yfinance",
    ):
        validate_config(config)


def test_validate_config_rejects_duplicate_symbols() -> None:
    config = base_config()

    config["extract"]["symbols"] = [
        "AAPL",
        "AAPL",
    ]

    with pytest.raises(
        ConfigError,
        match="duplicates",
    ):
        validate_config(config)


def test_cache_path_includes_interval(
    tmp_path: Path,
) -> None:
    assert (
        extractor._cache_path(
            "BTC/USD",
            tmp_path,
            "1h",
        ).name
        == "BTC-USD_1h.csv"
    )


def test_cache_validity_handles_missing_and_fresh_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cache.csv"

    assert not (
        extractor._cache_is_valid(
            path,
            24,
        )
    )

    path.write_text(
        "x",
        encoding="utf-8",
    )

    assert extractor._cache_is_valid(
        path,
        24,
    )

    with pytest.raises(
        ConfigError,
        match="non-negative",
    ):
        extractor._cache_is_valid(
            path,
            -1,
        )


def test_extract_symbol_reads_valid_cache(
    tmp_path: Path,
) -> None:
    config = base_config()

    config["extract"]["interval"] = "1d"

    config["cache"] = {
        "enabled": True,
        "directory": str(tmp_path),
        "max_age_hours": 24,
    }

    path = extractor._cache_path(
        "AAPL",
        tmp_path,
        "1d",
    )

    pd.DataFrame(
        {
            "open": [1],
            "high": [2],
            "low": [1],
            "close": [2],
            "volume": [3],
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
            ]
        ),
    ).to_csv(path)

    result = extractor.extract_symbol(
        "AAPL",
        config,
    )

    assert len(result) == 1


def test_extract_all_propagates_symbol_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = base_config()

    def fail(
        symbol: str,
        config: dict,
    ) -> pd.DataFrame:
        del config

        raise (extractor.ExtractionError(f"boom {symbol}"))

    monkeypatch.setattr(
        extractor,
        "extract_symbol",
        fail,
    )

    with pytest.raises(
        extractor.ExtractionError,
        match="boom AAPL",
    ):
        extractor.extract_all(config)


def test_load_config_reads_valid_yaml(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yaml"

    path.write_text(
        "database:\n  path: data/test.db\nextract:\n  source: yfinance\n  symbols: [AAPL]\n",
        encoding="utf-8",
    )

    config = extractor.load_config(path)

    assert config["extract"]["symbols"] == ["AAPL"]


def test_load_config_rejects_missing_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ConfigError,
        match="config not found",
    ):
        extractor.load_config(tmp_path / "missing.yaml")


def test_extract_symbol_fetches_and_caches_when_cache_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = base_config()

    config["extract"].update(
        {
            "period": "1mo",
            "interval": "1d",
        }
    )

    config["cache"] = {
        "enabled": True,
        "directory": str(tmp_path),
        "max_age_hours": 24,
    }

    frame = pd.DataFrame(
        {
            "open": [1.0],
            "high": [2.0],
            "low": [1.0],
            "close": [2.0],
            "volume": [3],
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
            ]
        ),
    )

    monkeypatch.setattr(
        extractor,
        "_fetch_from_api",
        lambda *args, **kwargs: frame,
    )

    result = extractor.extract_symbol(
        "AAPL",
        config,
    )

    assert len(result) == 1

    assert extractor._cache_path(
        "AAPL",
        tmp_path,
        "1d",
    ).exists()


def test_validate_config_rejects_missing_sections() -> None:
    with pytest.raises(
        ConfigError,
        match="extract",
    ):
        validate_config({"database": {"path": "x.db"}})

    with pytest.raises(
        ConfigError,
        match="database",
    ):
        validate_config(
            {
                "extract": {
                    "source": ("yfinance"),
                    "symbols": ["AAPL"],
                }
            }
        )
