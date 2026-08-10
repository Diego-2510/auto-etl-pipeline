from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import src.pipeline as pipeline
from src.pipeline import run_pipeline


def test_run_pipeline_returns_nonzero_for_missing_config(
    tmp_path: Path,
) -> None:
    assert run_pipeline(tmp_path / "missing.yaml") == 1


def test_run_pipeline_success_with_mocked_extract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "config.yaml"

    config.write_text(
        "database:\n"
        f"  path: '{tmp_path / 'market.db'}'\n"
        "extract:\n"
        "  source: yfinance\n"
        "  symbols: [AAPL]\n"
        "logging:\n"
        "  level: INFO\n",
        encoding="utf-8",
    )

    frame = pd.DataFrame(
        {
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [11.0],
            "volume": [100],
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
            ]
        ),
    )

    monkeypatch.setattr(
        pipeline,
        "extract_all",
        lambda _: {"AAPL": frame},
    )

    assert pipeline.run_pipeline(config) == 0


def test_run_pipeline_returns_nonzero_on_extraction_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "config.yaml"

    config.write_text(
        "database:\n"
        f"  path: '{tmp_path / 'market.db'}'\n"
        "extract:\n"
        "  source: yfinance\n"
        "  symbols: [AAPL]\n",
        encoding="utf-8",
    )

    def fail(
        _: dict,
    ) -> dict:
        raise (pipeline.ExtractionError("provider unavailable"))

    monkeypatch.setattr(
        pipeline,
        "extract_all",
        fail,
    )

    assert pipeline.run_pipeline(config) == 1


def test_parse_args_accepts_custom_config() -> None:
    args = pipeline.parse_args(
        [
            "--config",
            "custom.yaml",
        ]
    )

    assert args.config == "custom.yaml"
