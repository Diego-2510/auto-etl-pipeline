from __future__ import annotations

import pandas as pd
import pytest

from src.transformer import (
    TransformError,
    transform,
)


def frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [
                10.0,
                11.0,
            ],
            "high": [
                12.0,
                12.0,
            ],
            "low": [
                9.0,
                10.0,
            ],
            "close": [
                11.0,
                11.5,
            ],
            "volume": [
                100,
                120,
            ],
        },
        index=pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
            ]
        ),
    )


def test_transform_accepts_valid_ohlcv() -> None:
    result = transform(
        frame(),
        "TEST",
    )

    assert len(result) == 2

    assert str(result["volume"].dtype) == "int64"


def test_transform_drops_invalid_ohlcv_rows() -> None:
    data = frame()

    data.loc[
        pd.Timestamp("2026-01-02"),
        "high",
    ] = 5.0

    result = transform(
        data,
        "TEST",
    )

    assert list(result.index) == [pd.Timestamp("2026-01-01")]


def test_transform_rejects_missing_column() -> None:
    with pytest.raises(
        TransformError,
        match="missing columns",
    ):
        transform(
            frame().drop(columns=["volume"]),
            "TEST",
        )


def test_transform_rejects_fractional_volume() -> None:
    data = frame()

    data["volume"] = data["volume"].astype(float)

    data.loc[
        pd.Timestamp("2026-01-01"),
        "volume",
    ] = 1.5

    with pytest.raises(
        TransformError,
        match="integer values",
    ):
        transform(
            data,
            "TEST",
        )
