from __future__ import annotations

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
)


class TransformError(ValueError):
    """Raised when extracted data cannot be safely transformed."""


def transform(
    frame: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]

    if missing:
        raise TransformError(f"[{symbol}] missing columns: {missing}")

    if not isinstance(
        frame.index,
        pd.DatetimeIndex,
    ):
        raise TransformError(f"[{symbol}] index must be a DatetimeIndex")

    clean = frame.loc[
        :,
        REQUIRED_COLUMNS,
    ].copy()

    clean = clean[~clean.index.isna()]

    clean = clean[~clean.index.duplicated(keep="last")].sort_index()

    for column in REQUIRED_COLUMNS:
        clean[column] = pd.to_numeric(
            clean[column],
            errors="coerce",
        )

    clean = clean.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    clean = clean.dropna(subset=list(REQUIRED_COLUMNS))

    valid = (
        (clean["open"] > 0)
        & (clean["high"] > 0)
        & (clean["low"] > 0)
        & (clean["close"] > 0)
        & (clean["volume"] >= 0)
        & (
            clean["high"]
            >= clean[
                [
                    "open",
                    "close",
                    "low",
                ]
            ].max(axis=1)
        )
        & (
            clean["low"]
            <= clean[
                [
                    "open",
                    "close",
                    "high",
                ]
            ].min(axis=1)
        )
    )

    clean = clean.loc[valid].copy()

    if clean.empty:
        raise TransformError(f"[{symbol}] no valid OHLCV rows remain after validation")

    clean[
        [
            "open",
            "high",
            "low",
            "close",
        ]
    ] = clean[
        [
            "open",
            "high",
            "low",
            "close",
        ]
    ].astype(float)

    fractional_volume = clean["volume"] % 1 != 0

    if fractional_volume.any():
        raise TransformError(f"[{symbol}] volume must contain integer values")

    clean["volume"] = clean["volume"].astype("int64")

    return clean


def transform_all(
    data: dict[
        str,
        pd.DataFrame,
    ],
) -> dict[
    str,
    pd.DataFrame,
]:
    if not data:
        raise TransformError("no extracted data to transform")

    results: dict[
        str,
        pd.DataFrame,
    ] = {}

    failures: list[str] = []

    for symbol, frame in data.items():
        try:
            results[symbol] = transform(
                frame,
                symbol,
            )
        except TransformError as exc:
            failures.append(str(exc))

    if failures:
        raise TransformError("; ".join(failures))

    return results
