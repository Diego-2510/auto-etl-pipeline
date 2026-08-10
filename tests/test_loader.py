from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

import src.loader as loader
from src.database import (
    get_connection,
    init_schema,
)
from src.loader import load_all


def test_load_all_is_idempotent_with_temporary_sqlite(
    tmp_path: Path,
) -> None:
    conn = get_connection(tmp_path / "market.db")

    init_schema(conn)

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

    first = load_all(
        conn,
        {
            "AAPL": frame,
        },
    )

    second = load_all(
        conn,
        {
            "AAPL": frame,
        },
    )

    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM price_data
        """
    ).fetchone()[0]

    conn.close()

    assert first["AAPL"] == {
        "inserted": 1,
        "skipped": 0,
    }

    assert second["AAPL"] == {
        "inserted": 0,
        "skipped": 1,
    }

    assert count == 1


def test_load_all_rolls_back_on_symbol_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = get_connection(tmp_path / "rollback.db")

    init_schema(conn)

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

    original = loader.load_symbol

    calls = 0

    def sometimes_fail(
        conn_arg,
        symbol,
        frame_arg,
        source="yfinance",
    ):
        nonlocal calls

        calls += 1

        if calls == 2:
            raise sqlite3.IntegrityError("boom")

        return original(
            conn_arg,
            symbol,
            frame_arg,
            source,
        )

    monkeypatch.setattr(
        loader,
        "load_symbol",
        sometimes_fail,
    )

    with pytest.raises(sqlite3.IntegrityError):
        loader.load_all(
            conn,
            {
                "AAPL": frame,
                "MSFT": frame,
            },
        )

    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM price_data
        """
    ).fetchone()[0]

    conn.close()

    assert count == 0
