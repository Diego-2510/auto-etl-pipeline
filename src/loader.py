from __future__ import annotations

import sqlite3

import pandas as pd


def _asset_type(
    symbol: str,
) -> str:
    suffix = (
        symbol.rsplit(
            "-",
            1,
        )[-1]
        if "-" in symbol
        else ""
    )

    return (
        "crypto"
        if suffix
        in {
            "USD",
            "EUR",
            "GBP",
        }
        else "stock"
    )


def _asset_id(
    conn: sqlite3.Connection,
    symbol: str,
) -> int:
    conn.execute(
        """
        INSERT INTO assets (
            symbol,
            asset_type
        )
        VALUES (?, ?)
        ON CONFLICT(symbol) DO NOTHING
        """,
        (
            symbol,
            _asset_type(symbol),
        ),
    )

    row = conn.execute(
        """
        SELECT id
        FROM assets
        WHERE symbol = ?
        """,
        (symbol,),
    ).fetchone()

    if row is None:
        raise sqlite3.IntegrityError(f"failed to resolve asset id for {symbol}")

    return int(row[0])


def load_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    frame: pd.DataFrame,
    source: str = "yfinance",
) -> dict[str, int]:
    if frame.empty:
        return {
            "inserted": 0,
            "skipped": 0,
        }

    asset_id = _asset_id(
        conn,
        symbol,
    )

    before = conn.total_changes

    records = [
        (
            asset_id,
            date.strftime("%Y-%m-%dT%H:%M:%S"),
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            int(row.volume),
            source,
        )
        for date, row in frame.iterrows()
    ]

    conn.executemany(
        """
        INSERT INTO price_data (
            asset_id,
            date,
            open,
            high,
            low,
            close,
            volume,
            source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(
            asset_id,
            date,
            source
        ) DO NOTHING
        """,
        records,
    )

    inserted = conn.total_changes - before

    return {
        "inserted": inserted,
        "skipped": (len(records) - inserted),
    }


def load_all(
    conn: sqlite3.Connection,
    data: dict[
        str,
        pd.DataFrame,
    ],
    source: str = "yfinance",
) -> dict[
    str,
    dict[str, int],
]:
    summary: dict[
        str,
        dict[str, int],
    ] = {}

    try:
        conn.execute("BEGIN")

        for symbol, frame in data.items():
            summary[symbol] = load_symbol(
                conn,
                symbol,
                frame,
                source,
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    return summary
