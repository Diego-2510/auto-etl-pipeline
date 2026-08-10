from __future__ import annotations

import sqlite3
from pathlib import Path


def get_connection(
    db_path: str | Path,
) -> sqlite3.Connection:
    path = Path(db_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    conn = sqlite3.connect(path)

    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute("PRAGMA journal_mode = WAL")

    conn.row_factory = sqlite3.Row

    return conn


def init_schema(
    conn: sqlite3.Connection,
) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            asset_type TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL REFERENCES assets(id),
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(asset_id, date, source)
        );

        CREATE INDEX IF NOT EXISTS idx_price_data_asset_date
        ON price_data(asset_id, date);
        """
    )

    conn.commit()
