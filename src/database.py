"""Database module: Schema definition and connection management."""

import sqlite3
from pathlib import Path


def get_connection(db_path: str) -> sqlite3.Connection:
    """Create or connect to SQLite database.
    
    Uses WAL mode for better concurrent read performance.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Initialize database schema.
    
    Two normalized tables:
    - assets: Master data (symbol, type)
    - price_data: OHLCV time series linked to assets
    
    UNIQUE constraint on (asset_id, date, source) prevents duplicates
    without requiring application-level checks on INSERT.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS assets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL UNIQUE,
            asset_type  TEXT NOT NULL DEFAULT 'stock',
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id    INTEGER NOT NULL,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            source      TEXT NOT NULL DEFAULT 'yfinance',
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id),
            UNIQUE(asset_id, date, source)
        );

        CREATE INDEX IF NOT EXISTS idx_price_data_asset_date 
            ON price_data(asset_id, date);
    """)
    conn.commit()


def get_or_create_asset(conn: sqlite3.Connection, symbol: str, 
                         asset_type: str = "stock") -> int:
    """Return asset ID, creating the asset if it doesn't exist."""
    cursor = conn.execute(
        "SELECT id FROM assets WHERE symbol = ?", (symbol,)
    )
    row = cursor.fetchone()

    if row:
        return row["id"]

    cursor = conn.execute(
        "INSERT INTO assets (symbol, asset_type) VALUES (?, ?)",
        (symbol, asset_type)
    )
    conn.commit()
    return cursor.lastrowid


if __name__ == "__main__":
    conn = get_connection("data/market_data.db")
    init_schema(conn)
    print("Schema initialized successfully.")
    
    # Quick verification
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    tables = [row["name"] for row in cursor.fetchall()]
    print(f"Tables: {tables}")
    conn.close()
