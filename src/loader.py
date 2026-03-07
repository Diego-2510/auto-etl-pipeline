"""Loader module: Insert transformed data into SQLite database."""

import sqlite3
from datetime import datetime

import pandas as pd

from src.database import get_connection, get_or_create_asset, init_schema


def _detect_asset_type(symbol: str) -> str:
    """Infer asset type from symbol format.
    
    Simple heuristic:
    - Contains '-' like BTC-USD → crypto
    - Everything else → stock
    """
    if "-" in symbol and symbol.split("-")[-1] in ("USD", "EUR", "GBP"):
        return "crypto"
    return "stock"


def load_symbol(conn: sqlite3.Connection, symbol: str, 
                df: pd.DataFrame, source: str = "yfinance") -> dict:
    """Load a single symbol's data into the database.
    
    Uses INSERT OR IGNORE to skip duplicates based on the 
    UNIQUE(asset_id, date, source) constraint.
    
    Returns stats dict with inserted/skipped counts.
    """
    asset_type = _detect_asset_type(symbol)
    asset_id = get_or_create_asset(conn, symbol, asset_type)

    rows_before = conn.execute(
        "SELECT COUNT(*) FROM price_data WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0]

    records = []
    for date, row in df.iterrows():
        records.append((
            asset_id,
            date.strftime("%Y-%m-%d"),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            int(row["volume"]),
            source,
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO price_data 
            (asset_id, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()

    rows_after = conn.execute(
        "SELECT COUNT(*) FROM price_data WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0]

    inserted = rows_after - rows_before
    skipped = len(records) - inserted

    return {"inserted": inserted, "skipped": skipped, "total": rows_after}


def load_all(conn: sqlite3.Connection, data: dict[str, pd.DataFrame],
             source: str = "yfinance") -> dict:
    """Load all transformed data into the database.
    
    Returns summary dict mapping symbol → stats.
    """
    print(f"Loading {len(data)} symbols into database...")
    summary = {}

    for symbol, df in data.items():
        try:
            stats = load_symbol(conn, symbol, df, source)
            summary[symbol] = stats
            print(f"  ✓ {symbol}: +{stats['inserted']} inserted, "
                  f"{stats['skipped']} skipped, {stats['total']} total")
        except Exception as e:
            print(f"  ✗ {symbol}: {e}")
            summary[symbol] = {"error": str(e)}

    return summary


def print_db_summary(conn: sqlite3.Connection) -> None:
    """Print a quick overview of database contents."""
    assets = conn.execute("""
        SELECT a.symbol, a.asset_type, COUNT(p.id) as rows,
               MIN(p.date) as first_date, MAX(p.date) as last_date
        FROM assets a
        LEFT JOIN price_data p ON a.id = p.asset_id
        GROUP BY a.id
        ORDER BY a.symbol
    """).fetchall()

    print(f"\n{'Symbol':<12} {'Type':<8} {'Rows':>6} {'From':>12} {'To':>12}")
    print("-" * 54)
    for row in assets:
        print(f"{row['symbol']:<12} {row['asset_type']:<8} "
              f"{row['rows']:>6} {row['first_date'] or 'n/a':>12} "
              f"{row['last_date'] or 'n/a':>12}")

    total = conn.execute("SELECT COUNT(*) FROM price_data").fetchone()[0]
    print(f"\nTotal rows in price_data: {total}")


if __name__ == "__main__":
    from src.extractor import load_config, extract_all
    from src.transformer import transform_all

    config = load_config()

    # Full ETL pipeline
    raw_data = extract_all(config)
    clean_data = transform_all(raw_data)

    db_path = config["database"]["path"]
    conn = get_connection(db_path)
    init_schema(conn)

    load_all(conn, clean_data)
    print_db_summary(conn)

    conn.close()
