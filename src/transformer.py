"""Transformer module: Validate and clean extracted market data."""

import pandas as pd


def validate_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Ensure all required OHLCV columns exist.
    
    Raises ValueError if a critical column is missing entirely.
    """
    required = ["open", "high", "low", "close", "volume"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"[{symbol}] Missing columns: {missing}")

    return df


def clean_dates(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Validate and clean the DatetimeIndex.
    
    Steps:
    1. Ensure index is DatetimeIndex
    2. Remove rows with NaT (invalid dates)
    3. Drop duplicate dates (keep first occurrence)
    4. Sort chronologically
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(f"[{symbol}] Index must be DatetimeIndex, got {type(df.index)}")

    nat_count = df.index.isna().sum()
    if nat_count > 0:
        print(f"  [{symbol}] Dropping {nat_count} rows with invalid dates")
        df = df[df.index.notna()]

    dup_count = df.index.duplicated().sum()
    if dup_count > 0:
        print(f"  [{symbol}] Dropping {dup_count} duplicate dates")
        df = df[~df.index.duplicated(keep="first")]

    return df.sort_index()


def clean_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Clean OHLCV values using forward-fill, then drop remaining NaNs.
    
    Strategy:
    - Forward-fill: Use previous day's value (common in market data 
      where missing = no change, e.g. holidays)
    - Drop: Any row still NaN after ffill has no valid predecessor → remove
    
    This is more conservative than interpolation and avoids 
    introducing synthetic data points.
    """
    price_cols = ["open", "high", "low", "close"]

    # Count NaNs before cleaning
    nan_before = df[price_cols + ["volume"]].isna().sum().sum()

    if nan_before > 0:
        print(f"  [{symbol}] Found {nan_before} NaN values, applying forward-fill")

    # Forward-fill prices (previous close is best estimate)
    df[price_cols] = df[price_cols].ffill()

    # Volume NaN → 0 (no trades is valid, unlike price)
    df["volume"] = df["volume"].fillna(0)

    # Drop rows where ffill couldn't help (first row was NaN)
    remaining_nans = df[price_cols].isna().sum().sum()
    if remaining_nans > 0:
        print(f"  [{symbol}] Dropping {remaining_nans} rows with unfillable NaNs")
        df = df.dropna(subset=price_cols)

    return df


def validate_ohlcv_integrity(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Check OHLCV logical consistency and remove invalid rows.
    
    Rules:
    - Prices must be > 0
    - High must be >= Low
    - High must be >= Open and Close
    - Low must be <= Open and Close
    - Volume must be >= 0
    """
    initial_len = len(df)

    # Positive prices
    price_mask = (
        (df["open"] > 0) & 
        (df["high"] > 0) & 
        (df["low"] > 0) & 
        (df["close"] > 0)
    )

    # OHLC consistency
    consistency_mask = (
        (df["high"] >= df["low"]) &
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    )

    # Volume non-negative
    volume_mask = df["volume"] >= 0

    valid_mask = price_mask & consistency_mask & volume_mask
    df = df[valid_mask]

    removed = initial_len - len(df)
    if removed > 0:
        print(f"  [{symbol}] Removed {removed} rows failing integrity checks")

    return df


def enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to match database schema types."""
    df = df.copy()
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(int)
    return df


def transform(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Run full transformation pipeline on a single symbol's data.
    
    Pipeline order matters:
    1. Column check  → fail fast if data is structurally wrong
    2. Date cleaning → valid index before any row operations
    3. NaN handling  → fill/drop before integrity checks
    4. Integrity     → validate business rules on clean data
    5. Type casting  → final step before DB insertion
    
    Returns cleaned DataFrame ready for loading.
    """
    print(f"  Transforming {symbol} ({len(df)} rows)")

    df = validate_columns(df, symbol)
    df = clean_dates(df, symbol)
    df = clean_ohlcv(df, symbol)
    df = validate_ohlcv_integrity(df, symbol)
    df = enforce_types(df)

    print(f"  ✓ {symbol}: {len(df)} rows after transform")
    return df


def transform_all(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform all extracted symbol data.
    
    Returns dict with only successfully transformed symbols.
    """
    results = {}
    print(f"Transforming {len(data)} symbols...")

    for symbol, df in data.items():
        try:
            results[symbol] = transform(df, symbol)
        except (ValueError, TypeError) as e:
            print(f"  ✗ {symbol}: {e}")

    return results


if __name__ == "__main__":
    from src.extractor import load_config, extract_all

    config = load_config()
    raw_data = extract_all(config)
    clean_data = transform_all(raw_data)

    print(f"\nTransformed {len(clean_data)}/{len(raw_data)} symbols:")
    for symbol, df in clean_data.items():
        print(f"  {symbol}: {df.shape[0]} rows, "
              f"dtypes: { {c: str(df[c].dtype) for c in ['close', 'volume']} }")
