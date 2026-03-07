"""Extractor module: Fetch market data from APIs with CSV caching."""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
import yfinance as yf


def load_config(config_path: str = "config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config not found: {path}. Copy config_example.yaml to config.yaml"
        )
    with open(path) as f:
        return yaml.safe_load(f)


def _cache_path(symbol: str, cache_dir: str) -> Path:
    """Generate standardized cache file path for a symbol.
    
    Convention: data/AAPL_1d.csv  (lowercase-safe symbol + interval)
    """
    safe_symbol = symbol.replace("/", "-").replace(":", "-")
    return Path(cache_dir) / f"{safe_symbol}_1d.csv"


def _cache_is_valid(path: Path, max_age_hours: int) -> bool:
    """Check if cached file exists and is younger than max_age_hours."""
    if not path.exists():
        return False
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - modified < timedelta(hours=max_age_hours)


def _fetch_from_api(symbol: str, period: str, interval: str) -> pd.DataFrame:
    """Fetch OHLCV data from yfinance with basic retry logic.
    
    Exponential backoff: wait 1s, then 2s on retry.
    Max 3 attempts per symbol.
    """
    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)

            if df.empty:
                raise ValueError(f"No data returned for {symbol}")

            # Strip timezone - daily OHLCV doesn't need it
            df.index = df.index.tz_localize(None)
            
            df.index.name = "date"
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            # Keep only OHLCV columns
            ohlcv_cols = ["open", "high", "low", "close", "volume"]
            df = df[[c for c in ohlcv_cols if c in df.columns]]

            return df

        except Exception as e:
            if attempt < 2:
                wait = 2 ** attempt  # 1s, 2s
                print(f"  Retry {attempt + 1}/2 for {symbol} in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to fetch {symbol} after 3 attempts: {e}"
                )


def extract_symbol(symbol: str, config: dict) -> pd.DataFrame:
    """Extract data for a single symbol: cache-first, API-fallback.
    
    Returns DataFrame with DatetimeIndex and OHLCV columns.
    """
    cache_cfg = config.get("cache", {})
    extract_cfg = config.get("extract", {})

    cache_dir = cache_cfg.get("directory", "data/")
    max_age = cache_cfg.get("max_age_hours", 24)
    cache_enabled = cache_cfg.get("enabled", True)

    path = _cache_path(symbol, cache_dir)

    # Try cache first
    if cache_enabled and _cache_is_valid(path, max_age):
        print(f"  [CACHE HIT] {symbol} → {path}")
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df

    # Fetch from API
    print(f"  [API FETCH] {symbol}")
    df = _fetch_from_api(
        symbol=symbol,
        period=extract_cfg.get("period", "1y"),
        interval=extract_cfg.get("interval", "1d"),
    )

    # Write to cache
    if cache_enabled:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        df.to_csv(path)
        print(f"  [CACHED] {symbol} → {path}")

    return df


def extract_all(config: dict) -> dict[str, pd.DataFrame]:
    """Extract data for all symbols defined in config.
    
    Returns dict mapping symbol → DataFrame.
    """
    symbols = config.get("extract", {}).get("symbols", [])

    if not symbols:
        raise ValueError("No symbols defined in config.extract.symbols")

    results = {}
    print(f"Extracting {len(symbols)} symbols...")

    for symbol in symbols:
        try:
            results[symbol] = extract_symbol(symbol, config)
            print(f"  ✓ {symbol}: {len(results[symbol])} rows")
        except RuntimeError as e:
            print(f"  ✗ {symbol}: {e}")

    return results


if __name__ == "__main__":
    config = load_config()
    data = extract_all(config)

    print(f"\nExtracted {len(data)} symbols:")
    for symbol, df in data.items():
        print(f"  {symbol}: {df.shape[0]} rows, "
              f"{df.index.min().date()} → {df.index.max().date()}")
