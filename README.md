# auto-etl-pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

Production-style ETL pipeline for financial market data. Extracts OHLCV data via yfinance with CSV caching, validates and transforms with Pandas, and loads into a normalized SQLite database. Cron-automated with structured logging.

## Pipeline Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Extractor  │────▶│  Transformer│────▶│   Loader    │
│             │     │              │     │             │
│ yfinance API│     │ Validation   │     │ SQLite DB   │
│ CSV Cache   │     │ NaN Handling │     │Deduplication│
│ Exp Backoff │     │ Type Checks  │     │INSERT IGNORE│
└─────────────┘     └──────────────┘     └─────────────┘
        │                                        │
   data/*.csv                            data/market_data.db
```

## Features

- **Extract**: yfinance API client with exponential backoff retry (3 attempts, 1s/2s delay) and time-based CSV caching
- **Transform**: Column validation, NaN forward-fill, OHLCV integrity checks (High ≥ Low, prices > 0), type enforcement
- **Load**: Normalized schema (assets + price_data), `INSERT OR IGNORE` duplicate handling via UNIQUE constraint
- **Automate**: Cron-compatible bash runner, structured file logging (one log per day)
- **Config-driven**: YAML configuration with example template, no credentials in repo

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| Data Source | yfinance (Yahoo Finance API) |
| Database | SQLite with WAL mode |
| Data Processing | Pandas |
| Config | PyYAML |
| Scheduling | Cron + Bash |
| Logging | Python `logging` (file + console) |

## Project Structure

```
auto-etl-pipeline/
├── src/
│   ├── __init__.py
│   ├── database.py       # Schema, connection, asset management
│   ├── extractor.py      # API client, CSV caching, retry logic
│   ├── transformer.py    # Validation, cleaning, type enforcement
│   ├── loader.py         # DB insertion, duplicate handling
│   └── pipeline.py       # ETL orchestrator with logging
├── scripts/
│   └── run_pipeline.sh   # Cron-compatible runner
├── config_example.yaml   # Template config (no credentials)
├── requirements.txt
├── LICENSE
└── README.md
```

## Quick Start

```bash
# Clone
git clone https://github.com/Diego-2510/auto-etl-pipeline.git
cd auto-etl-pipeline

# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp config_example.yaml config.yaml

# Run
python -m src.pipeline
```

### Expected Output

```
2026-03-08 13:27:52 | INFO     | ETL Pipeline started
2026-03-08 13:27:52 | INFO     | Phase 1/3: Extract
2026-03-08 13:27:52 | INFO     | Extracted 3 symbols
2026-03-08 13:27:52 | INFO     | Phase 2/3: Transform
2026-03-08 13:27:52 | INFO     | Transformed 3/3 symbols
2026-03-08 13:27:52 | INFO     | Phase 3/3: Load
2026-03-08 13:27:52 | INFO     | Load complete: +868 inserted, 0 skipped
2026-03-08 13:27:52 | INFO     | ETL Pipeline finished successfully
```

## Cron Setup

Schedule daily runs after US market close (23:00 CET):

```bash
crontab -e
# Add:
0 23 * * * /path/to/auto-etl-pipeline/scripts/run_pipeline.sh >> /path/to/auto-etl-pipeline/logs/cron.log 2>&1
```

## SQL Query Examples

After running the pipeline, query the database directly:

```sql
-- Recent AAPL closing prices
SELECT p.date, p.close, p.volume
FROM price_data p
JOIN assets a ON p.asset_id = a.id
WHERE a.symbol = 'AAPL'
ORDER BY p.date DESC
LIMIT 5;

-- All assets with row counts
SELECT a.symbol, a.asset_type, COUNT(p.id) as rows,
       MIN(p.date) as first_date, MAX(p.date) as last_date
FROM assets a
LEFT JOIN price_data p ON a.id = p.asset_id
GROUP BY a.id;

-- Daily return for a symbol
SELECT date, close,
       ROUND((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date) * 100, 2) as return_pct
FROM price_data
WHERE asset_id = (SELECT id FROM assets WHERE symbol = 'AAPL')
ORDER BY date DESC
LIMIT 10;
```

Run queries via CLI:
```bash
sqlite3 data/market_data.db "SELECT a.symbol, COUNT(*) FROM assets a JOIN price_data p ON a.id = p.asset_id GROUP BY a.id;"
```

## Design Rationale

**Normalized Schema** — Assets and price data are separated (2NF) to avoid repeating symbol/type metadata across thousands of OHLCV rows. A UNIQUE constraint on `(asset_id, date, source)` provides database-level deduplication.

**Forward-Fill over Interpolation** — Missing values are filled using the previous day's price rather than interpolation. Interpolation creates synthetic data points that never existed, which would distort downstream analysis (e.g., backtesting). Forward-fill is the conservative, industry-standard approach.

**CSV Cache Layer** — Reduces API calls during development and provides fallback data if the API is temporarily unavailable. Cache age is configurable via `max_age_hours`.

**INSERT OR IGNORE** — Chosen over INSERT OR REPLACE because historical OHLCV data does not change after market close. Idempotent re-runs are safe without risk of overwriting valid data.

## Limitations

- **Daily data only**: Intraday intervals (1m, 5m) are not tested with the current caching strategy
- **yfinance dependency**: Relies on Yahoo Finance unofficial API; may break on upstream changes
- **Single-threaded**: Symbols are fetched sequentially; parallel extraction could improve performance
- **No alerting**: Pipeline failures are logged but do not trigger notifications

## License

[MIT](LICENSE)