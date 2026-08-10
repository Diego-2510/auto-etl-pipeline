# Auto ETL Pipeline

A small, reproducible batch ETL pipeline that extracts daily or intraday market data from **yfinance**, validates OHLCV records, and loads them idempotently into SQLite.

The repository is intentionally narrow in scope. It demonstrates deterministic batch ingestion, validation, retry behavior, transactional loading, duplicate protection, CLI exit codes, and testable failure handling without claiming streaming or multi-provider capabilities that are not implemented.

## Current Scope

Implemented data source:

- `yfinance`

Storage:

- SQLite

Pipeline stages:

1. Extract market data for configured symbols
2. Reuse a fresh local CSV cache when enabled
3. Validate and normalize OHLCV records
4. Load all transformed symbols in one SQLite transaction
5. Ignore already-loaded `(asset, timestamp, source)` rows
6. Return a non-zero process exit code on configuration, extraction, transformation, filesystem, or database failure

`ccxt` is intentionally not listed as supported because it is not implemented in this repository.

## Architecture

```text
config.yaml
    |
    v
Extractor ----> CSV cache
    |
    v
Transformer
    |
    v
Transactional Loader
    |
    v
SQLite
```

The pipeline separates extraction, transformation, persistence, and orchestration so each behavior can be tested independently.

## Quick Start

```bash
git clone https://github.com/Diego-2510/auto-etl-pipeline.git
cd auto-etl-pipeline

python -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

cp config_example.yaml config.yaml
python -m src.pipeline --config config.yaml
```

A successful run returns exit code `0`.

Expected failures return a non-zero exit code, which makes the command suitable for cron or another scheduler that relies on process status.

## Configuration

Example:

```yaml
database:
  path: "data/market_data.db"

extract:
  source: "yfinance"
  symbols:
    - "AAPL"
    - "MSFT"
    - "BTC-USD"
  period: "1y"
  interval: "1d"

cache:
  enabled: true
  directory: "data/cache"
  max_age_hours: 24

logging:
  level: "INFO"
  directory: "logs"
```

`extract.source` currently accepts only `yfinance`.

## Data Validation

The transformation stage requires:

- a `DatetimeIndex`
- `open`, `high`, `low`, `close`, and `volume`
- finite numeric OHLCV values
- strictly positive prices
- non-negative integral volume
- `high >= max(open, close, low)`
- `low <= min(open, close, high)`

Invalid rows are removed when row-level integrity checks fail.

If no valid rows remain for a symbol, the transformation fails rather than silently loading an empty result.

Missing OHLCV values are not forward-filled. This avoids inventing market observations in the cleanup layer.

## Idempotency

SQLite enforces:

```text
UNIQUE(asset_id, date, source)
```

The loader uses:

```sql
ON CONFLICT(asset_id, date, source) DO NOTHING
```

Running the same transformed input twice therefore does not insert duplicate price rows.

The integration test executes the same load twice against a temporary SQLite database and verifies that the second run reports the row as skipped.

## Transaction Semantics

All symbols in one `load_all(...)` invocation are loaded inside one explicit SQLite transaction.

If loading any symbol fails:

```text
ROLLBACK
```

is executed and no partially loaded batch is committed.

This is stronger than committing each symbol independently because a failed scheduled run cannot leave a partially persisted batch while still appearing complete.

## Cache Behavior

Cache files include both symbol and interval:

```text
AAPL_1d.csv
BTC-USD_1h.csv
```

This prevents data collected at different intervals from accidentally sharing the same cache filename.

A stale or missing cache triggers an API fetch.

## CLI

Run directly:

```bash
python -m src.pipeline --config config.yaml
```

Or use the cron-compatible shell wrapper:

```bash
./scripts/run_pipeline.sh
```

Custom configuration:

```bash
./scripts/run_pipeline.sh config_example.yaml
```

The wrapper expects the virtual environment at:

```text
.venv/
```

## Tests

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

Run:

```bash
python -m ruff format --check .
python -m ruff check .
python -m compileall -q src tests
python -m pytest
python -m pip_audit -r requirements.txt
```

The tests cover:

- configuration validation
- rejection of unimplemented providers
- cache naming and freshness
- cache reads and cache writes
- transformation of valid OHLCV data
- invalid OHLCV rows
- missing columns
- fractional volume
- SQLite schema initialization
- idempotent duplicate loading
- transaction rollback
- pipeline success
- pipeline non-zero failure status
- CLI argument parsing

## Project Structure

```text
auto-etl-pipeline/
├── .github/
│   ├── dependabot.yml
│   └── workflows/
│       └── ci.yml
├── scripts/
│   └── run_pipeline.sh
├── src/
│   ├── __init__.py
│   ├── database.py
│   ├── extractor.py
│   ├── loader.py
│   ├── pipeline.py
│   └── transformer.py
├── tests/
│   ├── test_extractor.py
│   ├── test_loader.py
│   ├── test_pipeline.py
│   └── test_transformer.py
├── config_example.yaml
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── .gitignore
├── LICENSE
└── README.md
```

## Known Limitations

- yfinance is the only implemented provider.
- The pipeline is batch-oriented, not streaming.
- SQLite is appropriate for the scope of this repository but is not presented as a distributed data platform.
- Provider availability and upstream schema changes remain external failure modes.
- The cache is local-filesystem based.
- There is no incremental watermark or late-event model.
- There is no schema registry.
- There is no orchestration service beyond the CLI/shell runner.

Those capabilities belong in a future dedicated data-platform project rather than being implied here without implementation.

## CI

GitHub Actions runs on Python 3.12 and 3.13 and checks:

- Ruff formatting
- Ruff linting
- bytecode compilation
- pytest with coverage
- dependency vulnerability audit

Dependabot monitors Python and GitHub Actions dependencies.

## License

MIT. See [`LICENSE`](LICENSE).