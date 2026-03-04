# auto-etl-pipeline
Production-style ETL pipeline for financial market data in Python. Extracts via yfinance/ccxt with CSV fallback, transforms with Pandas validation, and loads into normalized SQLite with duplicate detection. Cron-automated, config-driven, and fully documented.
