#!/bin/bash
# run_pipeline.sh - Cron-compatible ETL runner
#
# Usage: 
#   ./scripts/run_pipeline.sh              (default config)
#   ./scripts/run_pipeline.sh config.yaml  (custom config)

set -euo pipefail

# Resolve project root (one level up from scripts/)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run pipeline
python -m src.pipeline

# Exit code forwarding for cron monitoring
exit $?
