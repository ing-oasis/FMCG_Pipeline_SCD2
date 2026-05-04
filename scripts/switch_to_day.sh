#!/usr/bin/env bash
# Usage: ./scripts/switch_to_day.sh 1
#        ./scripts/switch_to_day.sh 2
#
# Copies the chosen day's source files into data/raw/, replacing whatever
# was there before. The DAG always reads from data/raw/ — this is how we
# simulate sequential daily ingest without changing DAG config between runs.

set -euo pipefail

if [ "$#" -ne 1 ] || ! [[ "$1" =~ ^[12]$ ]]; then
    echo "Usage: $0 <1|2>" >&2
    exit 1
fi

DAY="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/scripts/sim_data/day$DAY"
DEST_DIR="$PROJECT_ROOT/data/raw"

if [ ! -d "$SRC_DIR" ]; then
    echo "Error: $SRC_DIR not found." >&2
    echo "Run 'python scripts/generate_data.py' first." >&2
    exit 1
fi

# Wipe data/raw/ except for .gitkeep
find "$DEST_DIR" -type f ! -name ".gitkeep" -delete

# Copy the chosen day's CSVs and JSONs
cp "$SRC_DIR"/*.csv "$DEST_DIR/" 2>/dev/null || true
cp "$SRC_DIR"/*.json "$DEST_DIR/" 2>/dev/null || true

echo "Switched data/raw/ to day $DAY:"
ls -la "$DEST_DIR"/*.csv "$DEST_DIR"/*.json 2>/dev/null | awk '{print "  ", $NF, "(" $5 " bytes)"}'
