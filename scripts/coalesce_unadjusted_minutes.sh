#!/usr/bin/env bash
set -euo pipefail

# Coalesce unadjusted minute data from thousands of small parquet files 
# into single files per (ticker, year) partition using DuckDB.

# Configuration
BASE="${1:-$HOME/fomo_strategy}"
IN="$BASE/2_unadjusted_parquet/minute_data"
OUT="$BASE/2_unadjusted_parquet_coalesced/minute_data"
WORKERS="${2:-$(sysctl -n hw.ncpu 2>/dev/null || echo 4)}"

# Check if DuckDB is available
if ! command -v duckdb &> /dev/null; then
    echo "ERROR: DuckDB is not available. Please install DuckDB first:"
    echo "  brew install duckdb  # macOS"
    echo "  # or download from https://duckdb.org/docs/installation/"
    exit 1
fi

# Check input directory exists
if [[ ! -d "$IN" ]]; then
    echo "ERROR: Input directory does not exist: $IN"
    exit 1
fi

echo "Base: $BASE"
echo "Input: $IN"
echo "Output: $OUT"
echo "Workers: $WORKERS"

# Create output directory
mkdir -p "$OUT"

# Set DuckDB environment variable for concurrent processing
export DUCKDB_FORCE_CONCURRENT=true

echo "Scanning partitions..."
mapfile -t PARTS < <(find "$IN" -type d -path "*/ticker=*/year=*" | sort)

echo "Found ${#PARTS[@]} (ticker,year) partitions"

# Process each partition
for d in "${PARTS[@]}"; do
    T=$(basename "$(dirname "$d")"); T=${T#ticker=}
    Y=$(basename "$d");               Y=${Y#year=}
    dst="$OUT/ticker=$T/year=$Y"
    
    echo "Processing $T/$Y..."
    
    mkdir -p "$dst"
    
    # Check if output already exists
    if [[ -f "$dst/data.parquet" ]]; then
        echo "  SKIP: $dst/data.parquet already exists"
        continue
    fi
    
    # Count input files
    file_count=$(find "$d" -name "*.parquet" | wc -l)
    if [[ $file_count -eq 0 ]]; then
        echo "  WARNING: No parquet files found in $d"
        continue
    fi
    
    # Single-file write per partition to avoid many small files
    duckdb -c "
        PRAGMA threads=$WORKERS; 
        PRAGMA memory_limit='8GB';
        COPY (
            SELECT * FROM read_parquet('$d/*.parquet', union_by_name=true)
        )
        TO '$dst/data.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_REPLACE);
    "
    
    echo "  OK: $file_count files → 1 file"
done

echo "DONE. New coalesced tree at: $OUT"
echo "Next step: Update your split adjustment script to use the coalesced data" 