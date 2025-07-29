# Data Coalescing for Split Adjustment

This directory contains scripts to coalesce the unadjusted minute data from thousands of small parquet files into single files per (ticker, year) partition. This dramatically improves the performance of the split adjustment process.

## Problem

The original data structure has thousands of small parquet files:
```
2_unadjusted_parquet/minute_data/
├── ticker=AAPL/year=2020/
│   ├── part-00000-*.parquet
│   ├── part-00001-*.parquet
│   └── ... (thousands of files)
└── ticker=MSFT/year=2020/
    ├── part-00000-*.parquet
    └── ... (thousands of files)
```

This creates significant overhead when processing files for split adjustment.

## Solution

Coalesce the data into single files per partition:
```
2_unadjusted_parquet_coalesced/minute_data/
├── ticker=AAPL/year=2020/
│   └── data.parquet  (single file)
└── ticker=MSFT/year=2020/
    └── data.parquet  (single file)
```

## Scripts

### 1. Python Script (Recommended)
```bash
# Basic usage
python scripts/coalesce_unadjusted_minutes.py --base ~/fomo_strategy

# With custom workers
python scripts/coalesce_unadjusted_minutes.py --base ~/fomo_strategy --workers 8

# Overwrite existing files
python scripts/coalesce_unadjusted_minutes.py --base ~/fomo_strategy --overwrite
```

### 2. Bash Script (Alternative)
```bash
# Basic usage
bash scripts/coalesce_unadjusted_minutes.sh

# With custom base path and workers
bash scripts/coalesce_unadjusted_minutes.sh ~/fomo_strategy 8
```

## Requirements

- **DuckDB**: Required for efficient parquet processing
  ```bash
  # macOS
  brew install duckdb
  
  # Or download from https://duckdb.org/docs/installation/
  ```

## Performance Benefits

- **File Count**: Reduces from ~22.7M files to ~(#tickers × #years) files
- **Processing Speed**: Split adjustment runs 10-100x faster
- **I/O Efficiency**: Fewer file operations, better compression
- **Memory Usage**: More efficient memory utilization

## Usage Workflow

1. **Coalesce the data** (one-time operation):
   ```bash
   python scripts/coalesce_unadjusted_minutes.py --base ~/fomo_strategy
   ```

2. **Run split adjustment** (uses coalesced data automatically):
   ```bash
   python scripts/split_adjust_minutes.py --base ~/fomo_strategy
   ```

The split adjustment script automatically detects and uses coalesced data if available, falling back to the original structure if not.

## File Structure

After coalescing, your directory structure will be:
```
fomo_strategy/
├── 2_unadjusted_parquet/           # Original (many small files)
│   └── minute_data/
├── 2_unadjusted_parquet_coalesced/ # New (one file per partition)
│   └── minute_data/
│       ├── ticker=AAPL/
│       │   ├── year=2020/
│       │   │   └── data.parquet
│       │   └── year=2021/
│       │       └── data.parquet
│       └── ticker=MSFT/
│           └── ...
└── 3_adjusted_data/                # Output (uses coalesced input)
    └── minute_sa/
```

## Monitoring

Both scripts provide progress tracking with:
- File/partition counts
- Processing rate
- ETA estimates
- Error reporting

## Troubleshooting

### DuckDB not found
```bash
# Install DuckDB
brew install duckdb
```

### Permission errors
```bash
# Make script executable
chmod +x scripts/coalesce_unadjusted_minutes.sh
```

### Memory issues
Reduce the number of workers:
```bash
python scripts/coalesce_unadjusted_minutes.py --workers 4
```

### Disk space
The coalesced data may be slightly larger due to better compression, but the performance gain is significant. 