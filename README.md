# FOMO Strategy - Market Data Processing Pipeline

A comprehensive data processing pipeline for financial market data, specifically designed for split-adjusted minute-level OHLCV data processing.

## Overview

This project aims to:
- Process unadjusted minute-level market data
- Apply stock split adjustments
- Validate data quality
- Coalesce data for improved performance
- Generate comprehensive audit reports

## 📁 Project Structure

```
fomo_strategy/
├── 1_raw_files/                    # Reference data
│   └── reference/
│       └── splits.parquet          # Stock split events
├── 2_unadjusted_parquet/           # Raw minute data (gitignored)
│   └── minute_data/
│       └── ticker=XXX/
│           └── year=YYYY/
│               └── part-*.parquet
├── 3_adjusted_data/                # Processed data (gitignored)
│   ├── minute_sa/                  # Split-adjusted minute data
│   └── daily_sa/                   # Aggregated daily data
├── scripts/                        # Processing scripts
│   ├── split_adjust_minutes.py     # Main split adjustment script
│   ├── coalesce_unadjusted_minutes.py  # Data coalescing
│   ├── aggregate_daily_from_minutes.py # Daily aggregation
│   ├── validation.py               # Data quality validation
│   ├── verify_adjustment.py        # Adjustment verification
│   └── keys.py                     # API configuration
├── audit/                          # Audit reports (gitignored)
├── logs/                           # Processing logs (gitignored)
├── universe/                       # Universe definitions
├── requirements.txt                # Python dependencies
├── env_template.txt                # Environment template
└── README.md                       # This file
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone the repository
git clone <your-repo-url>
cd fomo_strategy

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install system dependencies (optional, for data coalescing)
# macOS: brew install duckdb
# Linux: Download from https://duckdb.org/docs/installation/
# Windows: Download from https://duckdb.org/docs/installation/

# Setup environment variables
cp env_template.txt .env
# Edit .env with your Polygon API key

# Setup API keys (optional, for scripts that need direct access)
cp scripts/keys_template.py scripts/keys.py
# The keys.py file is gitignored to prevent accidental commits
```

### 2. Prepare Your Data

Place your unadjusted minute data in the following structure:
```
2_unadjusted_parquet/minute_data/
├── ticker=AAPL/
│   ├── year=2023/
│   │   └── part-*.parquet
│   └── year=2024/
│       └── part-*.parquet
└── ticker=MSFT/
    └── year=2024/
        └── part-*.parquet
```

### 3. Run Split Adjustment

```bash
# Basic split adjustment
python scripts/split_adjust_minutes.py --base ~/fomo_strategy

# With API fallback for splits
python scripts/split_adjust_minutes.py --base ~/fomo_strategy --fetch-if-missing

# Process specific years
python scripts/split_adjust_minutes.py --base ~/fomo_strategy --years "2024,2023"

# Overwrite existing data
python scripts/split_adjust_minutes.py --base ~/fomo_strategy --overwrite
```

## 📊 Data Processing Pipeline

### 1. Data Coalescing (Optional)
For better performance, coalesce your data first:
```bash
python scripts/coalesce_unadjusted_minutes.py --base ~/fomo_strategy
```

### 2. Split Adjustment
The main processing step that applies stock split adjustments:
```bash
python scripts/split_adjust_minutes.py --base ~/fomo_strategy
```

### 3. Validation
Verify the quality and correctness of your data:
```bash
python scripts/validation.py --base ~/fomo_strategy
```

### 4. Adjustment Verification
Check that splits were applied correctly:
```bash
python scripts/verify_adjustment.py --base ~/fomo_strategy
```

### 5. Daily Aggregation
Create daily OHLCV bars from adjusted minute data (ensures perfect consistency):
```bash
python scripts/aggregate_daily_from_minutes.py --base ~/fomo_strategy
```

## 🔧 Configuration

### Environment Variables
Create a `.env` file with:
```bash
POLYGON_API_KEY=your_polygon_api_key_here
```

### Script Options

#### split_adjust_minutes.py
- `--base`: Project base directory
- `--fetch-if-missing`: Fetch splits from Polygon API if local file missing
- `--overwrite`: Overwrite existing adjusted files
- `--workers`: Number of parallel workers (default: CPU count)
- `--batch-size`: Files per batch (default: 2000)
- `--years`: Comma-separated years to process
- `--skip-adjustment-check`: Skip check for already-adjusted data

#### validation.py
- `--base`: Project base directory
- `--output`: Output directory for reports
- `--sample-size`: Number of files to sample for validation

## 📈 Data Format

### Input Data (Unadjusted)
Parquet files with columns:
- `ts`: Timestamp (UTC)
- `open`, `high`, `low`, `close`: Price data
- `volume`: Volume data
- `ticker`: Stock symbol (optional, inferred from path)

### Output Data (Adjusted)
Same structure as input, but with:
- Prices adjusted for stock splits
- Volumes adjusted inversely
- Proper timezone handling (ET for split dates)

### Splits Data
Parquet file with columns:
- `ticker`: Stock symbol
- `execution_date`: Split execution date
- `split_to`, `split_from`: Split ratio components

## 🔍 Quality Assurance

The pipeline includes comprehensive validation:
- Data completeness checks
- Price/volume consistency
- Split adjustment verification
- Missing data detection
- Duplicate removal

## 📝 Logging

All processing steps generate detailed logs in the `logs/` directory:
- Processing progress
- Error reports
- Performance metrics
- Data quality issues

## 🚨 Troubleshooting

### Common Issues

1. **Missing splits.parquet**: Use `--fetch-if-missing` flag
2. **Memory issues**: Reduce `--batch-size` or `--workers`
3. **Already adjusted data**: Use `--skip-adjustment-check`
4. **Permission errors**: Check file/directory permissions

### Performance Tips

1. Use coalesced data for faster processing
2. Adjust worker count based on available memory
3. Process specific years to reduce scope
4. Use SSD storage for better I/O performance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

[Add your license here]

## 🙏 Acknowledgments

- Polygon.io for market data API
- PyArrow for efficient parquet processing
- Pandas for data manipulation 