# FOMO Strategy

Intraday momentum strategy for detecting and trading FOMO (Fear of Missing Out) patterns in equity markets.

## Overview

This project identifies unusual volume and price momentum patterns in minute-level market data to predict intraday continuation moves driven by FOMO behavior.

## Quick Start

```bash
# Clone and setup
git clone https://github.com/JakobEmilPalmason/fomo_strategy.git
cd fomo_strategy
pip install -r requirements.txt

# Configure
cp env_template.txt .env
# Add your Polygon API key to .env

# Run split adjustment
python scripts/split_adjust_minutes.py --base ~/fomo_strategy
```

## Key Features
- Process years of minute-level market data
- Apply split adjustments automatically
- Validate data quality
- Detect FOMO patterns (in development)

## Project Structure
```
scripts/
├── split_adjust_minutes.py         # Split adjustment processing
├── validation.py                   # Data quality checks
├── aggregate_daily_from_minutes.py # Generate daily bars
└── coalesce_unadjusted_minutes.py # Performance optimization
```
