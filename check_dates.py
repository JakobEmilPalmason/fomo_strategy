#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def check_aapl_dates():
    """Check date ranges in AAPL 2020 files"""
    
    base = Path("/Users/solmate/fomo_strategy")
    aapl_dir = base / "2_unadjusted_parquet" / "minute_data" / "ticker=AAPL" / "year=2020"
    
    if not aapl_dir.exists():
        print("❌ No AAPL 2020 data found")
        return
    
    parquet_files = list(aapl_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} AAPL 2020 files")
    
    # Check first few files
    for i, file_path in enumerate(parquet_files[:5]):
        try:
            df = pd.read_parquet(file_path)
            df["ts"] = pd.to_datetime(df["ts"])
            min_date = df["ts"].min()
            max_date = df["ts"].max()
            print(f"File {i+1}: {min_date} to {max_date}")
        except Exception as e:
            print(f"File {i+1}: Error - {e}")

if __name__ == "__main__":
    check_aapl_dates() 