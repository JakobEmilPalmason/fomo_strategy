#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def debug_dates():
    """Debug date comparison"""
    
    split_date = pd.Timestamp("2020-08-31")
    print(f"Split date: {split_date}")
    print(f"Split date type: {type(split_date)}")
    
    base = Path("/Users/solmate/fomo_strategy")
    aapl_dir = base / "2_unadjusted_parquet" / "minute_data" / "ticker=AAPL" / "year=2020"
    
    parquet_files = list(aapl_dir.glob("*.parquet"))
    
    for i, file_path in enumerate(parquet_files[:5]):
        try:
            df = pd.read_parquet(file_path)
            df["ts"] = pd.to_datetime(df["ts"])
            min_date = df["ts"].min()
            max_date = df["ts"].max()
            print(f"\nFile {i+1}: {file_path.name}")
            print(f"  Min date: {min_date} (type: {type(min_date)})")
            print(f"  Max date: {max_date}")
            print(f"  Min < split? {min_date < split_date}")
            print(f"  Max > split? {max_date > split_date}")
        except Exception as e:
            print(f"File {i+1}: Error - {e}")

if __name__ == "__main__":
    debug_dates() 