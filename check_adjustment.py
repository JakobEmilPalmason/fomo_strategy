#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

def check_aapl_adjustment():
    """Check if AAPL data is already split-adjusted by looking at 2020 4:1 split"""
    
    # AAPL 4:1 split was on 2020-08-31
    split_date = pd.Timestamp("2020-08-31")
    expected_ratio = 4.0  # 4:1 split
    
    # Look for AAPL 2020 data
    base = Path("/Users/solmate/fomo_strategy")
    aapl_files = list(base.rglob("2_unadjusted_parquet/minute_data/ticker=AAPL/year=2020/*.parquet"))
    
    if not aapl_files:
        print("❌ No AAPL 2020 data found")
        return
    
    print(f"Found {len(aapl_files)} AAPL 2020 files")
    
    # Read first file
    sample_file = aapl_files[0]
    print(f"Checking file: {sample_file}")
    
    try:
        df = pd.read_parquet(sample_file)
        print(f"Columns: {df.columns.tolist()}")
        
        if "ts" not in df.columns or "close" not in df.columns:
            print("❌ Missing required columns (ts, close)")
            return
        
        # Convert timestamps
        df["ts"] = pd.to_datetime(df["ts"])
        df["ts_et"] = df["ts"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        df["date"] = df["ts_et"].dt.tz_localize(None).dt.normalize()
        
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Get prices before and after split
        pre_split = df[df["date"] < split_date]["close"]
        post_split = df[df["date"] > split_date]["close"]
        
        print(f"Pre-split data points: {len(pre_split)}")
        print(f"Post-split data points: {len(post_split)}")
        
        if pre_split.empty or post_split.empty:
            print("❌ Insufficient data around split date")
            return
        
        # Calculate price ratio
        pre_avg = pre_split.mean()
        post_avg = post_split.mean()
        price_ratio = pre_avg / post_avg if post_avg > 0 else 0
        
        print(f"\n📊 AAPL Price Analysis:")
        print(f"Pre-split average price: ${pre_avg:.2f}")
        print(f"Post-split average price: ${post_avg:.2f}")
        print(f"Price ratio (pre/post): {price_ratio:.2f}")
        print(f"Expected ratio (4:1 split): {expected_ratio:.2f}")
        
        # Determine if adjusted
        tolerance = 0.2  # 20% tolerance
        if abs(price_ratio - expected_ratio) < tolerance:
            print(f"\n⚠️  ALREADY ADJUSTED! Price ratio {price_ratio:.2f} ≈ split ratio {expected_ratio:.2f}")
            print("   Your data appears to be split-adjusted already.")
            print("   Do NOT run the split adjustment script!")
        elif abs(price_ratio - 1.0) < tolerance:
            print(f"\n✅ UNADJUSTED! Price ratio {price_ratio:.2f} ≈ 1.0")
            print("   Your data appears to be unadjusted.")
            print("   Safe to run the split adjustment script.")
        else:
            print(f"\n⚠️  UNEXPECTED! Price ratio {price_ratio:.2f}")
            print("   Manual verification recommended.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_aapl_adjustment() 