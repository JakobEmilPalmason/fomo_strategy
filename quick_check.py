#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def quick_aapl_check():
    """Quick check: AAPL price before vs after 2020-08-31 split"""
    
    # AAPL 4:1 split was on 2020-08-31
    split_date = pd.Timestamp("2020-08-31")
    
    # Look for AAPL 2020 data
    base = Path("/Users/solmate/fomo_strategy")
    aapl_dir = base / "2_unadjusted_parquet" / "minute_data" / "ticker=AAPL" / "year=2020"
    
    if not aapl_dir.exists():
        print("❌ No AAPL 2020 data found")
        return
    
    # Get all parquet files
    parquet_files = list(aapl_dir.glob("*.parquet"))
    if not parquet_files:
        print("❌ No parquet files in AAPL 2020 directory")
        return
    
    print(f"Found {len(parquet_files)} AAPL 2020 files")
    
    # Collect data from multiple files
    all_pre_split = []
    all_post_split = []
    
    for i, file_path in enumerate(parquet_files[:20]):  # Check first 20 files
        try:
            df = pd.read_parquet(file_path)
            if "ts" not in df.columns or "close" not in df.columns:
                continue
                
            # Convert timestamps to ET
            df["ts"] = pd.to_datetime(df["ts"])
            if getattr(df["ts"].dtype, "tz", None) is None:
                df["ts"] = df["ts"].dt.tz_localize("UTC")
            else:
                df["ts"] = df["ts"].dt.tz_convert("UTC")
            df["ts_et"] = df["ts"].dt.tz_convert("America/New_York")
            df["date"] = df["ts_et"].dt.tz_localize(None).dt.normalize()
            
            # Split data
            pre_split = df[df["date"] < split_date]["close"]
            post_split = df[df["date"] > split_date]["close"]
            
            if not pre_split.empty:
                all_pre_split.extend(pre_split.tolist())
            if not post_split.empty:
                all_post_split.extend(post_split.tolist())
                
        except Exception as e:
            continue
    
    print(f"Total pre-split data points: {len(all_pre_split)}")
    print(f"Total post-split data points: {len(all_post_split)}")
    
    if not all_pre_split or not all_post_split:
        print("❌ Insufficient data around split date")
        return
    
    # Calculate price ratio
    pre_avg = pd.Series(all_pre_split).mean()
    post_avg = pd.Series(all_post_split).mean()
    price_ratio = pre_avg / post_avg if post_avg > 0 else 0
    
    print(f"\n📊 AAPL Price Analysis:")
    print(f"Pre-split average price: ${pre_avg:.2f}")
    print(f"Post-split average price: ${post_avg:.2f}")
    print(f"Price ratio (pre/post): {price_ratio:.2f}")
    print(f"Expected ratio (4:1 split): 4.00")
    
    # Determine if adjusted
    if abs(price_ratio - 4.0) < 0.5:  # 12.5% tolerance
        print(f"\n⚠️  ALREADY ADJUSTED! Price ratio {price_ratio:.2f} ≈ 4.0")
        print("   Your data appears to be split-adjusted already.")
        print("   Do NOT run the split adjustment script!")
    elif abs(price_ratio - 1.0) < 0.2:  # 20% tolerance
        print(f"\n✅ UNADJUSTED! Price ratio {price_ratio:.2f} ≈ 1.0")
        print("   Your data appears to be unadjusted.")
        print("   Safe to run the split adjustment script.")
    else:
        print(f"\n⚠️  UNEXPECTED! Price ratio {price_ratio:.2f}")
        print("   Manual verification recommended.")

if __name__ == "__main__":
    quick_aapl_check() 