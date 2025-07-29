#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

def tight_aapl_check():
    """Tight window check: AAPL price Aug 26-28 vs Sep 1-3, 2020, ET RTH only"""
    
    # Tight windows around the split (avoid Aug 31 ex-date)
    pre_start = pd.Timestamp("2020-08-26")
    pre_end = pd.Timestamp("2020-08-28")
    post_start = pd.Timestamp("2020-09-01")
    post_end = pd.Timestamp("2020-09-03")
    
    # RTH hours: 09:30-16:00 ET
    rth_start = pd.Timestamp("09:30").time()
    rth_end = pd.Timestamp("16:00").time()
    
    print(f"Pre-split window: {pre_start.date()} to {pre_end.date()}")
    print(f"Post-split window: {post_start.date()} to {post_end.date()}")
    print(f"RTH hours: {rth_start} to {rth_end} ET")
    
    # Look for AAPL 2020 data
    base = Path("/Users/solmate/fomo_strategy")
    aapl_dir = base / "2_unadjusted_parquet" / "minute_data" / "ticker=AAPL" / "year=2020"
    
    if not aapl_dir.exists():
        print("❌ No AAPL 2020 data found")
        return
    
    parquet_files = list(aapl_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} AAPL 2020 files")
    
    # Collect RTH data from tight windows
    pre_prices = []
    post_prices = []
    
    for file_path in parquet_files:
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
            df["time"] = df["ts_et"].dt.time
            
            # Filter for RTH hours
            rth_mask = (df["time"] >= rth_start) & (df["time"] <= rth_end)
            df_rth = df[rth_mask]
            
            # Split data by tight windows
            pre_mask = (df_rth["date"] >= pre_start) & (df_rth["date"] <= pre_end)
            post_mask = (df_rth["date"] >= post_start) & (df_rth["date"] <= post_end)
            
            pre_prices.extend(df_rth[pre_mask]["close"].tolist())
            post_prices.extend(df_rth[post_mask]["close"].tolist())
                
        except Exception as e:
            continue
    
    print(f"\nPre-split RTH data points: {len(pre_prices)}")
    print(f"Post-split RTH data points: {len(post_prices)}")
    
    if not pre_prices or not post_prices:
        print("❌ Insufficient RTH data in tight windows")
        return
    
    # Calculate price ratio
    pre_avg = pd.Series(pre_prices).mean()
    post_avg = pd.Series(post_prices).mean()
    price_ratio = pre_avg / post_avg if post_avg > 0 else 0
    
    print(f"\n📊 AAPL Tight Window Analysis:")
    print(f"Pre-split average price: ${pre_avg:.2f}")
    print(f"Post-split average price: ${post_avg:.2f}")
    print(f"Price ratio (pre/post): {price_ratio:.2f}")
    print(f"Expected ratio (4:1 split): 4.00")
    
    # Determine if adjusted
    tolerance = 0.2  # 5% tolerance
    if abs(price_ratio - 4.0) < tolerance:
        print(f"\n✅ UNADJUSTED! Price ratio {price_ratio:.2f} ≈ 4.0")
        print("   Your data appears to be unadjusted.")
        print("   Safe to run the split adjustment script!")
    elif abs(price_ratio - 1.0) < tolerance:
        print(f"\n⚠️  ALREADY ADJUSTED! Price ratio {price_ratio:.2f} ≈ 1.0")
        print("   Your data appears to be split-adjusted already.")
        print("   Do NOT run the split adjustment script!")
    else:
        print(f"\n⚠️  UNEXPECTED! Price ratio {price_ratio:.2f}")
        print("   Check timezone handling, RTH filters, or data coverage.")

if __name__ == "__main__":
    tight_aapl_check() 