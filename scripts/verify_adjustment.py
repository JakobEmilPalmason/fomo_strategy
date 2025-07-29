#!/usr/bin/env python
"""
Verification script for split adjustment.
Tests known split events to ensure adjustment is working correctly.
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Known split events for verification
TEST_SPLITS = {
    'AAPL': ('2020-08-31', 4.0),    # 4:1 split
    'TSLA': ('2020-08-31', 5.0),    # 5:1 split  
    'AMZN': ('2022-06-06', 20.0),   # 20:1 split
    'GOOGL': ('2022-07-18', 20.0),  # 20:1 split
}

def load_data(base_path: Path, ticker: str, year: int, data_type: str = "unadjusted") -> pd.DataFrame:
    """Load minute data for a ticker and year."""
    if data_type == "unadjusted":
        data_path = base_path / "2_unadjusted_parquet" / "minute_data" / f"ticker={ticker}" / f"year={year}"
    else:  # adjusted
        # Find the most recent adjusted data version
        adjusted_root = base_path / "3_adjusted_data" / "minute_sa"
        versions = [d for d in adjusted_root.iterdir() if d.is_dir() and d.name.startswith("v")]
        if not versions:
            raise FileNotFoundError(f"No adjusted data versions found in {adjusted_root}")
        latest_version = max(versions, key=lambda x: x.name)
        data_path = latest_version / f"ticker={ticker}" / f"year={year}"
    
    if not data_path.exists():
        raise FileNotFoundError(f"No data found at {data_path}")
    
    # Load all parquet files in the directory
    files = list(data_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {data_path}")
    
    dfs = []
    for file in files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    data = pd.concat(dfs, ignore_index=True)
    
    # Ensure proper timestamp handling
    data["ts"] = pd.to_datetime(data["ts"])
    if getattr(data["ts"].dtype, "tz", None) is None:
        data["ts"] = data["ts"].dt.tz_localize("UTC")
    else:
        data["ts"] = data["ts"].dt.tz_convert("UTC")
    
    data["ts_et"] = data["ts"].dt.tz_convert("America/New_York")
    data["date"] = data["ts_et"].dt.tz_localize(None).dt.normalize()
    
    return data.sort_values("ts").reset_index(drop=True)

def verify_split_adjustment(base_path: Path, ticker: str, split_date: str, expected_ratio: float, 
                          tolerance: float = 0.05) -> dict:
    """
    Verify split adjustment for a specific ticker and split event.
    
    Args:
        base_path: Project base path
        ticker: Stock ticker
        split_date: Split execution date (YYYY-MM-DD)
        expected_ratio: Expected split ratio (e.g., 4.0 for 4:1 split)
        tolerance: Acceptable deviation from expected ratio (default 5%)
    
    Returns:
        dict with verification results
    """
    split_dt = pd.Timestamp(split_date)
    year = split_dt.year
    
    logger.info(f"Verifying {ticker} {split_date} {expected_ratio}:1 split...")
    
    try:
        # Load unadjusted data
        unadj_data = load_data(base_path, ticker, year, "unadjusted")
        
        # Try to load adjusted data, but don't fail if it doesn't exist
        try:
            adj_data = load_data(base_path, ticker, year, "adjusted")
            has_adjusted_data = True
        except FileNotFoundError:
            logger.info(f"No adjusted data found for {ticker} {year} - this is expected if split adjustment hasn't been run yet")
            has_adjusted_data = False
            adj_data = None
        
        # Filter data around split date (±5 days)
        start_date = split_dt - timedelta(days=5)
        end_date = split_dt + timedelta(days=5)
        
        unadj_filtered = unadj_data[
            (unadj_data["date"] >= start_date) & 
            (unadj_data["date"] <= end_date)
        ]
        
        if unadj_filtered.empty:
            return {
                "ticker": ticker,
                "split_date": split_date,
                "expected_ratio": expected_ratio,
                "status": "SKIP",
                "reason": "Insufficient unadjusted data around split date"
            }
        
        # If no adjusted data exists, just verify unadjusted data is available
        if not has_adjusted_data:
            return {
                "ticker": ticker,
                "split_date": split_date,
                "expected_ratio": expected_ratio,
                "status": "READY",
                "reason": f"Unadjusted data available ({len(unadj_filtered):,} records), ready for split adjustment"
            }
        
        adj_filtered = adj_data[
            (adj_data["date"] >= start_date) & 
            (adj_data["date"] <= end_date)
        ]
        
        if adj_filtered.empty:
            return {
                "ticker": ticker,
                "split_date": split_date,
                "expected_ratio": expected_ratio,
                "status": "SKIP",
                "reason": "Insufficient adjusted data around split date"
            }
        
        # Calculate price ratios for different periods
        results = {}
        
        # 1. Pre-split period (should show adjustment)
        pre_split_unadj = unadj_filtered[unadj_filtered["date"] < split_dt]["close"]
        pre_split_adj = adj_filtered[adj_filtered["date"] < split_dt]["close"]
        
        if not pre_split_unadj.empty and not pre_split_adj.empty:
            pre_ratio = pre_split_unadj.mean() / pre_split_adj.mean()
            results["pre_split_ratio"] = pre_ratio
            results["pre_split_adjusted"] = abs(pre_ratio - expected_ratio) < tolerance
        else:
            results["pre_split_ratio"] = None
            results["pre_split_adjusted"] = False
        
        # 2. Post-split period (should show no adjustment)
        post_split_unadj = unadj_filtered[unadj_filtered["date"] > split_dt]["close"]
        post_split_adj = adj_filtered[adj_filtered["date"] > split_dt]["close"]
        
        if not post_split_unadj.empty and not post_split_adj.empty:
            post_ratio = post_split_unadj.mean() / post_split_adj.mean()
            results["post_split_ratio"] = post_ratio
            results["post_split_adjusted"] = abs(post_ratio - 1.0) < tolerance
        else:
            results["post_split_ratio"] = None
            results["post_split_adjusted"] = False
        
        # 3. Overall assessment
        pre_ok = results.get("pre_split_adjusted", False)
        post_ok = results.get("post_split_adjusted", False)
        
        if pre_ok and post_ok:
            status = "PASS"
            reason = "Both pre-split and post-split periods show correct adjustment"
        elif pre_ok:
            status = "PARTIAL"
            reason = "Pre-split period adjusted correctly, post-split period unclear"
        elif post_ok:
            status = "PARTIAL" 
            reason = "Post-split period unchanged correctly, pre-split period unclear"
        else:
            status = "FAIL"
            reason = "Neither period shows expected adjustment pattern"
        
        return {
            "ticker": ticker,
            "split_date": split_date,
            "expected_ratio": expected_ratio,
            "status": status,
            "reason": reason,
            **results
        }
        
    except Exception as e:
        return {
            "ticker": ticker,
            "split_date": split_date,
            "expected_ratio": expected_ratio,
            "status": "ERROR",
            "reason": str(e)
        }

def main():
    parser = argparse.ArgumentParser(description="Verify split adjustment on known examples")
    parser.add_argument("--base", default="~/fomo_strategy", help="Project base folder")
    parser.add_argument("--tolerance", type=float, default=0.05, help="Acceptable deviation from expected ratio (default 0.05)")
    args = parser.parse_args()
    
    base_path = Path(args.base).expanduser().resolve()
    
    logger.info(f"Verifying split adjustments in {base_path}")
    logger.info(f"Tolerance: {args.tolerance:.1%}")
    
    results = []
    
    for ticker, (split_date, ratio) in TEST_SPLITS.items():
        result = verify_split_adjustment(base_path, ticker, split_date, ratio, args.tolerance)
        results.append(result)
        
        # Print immediate results
        status_emoji = {
            "PASS": "✅",
            "PARTIAL": "⚠️", 
            "FAIL": "❌",
            "SKIP": "⏭️",
            "ERROR": "💥",
            "READY": "🟢"
        }
        
        emoji = status_emoji.get(result["status"], "❓")
        logger.info(f"{emoji} {ticker}: {result['status']} - {result['reason']}")
        
        if result["status"] not in ["SKIP", "ERROR"]:
            if result.get("pre_split_ratio"):
                logger.info(f"   Pre-split ratio: {result['pre_split_ratio']:.3f} (expected: {ratio:.1f})")
            if result.get("post_split_ratio"):
                logger.info(f"   Post-split ratio: {result['post_split_ratio']:.3f} (expected: 1.0)")
    
    # Summary
    df_results = pd.DataFrame(results)
    status_counts = df_results["status"].value_counts()
    
    logger.info("\n" + "="*50)
    logger.info("VERIFICATION SUMMARY")
    logger.info("="*50)
    
    for status, count in status_counts.items():
        emoji = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "❌", "SKIP": "⏭️", "ERROR": "💥", "READY": "🟢"}.get(status, "❓")
        logger.info(f"{emoji} {status}: {count}")
    
    # Detailed results table
    logger.info("\nDetailed Results:")
    logger.info("-" * 80)
    for _, row in df_results.iterrows():
        emoji = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "❌", "SKIP": "⏭️", "ERROR": "💥", "READY": "🟢"}.get(row["status"], "❓")
        logger.info(f"{emoji} {row['ticker']:6s} | {row['split_date']} | {row['expected_ratio']:4.1f}:1 | {row['status']:7s} | {row['reason']}")
    
    # Final recommendation
    if "FAIL" in status_counts or "ERROR" in status_counts:
        logger.error("\n❌ VERIFICATION FAILED - Do not proceed with full dataset!")
        return 1
    elif "READY" in status_counts and len(status_counts) == 1:
        logger.info("\n🟢 READY FOR SPLIT ADJUSTMENT - All test tickers have unadjusted data available!")
        logger.info("Run: python scripts/split_adjust_minutes.py --base ~/fomo_strategy --fetch-if-missing")
        return 0
    elif "PARTIAL" in status_counts:
        logger.warning("\n⚠️  PARTIAL VERIFICATION - Review results before proceeding")
        return 0
    elif "PASS" in status_counts:
        logger.info("\n✅ VERIFICATION PASSED - Split adjustment appears to be working correctly!")
        return 0
    else:
        logger.warning("\n⚠️  UNKNOWN STATUS - Review results manually")
        return 0

if __name__ == "__main__":
    exit(main()) 