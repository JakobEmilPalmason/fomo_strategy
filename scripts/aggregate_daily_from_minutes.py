#!/usr/bin/env python
"""
Aggregate daily OHLCV data from split-adjusted minute data.

This script reads split-adjusted minute data and creates daily bars by aggregating
minute bars within each trading day. This ensures perfect consistency between
minute and daily timeframes.

Usage:
    python aggregate_daily_from_minutes.py --base ~/fomo_strategy
    python aggregate_daily_from_minutes.py --base ~/fomo_strategy --years "2024,2023"
    python aggregate_daily_from_minutes.py --base ~/fomo_strategy --overwrite
"""

import argparse
import os
import sys
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_ticker_from_path(p: Path) -> str | None:
    """Extract ticker from path containing 'ticker=XXXX' segment."""
    for part in p.parts:
        if part.startswith("ticker="):
            return part.split("=", 1)[1]
    return None

def parse_year_from_path(p: Path) -> int | None:
    """Extract year from path containing 'year=YYYY' segment."""
    for part in p.parts:
        if part.startswith("year="):
            try:
                return int(part.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
    return None

def get_destination_path(fp: Path, tkr: str, out_root: Path) -> Path:
    """Get the destination path for a file."""
    year = parse_year_from_path(fp)
    if year:
        return out_root / f"ticker={tkr}" / f"year={year}"
    return out_root / f"ticker={tkr}"

def should_skip_file(fp: Path, tkr: str, out_root: Path, overwrite: bool) -> bool:
    """Check if file should be skipped (already exists and not overwriting)."""
    if overwrite:
        return False
    
    # Create manifest file path for tracking processed files
    manifest_file = out_root / "_processed_files.txt"
    
    # If manifest doesn't exist, nothing has been processed yet
    if not manifest_file.exists():
        return False
    
    # Check if this specific input file has been processed
    try:
        with open(manifest_file, 'r') as f:
            processed_files = {line.strip() for line in f}
        return str(fp) in processed_files
    except Exception:
        # If manifest is corrupted, assume nothing is processed
        return False

def mark_file_processed(fp: Path, out_root: Path) -> None:
    """Mark a file as processed in the manifest."""
    manifest_file = out_root / "_processed_files.txt"
    try:
        with open(manifest_file, 'a') as f:
            f.write(f"{fp}\n")
    except Exception as e:
        logger.warning(f"Failed to mark {fp} as processed: {e}")

def clear_manifest_if_overwrite(out_root: Path, overwrite: bool) -> None:
    """Clear the manifest file if overwriting to ensure all files are reprocessed."""
    if overwrite:
        manifest_file = out_root / "_processed_files.txt"
        if manifest_file.exists():
            try:
                manifest_file.unlink()
                logger.info("Cleared existing manifest file for overwrite mode")
            except Exception as e:
                logger.warning(f"Failed to clear manifest file: {e}")

def aggregate_file_worker(args: tuple) -> tuple[bool, str, str]:
    """
    Worker function for parallel processing.
    Returns (success, message, file_path).
    """
    fp, tkr, out_root, overwrite = args
    
    try:
        # Check if we should skip this file
        if should_skip_file(fp, tkr, out_root, overwrite):
            return True, f"SKIP exists {tkr}", str(fp)
        
        # Read parquet file
        tbl = pq.read_table(fp)
        df = tbl.to_pandas()
        
        if "ts" not in df.columns:
            return False, f"missing ts column", str(fp)
        
        # Ensure we have a ticker
        if "ticker" not in df.columns and not tkr:
            return False, "missing ticker in both path and file", str(fp)
        if "ticker" not in df.columns:
            df["ticker"] = tkr
        
        # Ensure proper data types
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["ts"])
        
        # Convert timestamps to ET for proper date alignment
        if getattr(df["ts"].dtype, "tz", None) is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")
        df["ts_et"] = df["ts"].dt.tz_convert("America/New_York")
        df["date"] = df["ts_et"].dt.tz_localize(None).dt.normalize()  # naive ET midnight
        
        # Remove duplicate rows
        initial_rows = len(df)
        if "ticker" in df.columns:
            df = df.drop_duplicates(subset=["ticker", "ts"], keep="first")
        else:
            df = df.drop_duplicates(subset=["ts"], keep="first")
        
        duplicates_removed = initial_rows - len(df)
        if duplicates_removed > 0:
            logger.debug(f"Removed {duplicates_removed} duplicate rows from {fp}")
        
        # Type coercion guard: convert strings to numeric before aggregation
        for c in ("open", "high", "low", "close", "volume"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        
        # Aggregate to daily bars
        daily_agg = df.groupby(['ticker', 'date']).agg({
            'open': 'first',
            'high': 'max', 
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).reset_index()
        
        # Add year column for partitioning
        daily_agg['year'] = daily_agg['date'].dt.year.astype('int32')
        
        # Drop helper columns and keep only essential ones
        keep_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'year']
        daily_agg = daily_agg[keep_cols]
        
        # Write to dataset
        behavior = "delete_matching" if overwrite else "overwrite_or_ignore"
        table = pa.Table.from_pandas(daily_agg, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(out_root),
            partition_cols=["ticker", "year"],
            existing_data_behavior=behavior
        )
        
        # Mark file as processed
        mark_file_processed(fp, out_root)
        
        return True, f"OK daily_bars={len(daily_agg):,}", str(fp)
        
    except Exception as e:
        return False, f"ERR: {str(e)}", str(fp)

def format_eta(done: int, total: int, start_ts: float) -> tuple[float, float]:
    """Calculate processing rate and ETA."""
    elapsed = max(time.time() - start_ts, 1e-6)
    rate = done / elapsed
    rem = max(total - done, 0)
    eta = rem / max(rate, 1e-9)
    return rate, eta

def main():
    ap = argparse.ArgumentParser(description="Aggregate daily OHLCV from split-adjusted minute data.")
    ap.add_argument("--base", default="~/fomo_strategy", help="Project base folder")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing daily parts")
    ap.add_argument("--print-every", type=int, default=10, help="Print progress every N files")
    ap.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of parallel workers")
    ap.add_argument("--batch-size", type=int, default=2000, help="Number of files to process in each batch")
    ap.add_argument("--years", type=str, help="Comma-separated list of years to process (e.g., '2025,2024,2023')")
    args = ap.parse_args()

    # Setup paths
    base = Path(args.base).expanduser().resolve()
    
    # Input: split-adjusted minute data
    in_root = base / "3_adjusted_data" / "minute_sa"
    
    # Find the latest version directory
    if not in_root.exists():
        logger.error(f"Split-adjusted minute data not found: {in_root}")
        logger.error("Please run split_adjust_minutes.py first")
        sys.exit(1)
    
    # Find the most recent version directory
    version_dirs = [d for d in in_root.iterdir() if d.is_dir() and d.name.startswith("v")]
    if not version_dirs:
        logger.error(f"No version directories found in {in_root}")
        sys.exit(1)
    
    latest_version = max(version_dirs, key=lambda x: x.name)
    in_root = latest_version
    logger.info(f"Using split-adjusted data from: {in_root}")
    
    # Output: daily aggregated data
    out_root = base / "3_adjusted_data" / "daily_sa" / ("v" + datetime.now().strftime("%Y-%m-%d"))
    out_root.mkdir(parents=True, exist_ok=True)

    # Clear manifest if overwriting
    clear_manifest_if_overwrite(out_root, args.overwrite)

    logger.info(f"Base: {base}")
    logger.info(f"Input: {in_root}")
    logger.info(f"Output: {out_root}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Batch size: {args.batch_size}")

    # Find all parquet files
    parts = sorted(in_root.rglob("part-*.parquet"))
    if not parts:
        # Try coalesced data structure (data.parquet files)
        parts = sorted(in_root.rglob("data.parquet"))
    if not parts:
        # Fallback to any parquet files
        parts = sorted(in_root.rglob("*.parquet"))
    if not parts:
        logger.error(f"No parquet files found under {in_root}")
        sys.exit(2)

    # Filter by years if specified
    if args.years:
        try:
            target_years = {int(y.strip()) for y in args.years.split(",")}
            logger.info(f"Filtering to years: {sorted(target_years)}")
            
            filtered_parts = []
            for fp in parts:
                year = parse_year_from_path(fp)
                if year in target_years:
                    filtered_parts.append(fp)
            
            original_count = len(parts)
            parts = filtered_parts
            logger.info(f"Filtered from {original_count:,} to {len(parts):,} files")
            
        except ValueError as e:
            logger.error(f"Invalid year format in --years argument: {e}")
            sys.exit(1)

    total = len(parts)
    logger.info(f"Files to process: {total:,}")

    # Prepare work items
    work_items = []
    for fp in parts:
        tkr = parse_ticker_from_path(fp) or ""
        work_items.append((fp, tkr, out_root, args.overwrite))

    # Process files in parallel with batching
    start = time.time()
    errors = 0
    processed = 0
    
    BATCH_SIZE = args.batch_size
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for i in range(0, total, BATCH_SIZE):
            batch = work_items[i:i+BATCH_SIZE]
            batch_size = len(batch)
            
            logger.info(f"Submitting batch {i//BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1)//BATCH_SIZE} ({batch_size:,} files)")
            
            # Submit batch
            futures = [executor.submit(aggregate_file_worker, item) for item in batch]
            
            # Process completed tasks in this batch
            for future in as_completed(futures):
                processed += 1
                ok, msg, file_path = future.result()
                
                if not ok and not msg.startswith("SKIP"):
                    errors += 1
                    logger.error(f"Error processing {file_path}: {msg}")
                
                # Print progress
                if (processed % args.print_every == 0) or (processed == total) or (not ok):
                    pct = 100.0 * processed / total
                    rate, eta = format_eta(processed, total, start)
                    print(f"[{processed:,}/{total:,}] {pct:6.2f}% | {msg} | {rate:.2f} files/s ETA {eta/60:.1f} min", flush=True)

    elapsed = time.time() - start
    logger.info(f"Aggregation complete!")
    logger.info(f"Files: {total:,}, Errors: {errors}, Time: {elapsed/60:.1f} min")
    logger.info(f"Output: {out_root}")
    logger.info(f"Daily data is now available at: {out_root}")

if __name__ == "__main__":
    # Pandas settings for safety and performance
    pd.options.mode.copy_on_write = True
    pd.options.mode.chained_assignment = None
    
    sys.exit(main()) 