#!/usr/bin/env python
"""
Coalesce unadjusted minute data from thousands of small parquet files 
into single files per (ticker, year) partition.

This script reads from 2_unadjusted_parquet/minute_data and writes to 
2_unadjusted_parquet_coalesced/minute_data, creating one data.parquet 
file per (ticker, year) partition instead of thousands of small parts.

Usage:
    python coalesce_unadjusted_minutes.py --base ~/fomo_strategy
    python coalesce_unadjusted_minutes.py --base ~/fomo_strategy --workers 8
"""

import argparse
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import subprocess
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_partitions(input_root: Path) -> list[tuple[str, int, Path]]:
    """
    Find all (ticker, year) partitions in the input directory.
    Returns list of (ticker, year, partition_path) tuples.
    """
    partitions = []
    
    # Look for ticker=*/year=* directory structure
    for ticker_dir in input_root.glob("ticker=*"):
        if not ticker_dir.is_dir():
            continue
        
        ticker = ticker_dir.name.split("=", 1)[1]
        
        for year_dir in ticker_dir.glob("year=*"):
            if not year_dir.is_dir():
                continue
            
            try:
                year = int(year_dir.name.split("=", 1)[1])
                partitions.append((ticker, year, year_dir))
            except (ValueError, IndexError):
                logger.warning(f"Invalid year directory: {year_dir}")
                continue
    
    return sorted(partitions)

def coalesce_partition_worker(args: tuple) -> tuple[bool, str, str]:
    """
    Worker function to coalesce a single (ticker, year) partition.
    Returns (success, message, partition_info).
    """
    ticker, year, input_path, output_root, overwrite = args
    
    try:
        # Create output directory
        output_dir = output_root / f"ticker={ticker}" / f"year={year}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "data.parquet"
        
        # Check if output already exists and we're not overwriting
        if output_file.exists() and not overwrite:
            return True, f"SKIP exists {ticker}/{year}", f"{ticker}/{year}"
        
        # Find all parquet files in the input partition
        parquet_files = list(input_path.glob("*.parquet"))
        if not parquet_files:
            return False, f"no parquet files found", f"{ticker}/{year}"
        
        # Build DuckDB query to coalesce all files
        file_paths = [str(f) for f in parquet_files]
        file_list = "', '".join(file_paths)
        
        duckdb_query = f"""
        PRAGMA threads={os.cpu_count()};
        PRAGMA memory_limit='8GB';
        
        COPY (
            SELECT * FROM read_parquet(['{file_list}'], union_by_name=true)
        )
        TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_REPLACE);
        """
        
        # Execute DuckDB command
        result = subprocess.run(
            ["duckdb", "-c", duckdb_query],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per partition
        )
        
        if result.returncode != 0:
            return False, f"duckdb error: {result.stderr}", f"{ticker}/{year}"
        
        # Verify output file was created and has content
        if not output_file.exists() or output_file.stat().st_size == 0:
            return False, f"output file missing or empty", f"{ticker}/{year}"
        
        return True, f"OK {len(parquet_files)} files → 1", f"{ticker}/{year}"
        
    except subprocess.TimeoutExpired:
        return False, f"timeout after 5 minutes", f"{ticker}/{year}"
    except Exception as e:
        return False, f"error: {str(e)}", f"{ticker}/{year}"

def check_duckdb_available() -> bool:
    """Check if DuckDB is available in the system."""
    try:
        result = subprocess.run(["duckdb", "--version"], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def main():
    parser = argparse.ArgumentParser(description="Coalesce unadjusted minute data into single files per partition")
    parser.add_argument("--base", default="~/fomo_strategy", help="Project base folder")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing coalesced files")
    parser.add_argument("--print-every", type=int, default=10, help="Print progress every N partitions")
    args = parser.parse_args()
    
    # Setup paths
    base = Path(args.base).expanduser().resolve()
    input_root = base / "2_unadjusted_parquet" / "minute_data"
    output_root = base / "2_unadjusted_parquet_coalesced" / "minute_data"
    
    # Check if DuckDB is available
    if not check_duckdb_available():
        logger.error("DuckDB is not available. Please install DuckDB first:")
        logger.error("  brew install duckdb  # macOS")
        logger.error("  # or download from https://duckdb.org/docs/installation/")
        sys.exit(1)
    
    # Check input directory exists
    if not input_root.exists():
        logger.error(f"Input directory does not exist: {input_root}")
        sys.exit(1)
    
    logger.info(f"Base: {base}")
    logger.info(f"Input: {input_root}")
    logger.info(f"Output: {output_root}")
    logger.info(f"Workers: {args.workers}")
    
    # Find all partitions
    logger.info("Scanning partitions...")
    partitions = find_partitions(input_root)
    
    if not partitions:
        logger.error(f"No partitions found in {input_root}")
        sys.exit(1)
    
    logger.info(f"Found {len(partitions):,} (ticker, year) partitions")
    
    # Create output root directory
    output_root.mkdir(parents=True, exist_ok=True)
    
    # Prepare work items
    work_items = []
    for ticker, year, input_path in partitions:
        work_items.append((ticker, year, input_path, output_root, args.overwrite))
    
    # Process partitions in parallel
    start = time.time()
    errors = 0
    processed = 0
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_item = {executor.submit(coalesce_partition_worker, item): item for item in work_items}
        
        # Process completed tasks
        for future in as_completed(future_to_item):
            processed += 1
            ok, msg, partition_info = future.result()
            
            if not ok and not msg.startswith("SKIP"):
                errors += 1
                logger.error(f"Error processing {partition_info}: {msg}")
            
            # Print progress
            if (processed % args.print_every == 0) or (processed == len(partitions)) or (not ok):
                pct = 100.0 * processed / len(partitions)
                elapsed = time.time() - start
                rate = processed / max(elapsed, 1e-6)
                eta = (len(partitions) - processed) / max(rate, 1e-6)
                print(f"[{processed:,}/{len(partitions):,}] {pct:6.2f}% | {msg} | {rate:.2f} partitions/s ETA {eta/60:.1f} min", flush=True)
    
    elapsed = time.time() - start
    logger.info(f"Coalescing complete!")
    logger.info(f"Partitions: {len(partitions):,}, Errors: {errors}, Time: {elapsed/60:.1f} min")
    logger.info(f"Output: {output_root}")
    
    # Print summary statistics
    input_files = sum(len(list(p.glob("*.parquet"))) for _, _, p in partitions)
    output_files = len(partitions)
    compression_ratio = input_files / max(output_files, 1)
    
    logger.info(f"File reduction: {input_files:,} → {output_files:,} files ({compression_ratio:.1f}x fewer files)")
    logger.info(f"Next step: Update your split adjustment script to use the coalesced data")

if __name__ == "__main__":
    sys.exit(main()) 