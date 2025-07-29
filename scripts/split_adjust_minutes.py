#!/usr/bin/env python
# Split-adjust minute bars with visible % progress and ETA.
# Input (unadjusted):  ~/fomo_strategy/2_unadjusted_parquet/minute_data/ticker=.../year=.../part-*.parquet
# Splits source:       ~/fomo_strategy/1_raw_files/reference/splits.parquet  (preferred)
#                      If missing, fetch from Polygon REST using keys.py
# Output (adjusted):   ~/fomo_strategy/3_adjusted_data/minute_sa/vYYYY-MM-DD/ticker=.../year=.../part-*.parquet
#
# Usage:
#   python split_adjust_minutes.py --base ~/fomo_strategy
# Options:
#   --fetch-if-missing    Fetch splits from Polygon REST if local file absent
#   --overwrite           Overwrite existing adjusted files (default is skip existing parts)
#   --print-every N       Update progress every N files (default 10)
#   --workers N           Number of parallel workers (default: CPU count)
#   --batch-size N        Number of files to process in each batch (default 2000)

from __future__ import annotations
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
from dotenv import load_dotenv

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def get_api_key() -> str:
    """Get Polygon API key from environment."""
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise ValueError("POLYGON_API_KEY environment variable is required")
    return api_key

# ---------- Splits loading / building factors ----------
def load_or_fetch_splits(base: Path, fetch_if_missing: bool) -> pd.DataFrame:
    """Return DataFrame with columns: ticker, date (datetime64[D]), split_step (float)."""
    p = base / "1_raw_files" / "reference" / "splits.parquet"
    
    if p.exists():
        logger.info(f"Loading splits from {p}")
        s = pd.read_parquet(p)
    elif fetch_if_missing:
        api_key = get_api_key()
        logger.info("Fetching splits from Polygon REST API...")
        s = _fetch_splits_from_polygon(api_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        s.to_parquet(p, index=False)
        # Also save raw copy for adjustment check
        raw_p = base / "1_raw_files" / "reference" / "splits_raw.parquet"
        s.to_parquet(raw_p, index=False)
        logger.info(f"Saved splits to {p} and raw copy to {raw_p}")
    else:
        raise FileNotFoundError(f"Splits parquet not found: {p} (pass --fetch-if-missing to pull from Polygon REST)")

    # Normalize schema
    cols = {c.lower(): c for c in s.columns}
    t = cols.get("ticker", "ticker")
    d = cols.get("execution_date") or cols.get("ex_date") or cols.get("date") or "execution_date"
    to = cols.get("split_to") or "split_to"
    fr = cols.get("split_from") or "split_from"
    
    if not all(c in s.columns for c in [t, d, to, fr]):
        raise ValueError(f"Unexpected splits schema: {s.columns.tolist()}")
    
    s = s[[t, d, to, fr]].rename(columns={t: "ticker", d: "date", to: "split_to", fr: "split_from"})
    s["date"] = pd.to_datetime(s["date"], errors="coerce")
    s = s.dropna(subset=["ticker", "date", "split_to", "split_from"])
    s = s[(s["split_to"] > 0) & (s["split_from"] > 0)]
    s["split_step"] = 1.0 / (s["split_to"] / s["split_from"])  # back-adjust step
    s = s.sort_values(["ticker", "date"]).reset_index(drop=True)
    
    logger.info(f"Loaded {len(s):,} split events for {s['ticker'].nunique():,} tickers")
    return s[["ticker", "date", "split_step"]]

def _fetch_splits_from_polygon(api_key: str) -> pd.DataFrame:
    """Fetch splits from Polygon REST API with proper error handling."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    rows, pages = [], 0
    url = f"https://api.polygon.io/v3/reference/splits?limit=1000&order=asc&apiKey={api_key}"
    
    try:
        while True:
            r = session.get(url, timeout=60)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "2"))
                logger.info(f"Rate limited, sleeping {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            
            j = r.json()
            rows.extend(j.get("results", []))
            pages += 1
            
            if pages % 25 == 0:
                logger.info(f"Fetched {pages} pages, {len(rows):,} splits so far")
            
            next_url = j.get("next_url")
            if not next_url:
                break
            if "apiKey=" not in next_url:
                next_url += ("&" if "?" in next_url else "?") + f"apiKey={api_key}"
            url = next_url
            
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch splits from Polygon API: {e}")
    except ValueError as e:
        raise RuntimeError(f"Invalid JSON response from Polygon API: {e}")
    
    df = pd.DataFrame(rows)
    expect = {"ticker", "execution_date", "split_to", "split_from"}
    if not expect.issubset(df.columns):
        raise RuntimeError(f"Unexpected splits payload columns: {df.columns.tolist()}")
    
    logger.info(f"Successfully fetched {len(df):,} splits from Polygon API")
    return df

def build_future_factor(splits: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-ticker 'future' cumulative factor so that a split on date D
    does NOT affect bars on D (only dates strictly < D are adjusted).
    Returns columns: ticker, change_dt, post_factor
    """
    if splits.empty:
        return pd.DataFrame(columns=["ticker", "change_dt", "post_factor"])
    
    out = []
    for tkr, g in splits.groupby("ticker", sort=False):
        g = g.sort_values("date").copy()
        # reverse cumprod of split_step to get product of all steps strictly AFTER a given date
        rev = g["split_step"].iloc[::-1].cumprod().iloc[::-1].values
        gg = pd.DataFrame({"ticker": tkr, "change_dt": g["date"].values, "post_factor": rev})
        out.append(gg)
    
    fac = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["ticker", "change_dt", "post_factor"])
    return fac.sort_values(["ticker", "change_dt"]).reset_index(drop=True)

# ---------- Minute adjustment ----------
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

def clear_partition_if_overwrite(fp: Path, tkr: str, out_root: Path, overwrite: bool) -> None:
    """Clear the destination partition if overwriting to prevent duplicates."""
    if not overwrite:
        return
    
    dest_dir = get_destination_path(fp, tkr, out_root)
    if dest_dir.exists():
        try:
            # Remove all parquet files in the partition directory
            for parquet_file in dest_dir.glob("*.parquet"):
                parquet_file.unlink()
            logger.debug(f"Cleared partition {dest_dir} for overwrite")
        except Exception as e:
            logger.warning(f"Failed to clear partition {dest_dir}: {e}")

def clear_all_partitions_if_overwrite(parts: list[Path], out_root: Path, overwrite: bool) -> None:
    """Clear all destination partitions once before processing to prevent race conditions."""
    if not overwrite:
        return
    
    # Get unique (ticker, year) partitions
    partitions_to_clear = set()
    for fp in parts:
        tkr = parse_ticker_from_path(fp) or ""
        year = parse_year_from_path(fp)
        if year:
            partitions_to_clear.add((tkr, year))
    
    # Clear each partition once
    for tkr, year in partitions_to_clear:
        dest_dir = out_root / f"ticker={tkr}" / f"year={year}"
        if dest_dir.exists():
            try:
                # Remove all parquet files in the partition directory
                for parquet_file in dest_dir.glob("*.parquet"):
                    parquet_file.unlink()
                logger.info(f"Cleared partition {dest_dir} for overwrite")
            except Exception as e:
                logger.warning(f"Failed to clear partition {dest_dir}: {e}")
    
    logger.info(f"Cleared {len(partitions_to_clear)} partitions for overwrite mode")

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

def check_if_already_adjusted(base: Path, splits: pd.DataFrame) -> bool:
    """
    Check if input data is already split-adjusted by testing against a known split event.
    Returns True if data appears to be already adjusted.
    """
    # accept raw (execution_date, split_to, split_from) **or** normalized (date, split_step)
    cols = {c.lower(): c for c in splits.columns}
    if {"execution_date","split_to","split_from","ticker"} <= set(cols):
        s = splits.rename(columns={
            cols["execution_date"]: "execution_date",
            cols["split_to"]: "split_to",
            cols["split_from"]: "split_from",
            cols["ticker"]: "ticker",
        }).copy()
        s["execution_date"] = pd.to_datetime(s["execution_date"], errors="coerce")
        s["ratio"] = s["split_to"] / s["split_from"]
    elif {"date","split_step","ticker"} <= set(cols):
        s = splits.rename(columns={
            cols["date"]: "execution_date",
            cols["split_step"]: "split_step",
            cols["ticker"]: "ticker",
        }).copy()
        s["execution_date"] = pd.to_datetime(s["execution_date"], errors="coerce")
        s["ratio"] = 1.0 / s["split_step"]
    else:
        raise ValueError(f"Unsupported splits schema: {splits.columns.tolist()}")
    
    # Known split event: AAPL 4:1 split on 2020-08-31
    test_ticker = "AAPL"
    test_split_date = pd.Timestamp("2020-08-31")
    test_split_ratio = 4.0  # 4:1 split
    
    # Find this split in our data
    aapl_splits = s[s["ticker"] == test_ticker]
    if aapl_splits.empty:
        logger.warning(f"No splits found for {test_ticker}, skipping adjustment check")
        return False
    
    # Look for splits around the known date
    relevant_splits = aapl_splits[
        (aapl_splits["execution_date"] >= test_split_date - pd.Timedelta(days=5)) &
        (aapl_splits["execution_date"] <= test_split_date + pd.Timedelta(days=5))
    ]
    
    if relevant_splits.empty:
        logger.warning(f"No splits found around {test_split_date} for {test_ticker}, skipping adjustment check")
        return False
    
    # Get the actual split ratio from our data
    actual_split = relevant_splits.iloc[0]
    actual_ratio = actual_split["ratio"]
    
    logger.info(f"Found {test_ticker} split: ratio {actual_ratio:.2f} on {actual_split['execution_date']}")
    
    # Check input data around the split date
    in_root = base / "2_unadjusted_parquet" / "minute_data"
    aapl_dir = in_root / f"ticker={test_ticker}" / "year=2020"
    aapl_files = list(aapl_dir.glob("*.parquet"))
    
    if not aapl_files:
        logger.warning(f"No {test_ticker} 2020 data found, skipping adjustment check")
        return False
    
    # Read a sample file around the split date
    sample_file = aapl_files[0]
    try:
        df = pd.read_parquet(sample_file)
        if "ts" not in df.columns or "close" not in df.columns:
            logger.warning(f"Sample file missing required columns: {sample_file}")
            return False
        
        df["ts"] = pd.to_datetime(df["ts"])
        # Handle both timezone-naive and timezone-aware timestamps safely
        if getattr(df["ts"].dtype, "tz", None) is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")
        df["ts_et"] = df["ts"].dt.tz_convert("America/New_York")
        df["date"] = df["ts_et"].dt.tz_localize(None).dt.normalize()
        
        # Get prices before and after the split
        pre_split = df[df["date"] < actual_split["execution_date"]]["close"]
        post_split = df[df["date"] > actual_split["execution_date"]]["close"]
        
        if pre_split.empty or post_split.empty:
            logger.warning(f"Insufficient data around split date for {test_ticker}")
            return False
        
        # Calculate price ratio
        pre_avg = pre_split.mean()
        post_avg = post_split.mean()
        price_ratio = pre_avg / post_avg if post_avg > 0 else 0
        
        logger.info(f"{test_ticker} price ratio (pre/post): {price_ratio:.2f}")
        logger.info(f"Expected ratio (split): {actual_ratio:.2f}")
        
        # Check if the ratio is close to the split ratio (indicating already adjusted)
        tolerance = 0.1  # 10% tolerance
        if abs(price_ratio - actual_ratio) < tolerance:
            logger.warning(f"⚠️  Data appears to be ALREADY ADJUSTED! Price ratio {price_ratio:.2f} ≈ split ratio {actual_ratio:.2f}")
            return True
        elif abs(price_ratio - 1.0) < tolerance:
            logger.info(f"✅ Data appears to be UNADJUSTED. Price ratio {price_ratio:.2f} ≈ 1.0")
            return False
        else:
            logger.warning(f"⚠️  Unexpected price ratio {price_ratio:.2f}. Manual verification recommended.")
            return False
            
    except Exception as e:
        logger.warning(f"Error checking adjustment status: {e}")
        return False

def adjust_file_worker(args: tuple) -> tuple[bool, str, str]:
    """
    Worker function for parallel processing.
    Returns (success, message, file_path).
    """
    fp, tkr, out_root, overwrite = args
    
    # Load factors for this ticker from disk
    factors_file = out_root / "_factors" / f"{tkr}.parquet"
    if factors_file.exists():
        fac_tkr = pd.read_parquet(factors_file)
    else:
        fac_tkr = pd.DataFrame(columns=["change_dt", "post_factor"])
    
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
        
        # Ensure proper data types and infer year if needed
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["ts"])
        
        # Convert timestamps to ET for proper date alignment with split execution dates
        # Handle both timezone-naive and timezone-aware timestamps safely
        if getattr(df["ts"].dtype, "tz", None) is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")
        df["ts_et"] = df["ts"].dt.tz_convert("America/New_York")
        df["date"] = df["ts_et"].dt.tz_localize(None).dt.normalize()  # naive ET midnight
        
        if "year" not in df.columns:
            df["year"] = df["ts_et"].dt.year.astype("int32")
        
        # Remove duplicate rows to prevent double counting
        # Use (ticker, ts) as the key for duplicate detection
        initial_rows = len(df)
        if "ticker" in df.columns:
            df = df.drop_duplicates(subset=["ticker", "ts"], keep="first")
        else:
            df = df.drop_duplicates(subset=["ts"], keep="first")
        
        duplicates_removed = initial_rows - len(df)
        if duplicates_removed > 0:
            logger.debug(f"Removed {duplicates_removed} duplicate rows from {fp}")
        
        # Apply split adjustments
        if not fac_tkr.empty:
            # Ensure both sides use the same datetime precision for merge_asof
            df["date"] = df["date"].astype("datetime64[ns]")
            fac = fac_tkr.copy()
            fac["change_dt"] = pd.to_datetime(fac["change_dt"], errors="coerce").astype("datetime64[ns]")
            
            # Sort for merge_asof - use ET date for proper alignment with split execution dates
            df = df.sort_values("date")
            fac = fac.sort_values("change_dt")
            
            # merge_asof with direction="forward" and allow_exact_matches=False ensures:
            # 1. Bars on the ex-date get factor=1.0 (no adjustment on split day)
            # 2. Only bars on dates strictly before the ex-date get the adjustment factor
            # 3. Multiple splits on the same ex-date are all excluded for that day (correct convention)
            df = pd.merge_asof(
                left=df, right=fac,
                left_on="date", right_on="change_dt",
                direction="forward", allow_exact_matches=False
            )
            df["post_factor"] = df["post_factor"].fillna(1.0)
        else:
            df["post_factor"] = 1.0
        
        # Type coercion guard: convert strings to numeric before astype
        for c in ("open", "high", "low", "close", "volume"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        
        # Apply adjustments: price * factor, volume / factor
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col in df.columns:
                df[col] = (df[col].astype("float64") * df["post_factor"].astype("float64")).round(6)
        
        if "volume" in df.columns:
            # Keep volume as float to preserve precision and avoid reconciliation issues
            # No rounding at minute level to ensure sum(minute_volumes) == daily_volume
            df["volume"] = df["volume"].astype("float64") / df["post_factor"].astype("float64")
        
        # Drop helper columns (ticker already added above)
        keep_cols = [c for c in df.columns if c not in ("change_dt", "post_factor", "date", "ts_et")]
        # keep_cols = ["ts", "open", "high", "low", "close", "volume", "year", "ticker"]  # Ticker included!

        # 3. Use keep_cols (ticker preserved)
        table = pa.Table.from_pandas(df[keep_cols], preserve_index=False)
        # ✅ Result: Ticker column in output, partitioning works
        
        # Write to dataset
        behavior = "delete_matching" if overwrite else "overwrite_or_ignore"
        pq.write_to_dataset(
            table,
            root_path=str(out_root),
            partition_cols=["ticker", "year"],
            existing_data_behavior=behavior
        )
        
        # Mark file as processed
        mark_file_processed(fp, out_root)
        
        return True, f"OK rows={len(df):,}", str(fp)
        
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
    ap = argparse.ArgumentParser(description="Split-adjust minute parquet with parallel processing and progress tracking.")
    ap.add_argument("--base", default="~/fomo_strategy", help="Project base folder")
    ap.add_argument("--fetch-if-missing", action="store_true", help="Fetch splits from REST if local file missing")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing adjusted parts")
    ap.add_argument("--print-every", type=int, default=10, help="Print progress every N files")
    ap.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of parallel workers")
    ap.add_argument("--batch-size", type=int, default=2000, help="Number of files to process in each batch")
    ap.add_argument("--skip-adjustment-check", action="store_true", help="Skip check for already-adjusted data")
    ap.add_argument("--years", type=str, help="Comma-separated list of years to process (e.g., '2025,2024,2023')")
    args = ap.parse_args()

    # Setup paths
    base = Path(args.base).expanduser().resolve()
    
    # Try coalesced data first, fall back to original if not available
    coalesced_in_root = base / "2_unadjusted_parquet_coalesced" / "minute_data"
    original_in_root = base / "2_unadjusted_parquet" / "minute_data"
    
    if coalesced_in_root.exists():
        in_root = coalesced_in_root
        logger.info("Using coalesced data (faster processing)")
    else:
        in_root = original_in_root
        logger.info("Using original data (consider running coalesce script first for better performance)")
    
    out_root = base / "3_adjusted_data" / "minute_sa" / ("v" + datetime.now().strftime("%Y-%m-%d"))
    out_root.mkdir(parents=True, exist_ok=True)

    # Clear manifest if overwriting
    clear_manifest_if_overwrite(out_root, args.overwrite)

    logger.info(f"Base: {base}")
    logger.info(f"Input: {in_root}")
    logger.info(f"Output: {out_root}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Batch size: {args.batch_size}")

    # Load splits and build factors
    splits = load_or_fetch_splits(base, fetch_if_missing=args.fetch_if_missing)
    factors = build_future_factor(splits)
    logger.info(f"Split change points: {len(factors):,} across {factors['ticker'].nunique():,} tickers")

    # Check if input data is already adjusted (unless skipped)
    if not args.skip_adjustment_check:
        logger.info("Checking if input data is already split-adjusted...")
        # Load raw splits data for the check
        raw_splits_file = base / "1_raw_files" / "reference" / "splits_raw.parquet"
        if raw_splits_file.exists():
            raw_splits = pd.read_parquet(raw_splits_file)
        else:
            # Fallback to normalized splits if raw not available
            raw_splits = pd.read_parquet(base / "1_raw_files" / "reference" / "splits.parquet")
        if check_if_already_adjusted(base, raw_splits):
            logger.error("❌ Input data appears to be ALREADY ADJUSTED! Aborting to prevent double-adjustment.")
            logger.error("Use --skip-adjustment-check to override this check.")
            sys.exit(1)
        logger.info("✅ Input data appears to be unadjusted. Proceeding with split adjustment.")

    # Save per-ticker factors to disk for efficient worker access
    factors_dir = out_root / "_factors"
    factors_dir.mkdir(exist_ok=True)
    
    fac_by_ticker = {t: g[["change_dt", "post_factor"]].copy() for t, g in factors.groupby("ticker", sort=False)}
    
    # Save each ticker's factors to a separate parquet file
    for tkr, fac_df in fac_by_ticker.items():
        # Ensure consistent datetime precision when saving factors
        fac_df["change_dt"] = fac_df["change_dt"].astype("datetime64[ns]")
        factor_file = factors_dir / f"{tkr}.parquet"
        fac_df.to_parquet(factor_file, index=False)
    
    logger.info(f"Saved factors for {len(fac_by_ticker):,} tickers to {factors_dir}")

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

    # Clear all partitions once before processing to prevent race conditions
    clear_all_partitions_if_overwrite(parts, out_root, args.overwrite)

    # Prepare work items (no longer passing factor DataFrames)
    work_items = []
    for fp in parts:
        tkr = parse_ticker_from_path(fp) or ""
        work_items.append((fp, tkr, out_root, args.overwrite))

    # Process files in parallel with batching to avoid memory issues
    start = time.time()
    errors = 0
    processed = 0
    
    # Use batching to avoid submitting millions of jobs at once
    BATCH_SIZE = args.batch_size
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for i in range(0, total, BATCH_SIZE):
            batch = work_items[i:i+BATCH_SIZE]
            batch_size = len(batch)
            
            logger.info(f"Submitting batch {i//BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1)//BATCH_SIZE} ({batch_size:,} files)")
            
            # Submit batch
            futures = [executor.submit(adjust_file_worker, item) for item in batch]
            
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
    logger.info(f"Processing complete!")
    logger.info(f"Files: {total:,}, Errors: {errors}, Time: {elapsed/60:.1f} min")
    logger.info(f"Output: {out_root}")

if __name__ == "__main__":
    # Pandas settings for safety and performance
    pd.options.mode.copy_on_write = True
    pd.options.mode.chained_assignment = None
    
    sys.exit(main())
