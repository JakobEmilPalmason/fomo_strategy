# comprehensive_data_validation.py
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

def clean_for_json(obj):
    """Recursively clean objects for JSON serialization"""
    if isinstance(obj, dict):
        return {str(k): clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif hasattr(obj, 'isoformat'):  # datetime objects
        return obj.isoformat()
    elif isinstance(obj, (set, frozenset)):
        return list(obj)
    else:
        return obj

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (tuple, set)):
            return list(obj)
        if hasattr(obj, 'isoformat'):  # datetime objects
            return obj.isoformat()
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        return super().default(obj)

class ComprehensiveDataValidator:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.minute_path = self.base_path / "2_unadjusted_parquet/minute_data"
        self.daily_path = self.base_path / "2_unadjusted_parquet/daily_data"
        self.validation_results = {}
        
        # Load market calendar (US trading days)
        self.trading_days = self._get_trading_days()
        
    def _get_trading_days(self):
        """Get list of valid US trading days"""
        import pandas_market_calendars as mcal
        
        # Get NYSE calendar
        nyse = mcal.get_calendar('NYSE')
        
        # Get valid trading days from 2015 to present
        schedule = nyse.schedule(start_date='2015-07-27', end_date=datetime.now())
        
        return set(pd.to_datetime(schedule.index).date)
    
    def run_full_validation(self):
        """Run all validation checks"""
        print("🔍 Starting comprehensive data validation...\n")
        
        # 1. File completeness
        self.validate_file_completeness()
        
        # 2. Date coverage and gaps
        self.validate_date_coverage()
        
        # 3. Minute data integrity
        self.validate_minute_data_integrity()
        
        # 4. Data consistency
        self.validate_data_consistency()
        
        # 5. Volume validation
        self.validate_volume_integrity()
        
        # 6. Price sanity checks (excluding splits)
        self.validate_price_sanity()
        
        # 7. Ticker coverage
        self.validate_ticker_coverage()
        
        # 8. Trading hours validation
        self.validate_trading_hours()
        
        # 9. Data freshness
        self.validate_data_freshness()
        
        # Generate report
        self.generate_validation_report()
    
    def validate_file_completeness(self):
        """Check if all expected files exist"""
        print("1️⃣ Validating file completeness...")
        
        issues = []
        stats = {}
        
        # Check minute data
        minute_folders = list(self.minute_path.glob("ticker=*/"))
        stats['total_minute_tickers'] = len(minute_folders)
        
        # Sample check for file organization
        empty_folders = []
        file_counts = []
        
        for folder in minute_folders[:100]:  # Sample first 100
            files = list(folder.glob("*.parquet"))
            if len(files) == 0:
                empty_folders.append(folder.name)
            else:
                file_counts.append(len(files))
        
        stats['empty_ticker_folders'] = len(empty_folders)
        stats['avg_files_per_ticker'] = np.mean(file_counts) if file_counts else 0
        
        # Check daily data
        daily_file = self.daily_path / "daily_ohlcv_2015_present.parquet"
        stats['daily_file_exists'] = daily_file.exists()
        stats['daily_file_size_mb'] = daily_file.stat().st_size / 1024 / 1024 if daily_file.exists() else 0
        
        self.validation_results['file_completeness'] = stats
    
    def validate_date_coverage(self):
        """Check for missing dates and gaps"""
        print("\n2️⃣ Validating date coverage...")
        
        daily_file = self.daily_path / "daily_ohlcv_2015_present.parquet"
        df = pd.read_parquet(daily_file)
        df['date'] = pd.to_datetime(df['date'])
        
        stats = {}
        
        # Overall date range
        stats['start_date'] = str(df['date'].min().date())
        stats['end_date'] = str(df['date'].max().date())
        stats['total_unique_dates'] = df['date'].dt.date.nunique()
        
        # Get unique dates in data
        data_dates = set(df['date'].dt.date.unique())
        
        # Find missing trading days
        expected_start = datetime.strptime('2015-07-27', '%Y-%m-%d').date()
        expected_end = min(datetime.now().date(), df['date'].max().date())
        
        expected_trading_days = {d for d in self.trading_days 
                                if expected_start <= d <= expected_end}
        
        missing_dates = expected_trading_days - data_dates
        extra_dates = data_dates - expected_trading_days
        
        stats['expected_trading_days'] = len(expected_trading_days)
        stats['missing_trading_days'] = len(missing_dates)
        stats['unexpected_dates'] = len(extra_dates)
        
        # Sample of missing dates
        if missing_dates:
            stats['sample_missing_dates'] = [str(d) for d in sorted(missing_dates)[:10]]
        
        # Check for weekend data
        weekend_dates = [d for d in data_dates if d.weekday() in [5, 6]]
        stats['weekend_dates_count'] = len(weekend_dates)
        
        # Per-ticker analysis (sample)
        sample_tickers = df['ticker'].value_counts().head(10).index
        ticker_coverage = {}
        
        for ticker in sample_tickers:
            ticker_dates = set(df[df['ticker'] == ticker]['date'].dt.date.unique())
            ticker_missing = expected_trading_days - ticker_dates
            ticker_coverage[ticker] = {
                'total_days': len(ticker_dates),
                'missing_days': len(ticker_missing),
                'coverage_pct': len(ticker_dates) / len(expected_trading_days) * 100
            }
        
        stats['sample_ticker_coverage'] = ticker_coverage
        
        self.validation_results['date_coverage'] = stats
    
    def validate_minute_data_integrity(self):
        """Check minute data quality"""
        print("\n3️⃣ Validating minute data integrity...")
        
        stats = {
            'tickers_checked': 0,
            'issues': [],
            'minute_gaps': [],
            'data_quality': {}
        }
        
        # Sample 50 random tickers
        ticker_folders = list(self.minute_path.glob("ticker=*/"))
        sample_folders = np.random.choice(ticker_folders, 
                                        min(50, len(ticker_folders)), 
                                        replace=False)
        
        for folder in sample_folders:
            ticker = folder.name.split('=')[1]
            stats['tickers_checked'] += 1
            
            try:
                # Read all files for this ticker
                all_files = sorted(folder.glob("*.parquet"))
                if not all_files:
                    stats['issues'].append(f"{ticker}: No data files")
                    continue
                
                # Read a sample file
                sample_df = pd.read_parquet(all_files[-1])  # Most recent
                
                # Check columns
                expected_cols = ['ts', 'open', 'high', 'low', 'close', 'volume']
                missing_cols = [col for col in expected_cols if col not in sample_df.columns]
                if missing_cols:
                    stats['issues'].append(f"{ticker}: Missing columns {missing_cols}")
                
                # Check data types
                if 'ts' in sample_df.columns:
                    sample_df['ts'] = pd.to_datetime(sample_df['ts'])
                    sample_df['date'] = sample_df['ts'].dt.date
                    sample_df['time'] = sample_df['ts'].dt.time
                    
                    # Check for minute gaps during trading hours
                    trading_minutes = sample_df[
                        (sample_df['time'] >= pd.Timestamp('09:30').time()) &
                        (sample_df['time'] <= pd.Timestamp('16:00').time())
                    ].copy()
                    
                    if len(trading_minutes) > 1:
                        trading_minutes = trading_minutes.sort_values('ts')
                        time_diffs = trading_minutes['ts'].diff()
                        
                        # Find gaps > 2 minutes
                        large_gaps = time_diffs[time_diffs > pd.Timedelta(minutes=2)]
                        if len(large_gaps) > 0:
                            stats['minute_gaps'].append({
                                'ticker': ticker,
                                'gap_count': len(large_gaps),
                                'max_gap_minutes': large_gaps.max().total_seconds() / 60
                            })
                
                # Data quality checks
                quality_checks = {
                    'null_values': sample_df.isnull().sum().sum(),
                    'zero_prices': ((sample_df[['open', 'high', 'low', 'close']] == 0).sum().sum()),
                    'negative_volume': (sample_df['volume'] < 0).sum() if 'volume' in sample_df else 0,
                    'ohlc_violations': len(sample_df[
                        (sample_df['high'] < sample_df['low']) |
                        (sample_df['open'] > sample_df['high']) |
                        (sample_df['open'] < sample_df['low']) |
                        (sample_df['close'] > sample_df['high']) |
                        (sample_df['close'] < sample_df['low'])
                    ])
                }
                
                if any(quality_checks.values()):
                    stats['data_quality'][ticker] = quality_checks
                    
            except Exception as e:
                stats['issues'].append(f"{ticker}: Error reading data - {str(e)}")
        
        self.validation_results['minute_data_integrity'] = stats
    
    def validate_data_consistency(self):
        """Check consistency between minute and daily data"""
        print("\n4️⃣ Validating data consistency...")
        
        consistency_checks = []
        
        # Load daily data
        daily_file = self.daily_path / "daily_ohlcv_2015_present.parquet"
        daily_df = pd.read_parquet(daily_file)
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        # Pick 10 random date-ticker combinations
        recent_data = daily_df[daily_df['date'] >= '2024-01-01']
        sample_records = recent_data.sample(min(10, len(recent_data)))
        
        for _, record in sample_records.iterrows():
            ticker = record['ticker']
            date = record['date'].date()
            
            ticker_path = self.minute_path / f"ticker={ticker}"
            if not ticker_path.exists():
                continue
            
            try:
                # Find the file containing this date
                for file_path in ticker_path.glob("*.parquet"):
                    minute_df = pd.read_parquet(file_path)
                    minute_df['ts'] = pd.to_datetime(minute_df['ts'])
                    minute_df['date'] = minute_df['ts'].dt.date
                    
                    if date in minute_df['date'].values:
                        # Filter for this date and regular hours
                        day_minutes = minute_df[
                            (minute_df['date'] == date) &
                            (minute_df['ts'].dt.time >= pd.Timestamp('09:30').time()) &
                            (minute_df['ts'].dt.time <= pd.Timestamp('16:00').time())
                        ]
                        
                        if len(day_minutes) > 0:
                            # Aggregate minute to daily
                            minute_agg = {
                                'open': day_minutes.iloc[0]['open'],
                                'high': day_minutes['high'].max(),
                                'low': day_minutes['low'].min(),
                                'close': day_minutes.iloc[-1]['close'],
                                'volume': day_minutes['volume'].sum(),
                                'minute_count': len(day_minutes)
                            }
                            
                            # Compare
                            check = {
                                'ticker': ticker,
                                'date': str(date),
                                'minute_count': minute_agg['minute_count'],
                                'open_match': abs(minute_agg['open'] - record['open']) < 0.01,
                                'high_match': abs(minute_agg['high'] - record['high']) < 0.01,
                                'low_match': abs(minute_agg['low'] - record['low']) < 0.01,
                                'close_match': abs(minute_agg['close'] - record['close']) < 0.01,
                                'volume_diff_pct': abs(minute_agg['volume'] - record['volume']) / record['volume'] * 100 if record['volume'] > 0 else 0
                            }
                            
                            consistency_checks.append(check)
                            break
                            
            except Exception as e:
                consistency_checks.append({
                    'ticker': ticker,
                    'date': str(date),
                    'error': str(e)
                })
        
        self.validation_results['data_consistency'] = consistency_checks
    
    def validate_volume_integrity(self):
        """Validate volume data quality"""
        print("\n5️⃣ Validating volume integrity...")
        
        daily_df = pd.read_parquet(self.daily_path / "daily_ohlcv_2015_present.parquet")
        
        stats = {
            'zero_volume_records': len(daily_df[daily_df['volume'] == 0]),
            'negative_volume_records': len(daily_df[daily_df['volume'] < 0]),
            'null_volume_records': daily_df['volume'].isnull().sum(),
            'extreme_volume_records': len(daily_df[daily_df['volume'] > daily_df['volume'].quantile(0.9999)])
        }
        
        # Volume distribution by year
        daily_df['year'] = pd.to_datetime(daily_df['date']).dt.year
        yearly_stats = daily_df.groupby('year').agg({
            'volume': ['mean', 'median', 'std', 'min', 'max']
        }).round(0)
        
        stats['yearly_volume_stats'] = yearly_stats.reset_index().to_dict(orient='records')
        
        # Tickers with chronic zero volume
        zero_vol_by_ticker = daily_df[daily_df['volume'] == 0].groupby('ticker').size()
        chronic_zero_vol = zero_vol_by_ticker[zero_vol_by_ticker > 50]
        
        stats['tickers_with_frequent_zero_volume'] = len(chronic_zero_vol)
        stats['worst_zero_volume_tickers'] = chronic_zero_vol.head(10).to_dict()
        
        self.validation_results['volume_integrity'] = stats
    
    def validate_price_sanity(self):
        """Check for price anomalies (excluding splits)"""
        print("\n6️⃣ Validating price sanity...")
        
        daily_df = pd.read_parquet(self.daily_path / "daily_ohlcv_2015_present.parquet")
        
        stats = {
            'negative_prices': len(daily_df[
                (daily_df['open'] < 0) | (daily_df['high'] < 0) | 
                (daily_df['low'] < 0) | (daily_df['close'] < 0)
            ]),
            'zero_prices': len(daily_df[
                (daily_df['open'] == 0) | (daily_df['high'] == 0) | 
                (daily_df['low'] == 0) | (daily_df['close'] == 0)
            ]),
            'ohlc_violations': len(daily_df[
                (daily_df['high'] < daily_df['low']) |
                (daily_df['open'] > daily_df['high']) |
                (daily_df['open'] < daily_df['low']) |
                (daily_df['close'] > daily_df['high']) |
                (daily_df['close'] < daily_df['low'])
            ]),
            'identical_ohlc': len(daily_df[
                (daily_df['open'] == daily_df['high']) & 
                (daily_df['high'] == daily_df['low']) & 
                (daily_df['low'] == daily_df['close']) &
                (daily_df['volume'] > 0)  # But volume exists
            ])
        }
        
        # Price distribution checks
        price_stats = {
            'penny_stocks': len(daily_df[daily_df['close'] < 1]),
            'high_price_stocks': len(daily_df[daily_df['close'] > 1000]),
            'price_percentiles': {
                '1%': daily_df['close'].quantile(0.01),
                '25%': daily_df['close'].quantile(0.25),
                '50%': daily_df['close'].quantile(0.50),
                '75%': daily_df['close'].quantile(0.75),
                '99%': daily_df['close'].quantile(0.99)
            }
        }
        
        stats['price_distribution'] = price_stats
        
        self.validation_results['price_sanity'] = stats
    
    def validate_ticker_coverage(self):
        """Validate ticker coverage and lifecycles"""
        print("\n7️⃣ Validating ticker coverage...")
        
        daily_df = pd.read_parquet(self.daily_path / "daily_ohlcv_2015_present.parquet")
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        # Ticker statistics
        ticker_stats = daily_df.groupby('ticker').agg({
            'date': ['min', 'max', 'count'],
            'volume': 'mean'
        })
        
        ticker_stats.columns = ['first_date', 'last_date', 'total_days', 'avg_volume']
        ticker_stats['days_active'] = (ticker_stats['last_date'] - ticker_stats['first_date']).dt.days
        
        stats = {
            'total_unique_tickers': len(ticker_stats),
            'active_tickers': len(ticker_stats[ticker_stats['last_date'] >= '2024-01-01']),
            'delisted_tickers': len(ticker_stats[ticker_stats['last_date'] < '2024-01-01']),
            'new_listings_2024': len(ticker_stats[ticker_stats['first_date'] >= '2024-01-01'])
        }
        
        # Lifecycle distribution
        lifecycle_buckets = {
            'less_than_30_days': len(ticker_stats[ticker_stats['days_active'] < 30]),
            '30_to_365_days': len(ticker_stats[(ticker_stats['days_active'] >= 30) & 
                                               (ticker_stats['days_active'] < 365)]),
            '1_to_5_years': len(ticker_stats[(ticker_stats['days_active'] >= 365) & 
                                             (ticker_stats['days_active'] < 1825)]),
            'more_than_5_years': len(ticker_stats[ticker_stats['days_active'] >= 1825])
        }
        
        stats['lifecycle_distribution'] = lifecycle_buckets
        
        # Sample of short-lived tickers
        short_lived = ticker_stats[ticker_stats['days_active'] < 30].head(10)
        stats['sample_short_lived_tickers'] = short_lived[['first_date', 'last_date', 'days_active']].to_dict(orient='records')
        
        self.validation_results['ticker_coverage'] = stats
    
    def validate_trading_hours(self):
        """Validate that minute data is within trading hours"""
        print("\n8️⃣ Validating trading hours...")
        
        stats = {
            'tickers_checked': 0,
            'pre_market_data': [],
            'after_hours_data': [],
            'overnight_data': []
        }
        
        # Sample 20 random tickers
        ticker_folders = list(self.minute_path.glob("ticker=*/"))
        sample_folders = np.random.choice(ticker_folders, 
                                        min(20, len(ticker_folders)), 
                                        replace=False)
        
        for folder in sample_folders:
            ticker = folder.name.split('=')[1]
            stats['tickers_checked'] += 1
            
            try:
                # Read most recent file
                files = sorted(folder.glob("*.parquet"))
                if not files:
                    continue
                    
                df = pd.read_parquet(files[-1])
                df['ts'] = pd.to_datetime(df['ts'])
                df['time'] = df['ts'].dt.time
                
                # Count bars by time period
                pre_market = len(df[(df['time'] >= pd.Timestamp('04:00').time()) & 
                                   (df['time'] < pd.Timestamp('09:30').time())])
                regular = len(df[(df['time'] >= pd.Timestamp('09:30').time()) & 
                                (df['time'] <= pd.Timestamp('16:00').time())])
                after_hours = len(df[(df['time'] > pd.Timestamp('16:00').time()) & 
                                    (df['time'] <= pd.Timestamp('20:00').time())])
                overnight = len(df[(df['time'] > pd.Timestamp('20:00').time()) | 
                                  (df['time'] < pd.Timestamp('04:00').time())])
                
                if pre_market > 0:
                    stats['pre_market_data'].append({'ticker': ticker, 'bars': pre_market})
                if after_hours > 0:
                    stats['after_hours_data'].append({'ticker': ticker, 'bars': after_hours})
                if overnight > 0:
                    stats['overnight_data'].append({'ticker': ticker, 'bars': overnight})
                    
            except Exception as e:
                pass
        
        self.validation_results['trading_hours'] = stats
    
    def validate_data_freshness(self):
        """Check how recent the data is"""
        print("\n9️⃣ Validating data freshness...")
        
        daily_df = pd.read_parquet(self.daily_path / "daily_ohlcv_2015_present.parquet")
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        latest_date = daily_df['date'].max()
        days_behind = (datetime.now().date() - latest_date.date()).days
        
        stats = {
            'latest_date_in_daily': str(latest_date.date()),
            'days_behind_current': days_behind,
            'is_current': days_behind <= 5  # Allow for weekends/holidays
        }
        
        # Check latest date by ticker (sample)
        latest_by_ticker = daily_df.groupby('ticker')['date'].max()
        active_tickers = latest_by_ticker[latest_by_ticker >= latest_date - timedelta(days=5)]
        
        stats['tickers_with_recent_data'] = len(active_tickers)
        stats['tickers_stale'] = len(latest_by_ticker) - len(active_tickers)
        
        self.validation_results['data_freshness'] = stats
    
    def generate_validation_report(self):
        """Generate validation report"""
        print("\n📊 Generating validation report...")
        
        report_path = self.base_path / "data_quality_validation_report.json"
        
        # Add summary
        self.validation_results['summary'] = {
            'validation_date': datetime.now().isoformat(),
            'data_path': str(self.base_path),
            'validation_type': 'Data Quality (Excluding Splits)'
        }
        
        # Determine overall health
        critical_issues = []
        warnings = []
        
        # Check for critical issues
        date_coverage = self.validation_results.get('date_coverage', {})
        if date_coverage.get('missing_trading_days', 0) > 10:
            critical_issues.append(f"Missing {date_coverage['missing_trading_days']} trading days")
        
        consistency = self.validation_results.get('data_consistency', [])
        failed_checks = [c for c in consistency if not c.get('close_match', True)]
        if len(failed_checks) > len(consistency) * 0.2:  # >20% mismatch
            critical_issues.append("Significant minute/daily data mismatch")
        
        # Check for warnings
        volume_stats = self.validation_results.get('volume_integrity', {})
        if volume_stats.get('zero_volume_records', 0) > 10000:
            warnings.append(f"High number of zero volume records: {volume_stats['zero_volume_records']}")
        
        minute_integrity = self.validation_results.get('minute_data_integrity', {})
        if len(minute_integrity.get('minute_gaps', [])) > 5:
            warnings.append(f"Minute data gaps found in {len(minute_integrity['minute_gaps'])} tickers")
        
        self.validation_results['summary']['critical_issues'] = critical_issues
        self.validation_results['summary']['warnings'] = warnings
        self.validation_results['summary']['overall_status'] = (
            'CRITICAL' if critical_issues else
            'WARNING' if warnings else
            'GOOD'
        )
        
        # Clean data for JSON serialization
        cleaned_results = clean_for_json(self.validation_results)
        
        # Save report
        with open(report_path, 'w') as f:
            json.dump(cleaned_results, f, indent=2, cls=CustomJSONEncoder)
        
        print(f"\n✅ Validation complete! Report saved to: {report_path}")
        self.print_summary()
    
    def print_summary(self):
        """Print validation summary"""
        print("\n" + "="*70)
        print("DATA QUALITY VALIDATION SUMMARY")
        print("="*70)
        
        # File completeness
        fc = self.validation_results.get('file_completeness', {})
        print(f"\n📁 File Completeness:")
        print(f"   - Total minute tickers: {fc.get('total_minute_tickers', 'N/A')}")
        print(f"   - Daily file size: {fc.get('daily_file_size_mb', 0):.1f} MB")
        
        # Date coverage
        dc = self.validation_results.get('date_coverage', {})
        print(f"\n📅 Date Coverage:")
        print(f"   - Date range: {dc.get('start_date', 'N/A')} to {dc.get('end_date', 'N/A')}")
        print(f"   - Missing trading days: {dc.get('missing_trading_days', 'N/A')}")
        print(f"   - Weekend dates found: {dc.get('weekend_dates_count', 0)}")
        
        # Minute data
        mi = self.validation_results.get('minute_data_integrity', {})
        print(f"\n⏱️  Minute Data Integrity:")
        print(f"   - Tickers checked: {mi.get('tickers_checked', 0)}")
        print(f"   - Tickers with gaps: {len(mi.get('minute_gaps', []))}")
        print(f"   - Data quality issues: {len(mi.get('data_quality', {}))}")
        
        # Volume
        vi = self.validation_results.get('volume_integrity', {})
        print(f"\n📊 Volume Integrity:")
        print(f"   - Zero volume records: {vi.get('zero_volume_records', 0):,}")
        print(f"   - Tickers with frequent zero volume: {vi.get('tickers_with_frequent_zero_volume', 0)}")
        
        # Ticker coverage
        tc = self.validation_results.get('ticker_coverage', {})
        print(f"\n🏢 Ticker Coverage:")
        print(f"   - Total unique tickers: {tc.get('total_unique_tickers', 0):,}")
        print(f"   - Currently active: {tc.get('active_tickers', 0):,}")
        print(f"   - Delisted: {tc.get('delisted_tickers', 0):,}")
        
        # Data freshness
        df = self.validation_results.get('data_freshness', {})
        print(f"\n🕐 Data Freshness:")
        print(f"   - Latest date: {df.get('latest_date_in_daily', 'N/A')}")
        print(f"   - Days behind: {df.get('days_behind_current', 'N/A')}")
        
        # Overall status
        summary = self.validation_results.get('summary', {})
        status = summary.get('overall_status', 'UNKNOWN')
        
        print(f"\n🎯 Overall Status: {status}")
        
        if summary.get('critical_issues'):
            print("\n❌ CRITICAL ISSUES:")
            for issue in summary['critical_issues']:
                print(f"   - {issue}")
        
        if summary.get('warnings'):
            print("\n⚠️  WARNINGS:")
            for warning in summary['warnings']:
                print(f"   - {warning}")
        
        print("\n" + "="*70)

# Run validation
if __name__ == "__main__":
    validator = ComprehensiveDataValidator("/Users/solmate/fomo_strategy")
    validator.run_full_validation()