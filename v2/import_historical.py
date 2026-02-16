"""
Historical Data Import Script for BYD/Valley Tracking V2.0

Imports YTD historical data from bydhistorical.xlsx into Supabase
to provide baseline for trend analysis and historical comparisons.
"""

import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v2.data_processor import load_manual_export, process_data, calculate_kpis
from v2.supabase_client import SupabaseClient


def import_historical_data(filepath: str = 'bydhistorical.xlsx', snapshot_date: datetime = None):
    """
    Imports historical YTD data into Supabase.
    
    Args:
        filepath: Path to historical Excel export
        snapshot_date: Date to mark this snapshot (defaults to file's last modified date)
    """
    print("=" * 60)
    print("BYD/Valley Historical Data Import")
    print("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Determine snapshot date
    if snapshot_date is None:
        # Use file's last modified time
        file_mtime = os.path.getmtime(filepath)
        snapshot_date = datetime.fromtimestamp(file_mtime)
    
    print(f"\nImporting from: {filepath}")
    print(f"Snapshot date: {snapshot_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Load and process data
    try:
        df_raw = load_manual_export(filepath)
        df_processed = process_data(df_raw)
        print(f"✓ Processed {len(df_processed)} historical records")
    except Exception as e:
        print(f"❌ Error loading/processing data: {e}")
        return False
    
    # Step 2: Calculate KPIs for this historical period
    try:
        kpis = calculate_kpis(df_processed)
        print(f"\nHistorical KPI Summary:")
        print(f"  Total Jobs: {kpis['total_jobs']}")
        print(f"  On-Time %: {kpis['on_time_pct']:.1f}%")
        print(f"  Avg Delay: {kpis['avg_delay_days']:.1f} days")
        print(f"  Overdue: {kpis['overdue_count']}")
    except Exception as e:
        print(f"❌ Error calculating KPIs: {e}")
        return False
    
    # Step 3: Insert into Supabase
    try:
        print(f"\nConnecting to Supabase...")
        supabase = SupabaseClient()
        
        # Insert job snapshots
        inserted_count = supabase.insert_snapshot(df_processed, snapshot_date=snapshot_date)
        
        # Insert KPIs
        report_date = snapshot_date.date()
        supabase.insert_kpis(kpis, report_date=report_date)
        
        print(f"✓ Successfully imported {inserted_count} records to Supabase")
        print(f"✓ KPIs recorded for {report_date}")
        
    except Exception as e:
        print(f"❌ Error importing to Supabase: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("SUCCESS: Historical data import complete!")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Import historical BYD/Valley data to Supabase')
    parser.add_argument('--file', '-f', default='bydhistorical.xlsx', help='Path to historical export file')
    parser.add_argument('--date', '-d', help='Snapshot date (YYYY-MM-DD format)')
    
    args = parser.parse_args()
    
    # Parse snapshot date if provided
    snapshot_date = None
    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    success = import_historical_data(filepath=args.file, snapshot_date=snapshot_date)
    sys.exit(0 if success else 1)
