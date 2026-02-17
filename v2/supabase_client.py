"""
Supabase Integration Module

Handles all database operations for historical tracking and trend analysis.
"""

from supabase import create_client, Client
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import os


class SupabaseClient:
    """Client for interacting with Supabase database."""
    
    def __init__(self, url: str = None, key: str = None):
        """
        Initialize Supabase client.
        
        Args:
            url: Supabase project URL (or from SUPABASE_URL env var)
            key: Supabase anon key (or from SUPABASE_KEY env var)
        """
        self.url = url or os.getenv('SUPABASE_URL')
        self.key = key or os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and KEY must be provided or set in environment")
        
        self.client: Client = create_client(self.url, self.key)
    
    def insert_snapshot(self, df: pd.DataFrame, snapshot_date: datetime = None) -> int:
        """
        Inserts job snapshot into database.
        
        Args:
            df: Processed DataFrame with job data
            snapshot_date: Timestamp of snapshot (defaults to now)
            
        Returns:
            Number of records inserted
        """
        if snapshot_date is None:
            snapshot_date = datetime.now()
        
        records = []
        
        for _, row in df.iterrows():
            record = {
                'snapshot_date': snapshot_date.isoformat(),
                'job_id': str(row.get('Job_ID', '')),
                'planned_date': row.get('Planned_Date').isoformat() if pd.notna(row.get('Planned_Date')) else None,
                'actual_date': row.get('Actual_Date').isoformat() if pd.notna(row.get('Actual_Date')) else None,
                'delay_days': float(row.get('Delay_Days')) if pd.notna(row.get('Delay_Days')) else None,
                'status': str(row.get('Status', '')),
                'carrier': str(row.get('Carrier', '')),
                'state': str(row.get('State', '')),
                'scan_user': str(row.get('Scan_User', '')),
                'scan_timestamp': row.get('Scan_Timestamp').isoformat() if pd.notna(row.get('Scan_Timestamp')) else None,
                'product_description': str(row.get('Product_Description', '')),
                'piece_count': int(row.get('Piece_Count', 0)),
                'white_glove': bool(row.get('White_Glove', False)),
                'notification_detail': str(row.get('Notification_Detail', '')),
                'miles_oneway': float(row.get('Miles_OneWay', 0.0))
            }
            records.append(record)
        
        try:
            result = self.client.table('job_snapshots').insert(records).execute()
            print(f"✓ Inserted {len(records)} records into job_snapshots")
            return len(records)
        except Exception as e:
            print(f"❌ Error inserting snapshot: {e}")
            return 0
    
    def insert_kpis(self, kpis: Dict, report_date: datetime = None) -> bool:
        """
        Inserts KPI snapshot for historical tracking.
        
        Args:
            kpis: Dictionary of KPI values from calculate_kpis
            report_date: Date of report (defaults to today)
            
        Returns:
            True if successful
        """
        if report_date is None:
            report_date = datetime.now().date()
        
        record = {
            'report_date': report_date.isoformat(),
            'on_time_pct': kpis.get('on_time_pct', 0),
            'avg_delay_days': kpis.get('avg_delay_days', 0),
            'total_jobs': kpis.get('total_jobs', 0),
            'overdue_count': kpis.get('overdue_count', 0),
            'ready_for_routing': kpis.get('ready_for_routing', 0),
            'avg_scans_per_job': kpis.get('avg_scans_per_job', 0)
        }
        
        try:
            # Use upsert to handle re-runs on the same day
            # on_conflict='report_date' ensures we update the existing record for today
            self.client.table('kpi_history').upsert(record, on_conflict='report_date').execute()
            print(f"✓ Inserted/Updated KPIs for {report_date}")
            return True
        except Exception as e:
            print(f"❌ Error inserting KPIs: {e}")
            return False
    
    def get_latest_snapshot(self) -> Optional[pd.DataFrame]:
        """
        Retrieves the most recent complete snapshot for trend comparison.
        
        Returns:
            DataFrame of previous snapshot or None
        """
        try:
            # 1. Get the latest snapshot date
            date_query = self.client.table('job_snapshots') \
                .select('snapshot_date') \
                .order('snapshot_date', desc=True) \
                .limit(1) \
                .execute()
            
            if not date_query.data:
                print("⚠ No previous snapshot found")
                return None
                
            latest_date = date_query.data[0]['snapshot_date']
            
            # 2. Fetch all records for this specific snapshot
            # Supabase API limits standard requests to 1000 rows, so we might need pagination
            # But for simplicity in this specific project context (assuming < 1000 active jobs usually?)
            # let's set a high limit. If > 1000, we'd need loop.
            # However, standard limit is 1000. Let's try to fetch more if possible or just assuming < 5000.
            
            all_records = []
            offset = 0
            batch_size = 1000
            
            while True:
                result = self.client.table('job_snapshots') \
                    .select('*') \
                    .eq('snapshot_date', latest_date) \
                    .range(offset, offset + batch_size - 1) \
                    .execute()
                
                if not result.data:
                    break
                    
                all_records.extend(result.data)
                
                if len(result.data) < batch_size:
                    break
                    
                offset += batch_size
            
            if all_records:
                df = pd.DataFrame(all_records)
                print(f"✓ Retrieved {len(df)} records from snapshot {latest_date}")
                return df
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error retrieving snapshot: {e}")
            return None

    def get_snapshot_by_date(self, target_date: datetime.date) -> Optional[pd.DataFrame]:
        """
        Retrieves the snapshot for a specific date.
        
        Args:
            target_date: The date to fetch data for.
            
        Returns:
            DataFrame of the snapshot or None
        """
        try:
            # Construct start and end timestamps for the target date
            start_ts = datetime.combine(target_date, datetime.min.time()).isoformat()
            end_ts = datetime.combine(target_date, datetime.max.time()).isoformat()
            
            # Fetch all records for this date range
            # Utilizing pagination similar to get_latest_snapshot
            
            all_records = []
            offset = 0
            batch_size = 1000
            
            while True:
                result = self.client.table('job_snapshots') \
                    .select('*') \
                    .gte('snapshot_date', start_ts) \
                    .lte('snapshot_date', end_ts) \
                    .range(offset, offset + batch_size - 1) \
                    .execute()
                
                if not result.data:
                    break
                    
                all_records.extend(result.data)
                
                if len(result.data) < batch_size:
                    break
                    
                offset += batch_size
            
            if all_records:
                df = pd.DataFrame(all_records)
                
                # Normalize column names to match local processing expected format if needed
                # The DB columns are snake_case, but the app largely expects Title Case 
                # based on process_data output.
                # Let's map them back to what the app UI expects.
                
                # Map DB columns (snake_case) to App columns (Title Case / CamelCase)
                column_map = {
                    'job_id': 'Job_ID',
                    'planned_date': 'Planned_Date',
                    'actual_date': 'Actual_Date',
                    'delay_days': 'Delay_Days',
                    'status': 'Status',
                    'carrier': 'Carrier',
                    'state': 'State',
                    'scan_user': 'Last_Scan_User',
                    'scan_timestamp': 'Scan_Timestamp',
                    'product_description': 'Product_Name',
                    'piece_count': 'Piece_Count',
                    'white_glove': 'White_Glove',
                    'notification_detail': 'Notification_Detail',
                    'miles_oneway': 'Miles_OneWay'
                }
                
                df = df.rename(columns=column_map)
                
                # Ensure date columns are datetime objects
                if 'Planned_Date' in df.columns:
                    df['Planned_Date'] = pd.to_datetime(df['Planned_Date'])
                if 'Actual_Date' in df.columns:
                    df['Actual_Date'] = pd.to_datetime(df['Actual_Date'])
                if 'Scan_Timestamp' in df.columns:
                    df['Scan_Timestamp'] = pd.to_datetime(df['Scan_Timestamp'])
                
                # Ensure generic columns exist if missing (to avoid UI errors)
                required_cols = ['Stop_Number', 'Product_Serial', 'Assigned_Driver', 'Customer_Notes', 'Is_Routed']
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = '' # Fill missing with empty string
                        
                # Handle Is_Routed specifically (boolean)
                if 'Is_Routed' not in df.columns:
                     df['Is_Routed'] = False

                print(f"✓ Retrieved {len(df)} records for date {target_date}")
                return df
            else:
                print(f"⚠ No snapshot found for {target_date}")
                return None
                
        except Exception as e:
            print(f"❌ Error retrieving snapshot for date {target_date}: {e}")
            return None
    
    def get_historical_kpis(self, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Retrieves KPI history for trend charts.
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame with KPI time series
        """
        try:
            cutoff_date = (datetime.now().date() - pd.Timedelta(days=days)).isoformat()
            
            result = self.client.table('kpi_history') \
                .select('*') \
                .gte('report_date', cutoff_date) \
                .order('report_date') \
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                df['report_date'] = pd.to_datetime(df['report_date'])
                print(f"✓ Retrieved {len(df)} days of KPI history")
                return df
            else:
                print("⚠ No KPI history found")
                return None
        except Exception as e:
            print(f"❌ Error retrieving KPI history: {e}")
            return None
    
    def compare_with_history(self, current_kpis: Dict) -> Dict:
        """
        Compares current KPIs with previous period to show trends.
        
        Args:
            current_kpis: Current KPI values
            
        Returns:
            Dictionary with trend indicators (up/down/same)
        """
        history = self.get_historical_kpis(days=7)
        
        if history is None or len(history) < 2:
            return {key: '→' for key in current_kpis.keys()}
        
        # Get previous value (most recent before today)
        previous = history.iloc[-2]
        
        trends = {}
        for key in ['on_time_pct', 'avg_delay_days', 'overdue_count']:
            if key in current_kpis:
                current = current_kpis[key]
                prev = previous.get(key, current)
                
                # For avg_delay_days and overdue_count, lower is better
                if key in ['avg_delay_days', 'overdue_count']:
                    if current < prev:
                        trends[key] = '↓ (Improved)'
                    elif current > prev:
                        trends[key] = '↑ (Worsened)'
                    else:
                        trends[key] = '→ (Stable)'
                else:
                    # For on_time_pct, higher is better
                    if current > prev:
                        trends[key] = '↑ (Improved)'
                    elif current < prev:
                        trends[key] = '↓ (Worsened)'
                    else:
                        trends[key] = '→ (Stable)'
        
        return trends


def create_schema_sql() -> str:
    """
    Returns SQL to create Supabase schema.
    Run this manually in Supabase SQL editor.
    """
    return """
-- Job Snapshots Table
CREATE TABLE IF NOT EXISTS job_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_date TIMESTAMP NOT NULL,
    job_id TEXT NOT NULL,
    planned_date DATE,
    actual_date TIMESTAMP,
    delay_days NUMERIC,
    status TEXT,
    carrier TEXT,
    market TEXT,
    scan_user TEXT,
    scan_timestamp TIMESTAMP,
    product_description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_job_snapshots_date ON job_snapshots(snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_job_snapshots_job_id ON job_snapshots(job_id);

-- KPI History Table
CREATE TABLE IF NOT EXISTS kpi_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_date DATE NOT NULL UNIQUE,
    on_time_pct NUMERIC,
    avg_delay_days NUMERIC,
    total_jobs INTEGER,
    overdue_count INTEGER,
    ready_for_routing INTEGER,
    avg_scans_per_job NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for time series queries
CREATE INDEX IF NOT EXISTS idx_kpi_history_date ON kpi_history(report_date DESC);
"""


if __name__ == "__main__":
    print("Supabase Schema SQL:")
    print(create_schema_sql())
    print("\nCopy the above SQL and run in Supabase SQL Editor to set up the database.")
