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
                'miles_oneway': float(row.get('Miles_OneWay', 0.0)),
                # New fields
                'market': str(row.get('Market', '')),
                'city': str(row.get('City', '')),
                'customer_name': str(row.get('Customer_Name', '')),
                'delivery_address': str(row.get('Delivery_Address', '')),
                'date_received': row.get('Date_Received').isoformat() if pd.notna(row.get('Date_Received')) else None,
                'job_created_at': row.get('Job_Created_At').isoformat() if pd.notna(row.get('Job_Created_At')) else None,
                'client_order_number': str(row.get('Client_Order_Number', '')),
                'prior_job_id': str(row.get('Prior_Job_ID', '')),
                'signed_by': str(row.get('Signed_By', '')),
                'delivery_scan_count': int(row.get('Delivery_Scan_Count', 0)),
                'product_weight_lbs': int(row.get('Product_Weight_Lbs', 0)),
                'crew_required': int(row.get('Crew_Required', 1)),
                'driver_notes': str(row.get('Driver_Notes', '')),
                'job_type': str(row.get('Job_Type', 'Delivery')),
                'arrival_time': row.get('Arrival_Time').isoformat() if pd.notna(row.get('Arrival_Time')) else None,
                'dwell_minutes': float(row.get('Dwell_Minutes')) if pd.notna(row.get('Dwell_Minutes')) else None,
                'lead_time_days': int(row.get('Lead_Time_Days')) if pd.notna(row.get('Lead_Time_Days')) else None,
            }
            records.append(record)
        
        try:
            result = self.client.table('job_snapshots').insert(records).execute()
            print(f"[OK] Inserted {len(records)} records into job_snapshots")
            return len(records)
        except Exception as e:
            print(f"[ERROR] Error inserting snapshot: {e}")
            return 0
    
    def upsert_active_jobs(self, df: pd.DataFrame) -> int:
        """
        Syncs the job_snapshots table with the current export.

        Logic:
          1. Fetch all existing job_ids from the DB
          2. Delete rows whose job_id is in the current export (will be re-inserted fresh)
          3. Delete rows whose job_id is NOT in the export (completed/delivered)
          4. Insert all current export rows

        This preserves historical jobs from prior imports while keeping
        the data accurate on every run.

        Args:
            df: Processed DataFrame with ACTIVE jobs only (no completed)

        Returns:
            Number of records inserted
        """
        # Collect current job_ids from the export
        export_job_ids = set(df['Job_ID'].astype(str).tolist()) if 'Job_ID' in df.columns else set()

        # Step 1: Fetch all existing job_ids from the DB
        existing_ids = set()
        try:
            offset = 0
            while True:
                res = self.client.table('job_snapshots') \
                    .select('job_id') \
                    .range(offset, offset + 999) \
                    .execute()
                if not res.data:
                    break
                existing_ids.update(r['job_id'] for r in res.data)
                if len(res.data) < 1000:
                    break
                offset += 1000
            print(f"[OK] Found {len(existing_ids)} existing jobs in DB")
        except Exception as e:
            print(f"[WARN] Could not fetch existing job_ids: {e}")

        # Step 2: Delete rows that will be refreshed or are stale
        # Delete jobs in the current export (will re-insert with fresh data)
        # AND jobs NOT in the export (completed/delivered — stale)
        ids_to_delete = list(existing_ids)
        stale_count = len(existing_ids - export_job_ids)
        refresh_count = len(existing_ids & export_job_ids)
        deleted = 0
        try:
            for i in range(0, len(ids_to_delete), 50):
                batch = ids_to_delete[i:i + 50]
                res = self.client.table('job_snapshots') \
                    .delete() \
                    .in_('job_id', batch) \
                    .execute()
                if res.data:
                    deleted += len(res.data)
            print(f"[OK] Removed {deleted} rows ({stale_count} completed, {refresh_count} to refresh)")
        except Exception as e:
            print(f"[WARN] Error during cleanup: {e}")

        # Step 3: Insert all current export rows
        return self.insert_snapshot(df)


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
            print(f"[OK] Inserted/Updated KPIs for {report_date}")
            return True
        except Exception as e:
            print(f"[ERROR] Error inserting KPIs: {e}")
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
                print("[WARN] No previous snapshot found")
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
                print(f"[OK] Retrieved {len(df)} records from snapshot {latest_date}")
                return df
            else:
                return None
                
        except Exception as e:
            print(f"[ERROR] Error retrieving snapshot: {e}")
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
                    'miles_oneway': 'Miles_OneWay',
                    # New fields
                    'market': 'Market',
                    'city': 'City',
                    'customer_name': 'Customer_Name',
                    'delivery_address': 'Delivery_Address',
                    'date_received': 'Date_Received',
                    'job_created_at': 'Job_Created_At',
                    'client_order_number': 'Client_Order_Number',
                    'prior_job_id': 'Prior_Job_ID',
                    'signed_by': 'Signed_By',
                    'delivery_scan_count': 'Delivery_Scan_Count',
                    'product_weight_lbs': 'Product_Weight_Lbs',
                    'crew_required': 'Crew_Required',
                    'driver_notes': 'Driver_Notes',
                    'job_type': 'Job_Type',
                    'arrival_time': 'Arrival_Time',
                    'dwell_minutes': 'Dwell_Minutes',
                    'lead_time_days': 'Lead_Time_Days',
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

                print(f"[OK] Retrieved {len(df)} records for date {target_date}")
                return df
            else:
                print(f"[WARN] No snapshot found for {target_date}")
                return None
                
        except Exception as e:
            print(f"[ERROR] Error retrieving snapshot for date {target_date}: {e}")
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
                print(f"[OK] Retrieved {len(df)} days of KPI history")
                return df
            else:
                print("[WARN] No KPI history found")
                return None
        except Exception as e:
            print(f"[ERROR] Error retrieving KPI history: {e}")
            return None
    
    # ================================================================
    # NEW: Job History (Improvement #1)
    # ================================================================

    def insert_job_history(self, df: pd.DataFrame) -> int:
        """
        Archives completed/delivered jobs into job_history table.
        Uses upsert so re-runs on the same day are safe.
        
        Args:
            df: DataFrame of completed jobs (already filtered by status)
            
        Returns:
            Number of records archived
        """
        if df.empty:
            return 0
        
        records = []
        for _, row in df.iterrows():
            record = {
                'job_id': str(row.get('Job_ID', '')),
                'carrier': str(row.get('Carrier', '')),
                'state': str(row.get('State', '')),
                'planned_date': row.get('Planned_Date').isoformat() if pd.notna(row.get('Planned_Date')) else None,
                'actual_date': row.get('Actual_Date').isoformat() if pd.notna(row.get('Actual_Date')) else None,
                'delay_days': float(row.get('Delay_Days')) if pd.notna(row.get('Delay_Days')) else None,
                'status': str(row.get('Status', '')),
                'product_description': str(row.get('Product_Description', '')),
                'product_serial': str(row.get('Product_Serial', '')),
                'piece_count': int(row.get('Piece_Count', 0)),
                'white_glove': bool(row.get('White_Glove', False)),
                'scan_user': str(row.get('Scan_User', '')),
                'scan_timestamp': row.get('Scan_Timestamp').isoformat() if pd.notna(row.get('Scan_Timestamp')) else None,
                'notification_detail': str(row.get('Notification_Detail', '')),
                'miles_oneway': float(row.get('Miles_OneWay', 0.0)),
                # New fields
                'market': str(row.get('Market', '')),
                'city': str(row.get('City', '')),
                'customer_name': str(row.get('Customer_Name', '')),
                'delivery_address': str(row.get('Delivery_Address', '')),
                'date_received': row.get('Date_Received').isoformat() if pd.notna(row.get('Date_Received')) else None,
                'job_created_at': row.get('Job_Created_At').isoformat() if pd.notna(row.get('Job_Created_At')) else None,
                'client_order_number': str(row.get('Client_Order_Number', '')),
                'prior_job_id': str(row.get('Prior_Job_ID', '')),
                'signed_by': str(row.get('Signed_By', '')),
                'delivery_scan_count': int(row.get('Delivery_Scan_Count', 0)),
                'product_weight_lbs': int(row.get('Product_Weight_Lbs', 0)),
                'crew_required': int(row.get('Crew_Required', 1)),
                'driver_notes': str(row.get('Driver_Notes', '')),
                'job_type': str(row.get('Job_Type', 'Delivery')),
                'arrival_time': row.get('Arrival_Time').isoformat() if pd.notna(row.get('Arrival_Time')) else None,
                'dwell_minutes': float(row.get('Dwell_Minutes')) if pd.notna(row.get('Dwell_Minutes')) else None,
                'lead_time_days': int(row.get('Lead_Time_Days')) if pd.notna(row.get('Lead_Time_Days')) else None,
            }
            records.append(record)
        
        try:
            # Upsert on job_id — safe to re-run
            self.client.table('job_history').upsert(records, on_conflict='job_id').execute()
            print(f"[OK] Archived {len(records)} completed jobs to job_history")
            return len(records)
        except Exception as e:
            print(f"[ERROR] Error archiving job history: {e}")
            return 0

    def get_job_history(self, days: int = 90) -> Optional[pd.DataFrame]:
        """
        Retrieves historical completed-job records for analytics.
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame of completed jobs or None
        """
        try:
            cutoff_date = (datetime.now().date() - pd.Timedelta(days=days)).isoformat()
            
            all_records = []
            offset = 0
            batch_size = 1000
            
            while True:
                result = self.client.table('job_history') \
                    .select('*') \
                    .gte('completed_at', cutoff_date) \
                    .order('completed_at', desc=True) \
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
                print(f"[OK] Retrieved {len(df)} historical job records")
                return df
            else:
                print("[INFO] No job history records found")
                return None
        except Exception as e:
            print(f"[ERROR] Error retrieving job history: {e}")
            return None

    # ================================================================
    # NEW: Carrier-Level KPIs (Improvement #2)
    # ================================================================

    def insert_carrier_kpis(self, carrier_kpis: List[Dict], report_date=None) -> bool:
        """
        Stores per-carrier KPI records.
        
        Args:
            carrier_kpis: List of dicts from calculate_carrier_kpis()
            report_date: Date of report (defaults to today)
            
        Returns:
            True if successful
        """
        if not carrier_kpis:
            return True
        
        if report_date is None:
            report_date = datetime.now().date()
        
        records = []
        for kpi in carrier_kpis:
            records.append({
                'report_date': report_date.isoformat(),
                'carrier': kpi['carrier'],
                'on_time_pct': kpi.get('on_time_pct', 0),
                'avg_delay_days': kpi.get('avg_delay_days', 0),
                'total_jobs': kpi.get('total_jobs', 0),
                'overdue_count': kpi.get('overdue_count', 0),
                'ready_for_routing': kpi.get('ready_for_routing', 0)
            })
        
        try:
            self.client.table('kpi_carrier_history').upsert(
                records, on_conflict='report_date,carrier'
            ).execute()
            print(f"[OK] Stored KPIs for {len(records)} carriers")
            return True
        except Exception as e:
            print(f"[ERROR] Error inserting carrier KPIs: {e}")
            return False

    # ================================================================
    # NEW: Stage Transitions (Improvement #4)
    # ================================================================

    def insert_transitions(self, transitions: List[Dict]) -> int:
        """
        Stores job stage transition records.
        Uses upsert so duplicate transitions are ignored.
        
        Args:
            transitions: List of transition dicts from detect_transitions()
            
        Returns:
            Number of transitions stored
        """
        if not transitions:
            return 0
        
        try:
            self.client.table('job_stage_transitions').upsert(
                transitions, on_conflict='job_id,to_status'
            ).execute()
            print(f"[OK] Stored {len(transitions)} stage transitions")
            return len(transitions)
        except Exception as e:
            print(f"[ERROR] Error inserting transitions: {e}")
            return 0

    def get_dwell_times(self) -> Optional[pd.DataFrame]:
        """
        Queries the v_stage_dwell_times view for average time in each stage.
        
        Returns:
            DataFrame with dwell time analytics or None
        """
        try:
            result = self.client.table('v_stage_dwell_times') \
                .select('*') \
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                print(f"[OK] Retrieved dwell times for {len(df)} stages")
                return df
            else:
                print("[INFO] No dwell time data available yet")
                return None
        except Exception as e:
            print(f"[ERROR] Error retrieving dwell times: {e}")
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
            return {key: '->' for key in current_kpis.keys()}
        
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
                        trends[key] = 'v (Improved)'
                    elif current > prev:
                        trends[key] = '^ (Worsened)'
                    else:
                        trends[key] = '-> (Stable)'
                else:
                    # For on_time_pct, higher is better
                    if current > prev:
                        trends[key] = '^ (Improved)'
                    elif current < prev:
                        trends[key] = 'v (Worsened)'
                    else:
                        trends[key] = '-> (Stable)'
        
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
