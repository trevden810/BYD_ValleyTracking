"""
V2 Data Comparator Module

Compares two DataFrames (current vs previous) to identify daily changes:
- New job creations
- New arrivals (Actual_Date set)
- New deliveries (Status changed to Delivered)
- Newly overdue jobs
"""

import pandas as pd
from typing import Dict, List, Any

def compare_snapshots(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Compares current export with previous snapshot to find deltas.
    
    Args:
        current_df: processed DataFrame from today
        previous_df: DataFrame from previous snapshot (Supabase)
        
    Returns:
        Dictionary containing lists of changed records:
        {
            'new_jobs': [...],
            'new_arrivals': [...],
            'new_deliveries': [...],
            'new_overdue': [...]
        }
    """
    deltas = {
        'new_jobs': [],
        'new_arrivals': [],
        'new_deliveries': [],
        'new_overdue': []
    }
    
    if previous_df is None or previous_df.empty:
        # If no history, everything is "new" but we might not want to overwhelm matches
        # For now, just return empty or maybe all as new? 
        # Strategy: Return empty delta if no history to avoid noise on first run
        return deltas

    # Ensure Job_ID is string for consistent merging
    current_df['Job_ID'] = current_df['Job_ID'].astype(str)
    previous_df['job_id'] = previous_df['job_id'].astype(str)
    
    # helper to fast-lookup previous row by job_id
    # Using 'job_id' (lowercase) because Supabase returns lowercase column names
    prev_map = previous_df.set_index('job_id').to_dict('index')
    
    for _, row in current_df.iterrows():
        job_id = row['Job_ID']
        
        # 1. New Jobs
        if job_id not in prev_map:
            deltas['new_jobs'].append({
                'Job_ID': job_id,
                'Carrier': row.get('Carrier', ''),
                'Market': row.get('Market', ''),
                'Status': row.get('Status', '')
            })
            continue # specific logic for new jobs ends here
            
        prev_row = prev_map[job_id]
        
        # 2. New Arrivals (Actual_Date was null/None, now is set)
        curr_actual = row.get('Actual_Date')
        prev_actual = prev_row.get('actual_date')
        
        is_arrived_now = pd.notna(curr_actual)
        is_arrived_before = pd.notna(prev_actual)
        
        if is_arrived_now and not is_arrived_before:
            deltas['new_arrivals'].append({
                'Job_ID': job_id,
                'Carrier': row.get('Carrier', ''),
                'Actual_Date': curr_actual.strftime('%Y-%m-%d %H:%M') if hasattr(curr_actual, 'strftime') else str(curr_actual),
                'Delay_Days': row.get('Delay_Days', 0)
            })
            
        # 3. New Deliveries (Status changed to Delivered/Complete)
        curr_status = str(row.get('Status', '')).lower()
        prev_status = str(prev_row.get('status', '')).lower()
        
        completed_keywords = ['delivered', 'complete', 'completed']
        is_complete_now = curr_status in completed_keywords
        is_complete_before = prev_status in completed_keywords
        
        if is_complete_now and not is_complete_before:
            deltas['new_deliveries'].append({
                'Job_ID': job_id,
                'Carrier': row.get('Carrier', ''),
                'Status': row.get('Status', '')
            })

        # 4. Newly Overdue
        # Logic: Planned date < Today AND Actual Date is NaT
        # Change: previously wasn't overdue (maybe planned date was future, or it had arrived?)
        # Simplification: Just list what is overdue NOW that wasn't flagged before? 
        # Actually simplest useful metric: It became overdue TODAY.
        # i.e. Planned Date was Yesterday? Or just "Is Overdue Now" AND "Was Not Overdue Before"?
        
        # Let's rely on the pre-calculated logic if possible, or re-eval:
        if pd.isna(curr_actual): # Still not arrived
            curr_planned = row.get('Planned_Date')
            
            if pd.notna(curr_planned):
                # check if it just became overdue (e.g. planned date was yesterday)
                # OR check if it wasn't in the overdue list before. 
                # Simpler: If it's overdue now, and wasn't marked as overdue (or didn't exist) before?
                # But we know it existed (we are in the not-new-job block).
                
                # Check if it was overdue before:
                # In previous snapshot, checks if actual_date was null and planned_date < snapshot_date
                
                # Note: This determines "Freshly" overdue vs "Still" overdue.
                # "Freshly" overdue means Planned_Date < Today && Planned_Date >= Yesterday? 
                # Let's stick to: Planned Date is BEFORE Today, and in previous check it wasn't overdue.
                
                # For simplicity in V1 of this feature: 
                # If Planned_Date is strictly yesterday, it became overdue today.
                from datetime import datetime, timedelta
                yesterday = (datetime.now() - timedelta(days=1)).date()
                if curr_planned.date() == yesterday:
                     deltas['new_overdue'].append({
                        'Job_ID': job_id,
                        'Carrier': row.get('Carrier', ''),
                        'Planned_Date': curr_planned.strftime('%Y-%m-%d')
                    })

    return deltas
