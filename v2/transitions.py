"""
Workflow Stage Transition Tracker

Detects status changes between snapshots to build a timeline
of how jobs move through workflow stages. Used for dwell-time analysis.

Stages: Manifested -> Arrived -> Routed -> Confirmed -> Delivered
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any


def detect_transitions(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Compares current and previous snapshots to detect status changes.
    
    Args:
        current_df: Processed DataFrame from today's export
        previous_df: DataFrame from previous snapshot (Supabase)
        
    Returns:
        List of transition records:
        [
            {
                'job_id': 'JOB-001',
                'from_status': 'Manifested',
                'to_status': 'Arrived',
                'transitioned_at': '2026-02-20T08:30:00'
            },
            ...
        ]
    """
    transitions = []
    
    if previous_df is None or previous_df.empty:
        # No previous snapshot — record initial status for all jobs
        for _, row in current_df.iterrows():
            job_id = str(row.get('Job_ID', ''))
            status = str(row.get('Status', ''))
            if job_id and status:
                transitions.append({
                    'job_id': job_id,
                    'from_status': None,
                    'to_status': status,
                    'transitioned_at': datetime.now().isoformat()
                })
        return transitions
    
    # Ensure consistent ID types
    current_df = current_df.copy()
    previous_df = previous_df.copy()
    current_df['Job_ID'] = current_df['Job_ID'].astype(str)
    previous_df['job_id'] = previous_df['job_id'].astype(str)
    
    # Build lookup of previous statuses
    prev_map = {}
    for _, row in previous_df.iterrows():
        jid = row.get('job_id', '')
        prev_map[jid] = str(row.get('status', '')).strip()
    
    for _, row in current_df.iterrows():
        job_id = str(row.get('Job_ID', ''))
        curr_status = str(row.get('Status', '')).strip()
        
        if not job_id or not curr_status:
            continue
        
        if job_id not in prev_map:
            # New job — record its initial status
            transitions.append({
                'job_id': job_id,
                'from_status': None,
                'to_status': curr_status,
                'transitioned_at': datetime.now().isoformat()
            })
        else:
            prev_status = prev_map[job_id]
            
            # Status changed — record the transition
            if curr_status.lower() != prev_status.lower():
                transitions.append({
                    'job_id': job_id,
                    'from_status': prev_status,
                    'to_status': curr_status,
                    'transitioned_at': datetime.now().isoformat()
                })
    
    return transitions


# For testing
if __name__ == "__main__":
    print("Workflow Stage Transition Tracker")
    print("=" * 40)
    print("\nFunctions available:")
    print("  - detect_transitions(current_df, previous_df)")
