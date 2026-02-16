"""
Test script for V2 Comparator
"""
import pandas as pd
from datetime import datetime
from v2.comparator import compare_snapshots

def test_comparator():
    print("Testing Daily Delta Comparator...")
    
    # 1. Setup "Yesterday" (Previous Snapshot)
    previous_data = [
        # Job 1: Planned, not arrived
        {'job_id': 'JOB-001', 'carrier': 'Carrier A', 'status': 'Manifested', 'actual_date': None, 'planned_date': '2026-02-15'},
        # Job 2: Arrived, On Time
        {'job_id': 'JOB-002', 'carrier': 'Carrier B', 'status': 'En Route', 'actual_date': '2026-02-14T10:00:00', 'planned_date': '2026-02-14'},
        # Job 3: In Transit
        {'job_id': 'JOB-003', 'carrier': 'Carrier C', 'status': 'In Transit', 'actual_date': '2026-02-12T09:00:00', 'planned_date': '2026-02-12'}
    ]
    prev_df = pd.DataFrame(previous_data)
    
    # 2. Setup "Today" (Current Export)
    current_data = [
        # Job 1: Now Arrived! (New Arrival)
        {'Job_ID': 'JOB-001', 'Carrier': 'Carrier A', 'Status': 'Arrived', 'Actual_Date': pd.Timestamp('2026-02-16 08:30:00'), 'Planned_Date': pd.Timestamp('2026-02-15'), 'Delay_Days': 1},
        
        # Job 2: No Change
        {'Job_ID': 'JOB-002', 'Carrier': 'Carrier B', 'Status': 'En Route', 'Actual_Date': pd.Timestamp('2026-02-14 10:00:00'), 'Planned_Date': pd.Timestamp('2026-02-14'), 'Delay_Days': 0},
        
        # Job 3: Now Delivered! (New Delivery)
        {'Job_ID': 'JOB-003', 'Carrier': 'Carrier C', 'Status': 'Delivered', 'Actual_Date': pd.Timestamp('2026-02-12 09:00:00'), 'Planned_Date': pd.Timestamp('2026-02-12'), 'Delay_Days': 0},
        
        # Job 4: Brand New! (New Job)
        {'Job_ID': 'JOB-004', 'Carrier': 'Carrier D', 'Status': 'Manifested', 'Actual_Date': pd.NaT, 'Planned_Date': pd.Timestamp('2026-02-20'), 'Delay_Days': None},
        
        # Job 5: Became Overdue (Planned yesterday, no arrival)
        # Assuming run date is 2026-02-17
        {'Job_ID': 'JOB-005', 'Carrier': 'Carrier E', 'Status': 'Manifested', 'Actual_Date': pd.NaT, 'Planned_Date': pd.Timestamp('2026-02-16'), 'Delay_Days': None}
    ]
    curr_df = pd.DataFrame(current_data)
    
    # 3. Run Comparison
    deltas = compare_snapshots(curr_df, prev_df)
    
    # 4. Verify Results
    print("\nResults:")
    print(f"New Jobs: {len(deltas['new_jobs'])} (Expected 2: JOB-004, JOB-005)")
    for job in deltas['new_jobs']:
        print(f" - {job['Job_ID']}")
        
    print(f"New Arrivals: {len(deltas['new_arrivals'])} (Expected 1: JOB-001)")
    for job in deltas['new_arrivals']:
        print(f" - {job['Job_ID']}")
        
    print(f"New Deliveries: {len(deltas['new_deliveries'])} (Expected 1: JOB-003)")
    for job in deltas['new_deliveries']:
        print(f" - {job['Job_ID']}")

    # Check for specific IDs
    job_ids_new = [j['Job_ID'] for j in deltas['new_jobs']]
    assert 'JOB-004' in job_ids_new
    
    job_ids_arr = [j['Job_ID'] for j in deltas['new_arrivals']]
    assert 'JOB-001' in job_ids_arr
    
    job_ids_del = [j['Job_ID'] for j in deltas['new_deliveries']]
    assert 'JOB-003' in job_ids_del
    
    print("\n✓ SUCCESS: Logic verification passed!")

if __name__ == "__main__":
    test_comparator()
