import pandas as pd
import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data

load_dotenv()

def inspect_bol():
    token = get_token()
    if not token:
        print("Failed to get token")
        return

    print("Fetching jobs...")
    raw_data = fetch_jobs(token, days_back=5) # fetching recent jobs
    if not raw_data:
        print("No jobs found")
        return

    df = process_data(raw_data)
    
    if df.empty:
        print("DataFrame is empty")
        return

    print("\n--- Data Inspection (First 10 records) ---")
    cols_to_show = ['BOL_Number']
    
    # Check which source columns exist
    if 'order_C1' in raw_data[0]: cols_to_show.append('order_C1')
    else: print("order_C1 not in raw data")
        
    if 'order_C2' in raw_data[0]: cols_to_show.append('order_C2')
    else: print("order_C2 not in raw data")
        
    if '_kp_job_id' in raw_data[0]: cols_to_show.append('_kp_job_id')
    else: print("_kp_job_id not in raw data")

    # We need to look at raw data vs processed df to see where it came from
    # But process_data overwrites. Let's look at a sample of the processed DF 
    # and try to check the raw dict for the same index if possible,
    # or just print the relevant columns from DF if they were preserved (they aren't mostly).
    
    # Actually, process_data creates 'order_C1_str' and 'order_C2_str'. Let's check those.
    debug_cols = ['BOL_Number']
    if 'order_C1_str' in df.columns: debug_cols.append('order_C1_str')
    if 'order_C2_str' in df.columns: debug_cols.append('order_C2_str')
    if '_kp_job_id' in df.columns: debug_cols.append('_kp_job_id')

    print(df[debug_cols].head(10).to_string())

if __name__ == "__main__":
    inspect_bol()
