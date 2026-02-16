import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
import pandas as pd
import sys

# Load environment variables
load_dotenv()

# Set encoding
sys.stdout.reconfigure(encoding='utf-8')

def verify_calculations():
    print("Fetching and processing data...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    # Fetch jobs
    jobs = fetch_jobs(token)
    
    if jobs:
        print(f"✓ Retrieved {len(jobs)} jobs")
        
        # Process data
        df = process_data(jobs)
        
        print("\n📋 Columns in Processed DataFrame:")
        print(df.columns.tolist())
        
        # Check if relevant columns exist
        cols = ['BOL_Number', 'Status', 'job_date', 'time_complete', 'Planned_Date', 'Actual_Date', 'Delay_Days']
        available_cols = [c for c in cols if c in df.columns]
        
        print(f"\n📋 Data Preview (Completed Jobs):")
        # Filter for completed jobs or jobs with time_complete
        if 'time_complete' in df.columns:
            completed = df[df['time_complete'] != '']
            if not completed.empty:
                print(completed[available_cols].head(5).to_string())
            else:
                print("No jobs with time_complete found!")
        else:
            print("time_complete column MISSING!")
            
        print("\n📋 Data Preview (All Jobs - Head):")
        print(df[available_cols].head(5).to_string())
        
        # Check specific calculation types
        if 'Delay_Days' in df.columns:
            print(f"\nDelay_Days Type: {df['Delay_Days'].dtype}")
            print(f"Delay_Days Stats: {df['Delay_Days'].describe()}")
            
    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    verify_calculations()
