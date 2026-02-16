import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
import pandas as pd
import sys

# Load environment variables
load_dotenv()

def verify_calculations():
    # Write to file directly
    with open('verification_results.txt', 'w', encoding='utf-8') as f:
        f.write("Fetching and processing data...\n")
        
        # Get token
        token = get_token()
        if not token:
            f.write("❌ Authentication failed!\n")
            return
        
        # Fetch jobs
        jobs = fetch_jobs(token)
        
        if jobs:
            f.write(f"✓ Retrieved {len(jobs)} jobs\n")
            
            # Process data
            df = process_data(jobs)
            
            f.write("\n📋 Columns in Processed DataFrame:\n")
            f.write(str(df.columns.tolist()) + "\n")
            
            # Check if relevant columns exist
            cols = ['BOL_Number', 'Status', 'job_date', 'time_complete', 'Planned_Date', 'Actual_Date', 'Delay_Days']
            available_cols = [c for c in cols if c in df.columns]
            
            f.write(f"\n📋 Data Preview (Completed Jobs):\n")
            # Filter for completed jobs or jobs with time_complete
            if 'time_complete' in df.columns:
                completed = df[df['time_complete'] != '']
                if not completed.empty:
                    f.write(completed[available_cols].head(5).to_string() + "\n")
                else:
                    f.write("No jobs with time_complete found!\n")
            else:
                f.write("time_complete column MISSING!\n")
                
            f.write("\n📋 Data Preview (All Jobs - Head):\n")
            f.write(df[available_cols].head(5).to_string() + "\n")
            
            # Check specific calculation types
            if 'Delay_Days' in df.columns:
                f.write(f"\nDelay_Days Type: {df['Delay_Days'].dtype}\n")
                f.write(f"Delay_Days Stats:\n{df['Delay_Days'].describe()}\n")
                
        else:
            f.write("❌ No jobs found!\n")
            
    print("Verification data written to verification_results.txt")

if __name__ == "__main__":
    verify_calculations()
