import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
import pandas as pd
import sys

# Load environment variables
load_dotenv()

def verify_calculations():
    # Write to file directly
    with open('verification_v3_results.txt', 'w', encoding='utf-8') as f:
        f.write("Fetching and processing data...\n")
        
        # Get token
        token = get_token()
        jobs = fetch_jobs(token)
        
        if jobs:
            df = process_data(jobs)
            
            # Check if relevant columns exist
            cols = ['BOL_Number', 'Status', 'job_date', 'time_complete', 'Actual_Date_Str', 'Actual_Date', 'Delay_Days']
            available_cols = [c for c in cols if c in df.columns]
            
            f.write(f"\n📋 Data Preview (Completed Jobs with non-empty time):\n")
            if 'time_complete' in df.columns:
                completed = df[df['time_complete'].astype(str) != '']
                if not completed.empty:
                    f.write(completed[available_cols].head(10).to_string() + "\n")
                else:
                    f.write("No jobs with time_complete found!\n")
            
            # Inspect specific types of Actual_Date_Str
            if 'Actual_Date_Str' in df.columns:
                sample = df['Actual_Date_Str'].dropna().head(5)
                f.write(f"\nActual_Date_Str Sample:\n{sample}\n")
                f.write(f"Actual_Date_Str Dtype: {df['Actual_Date_Str'].dtype}\n")
                
            # Try to force conversion here to see if it works
            if 'Actual_Date_Str' in completed.columns:
                try:
                    test_series = pd.to_datetime(completed['Actual_Date_Str'], errors='coerce', dayfirst=False)
                    f.write(f"\nTest Conversion on Completed:\n{test_series.head(10)}\n")
                except Exception as e:
                    f.write(f"\nTest Conversion Failed: {e}\n")

if __name__ == "__main__":
    verify_calculations()
