import pandas as pd
from utils.api import get_token, fetch_jobs, process_data
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

def debug_parsing():
    with open('debug_date_results.txt', 'w', encoding='utf-8') as f:
        f.write(f"Pandas Version: {pd.__version__}\n\n")
        
        f.write("Fetching jobs...\n")
        token = get_token()
        jobs = fetch_jobs(token)
        
        if jobs:
            # Process data partially to match app env
            df = pd.DataFrame(jobs)
            
            f.write("DataFrame Created.\n")
            if 'job_date' in df.columns:
                f.write(f"job_date dtype: {df['job_date'].dtype}\n")
                f.write(f"job_date sample: {df['job_date'].head(1).values}\n")
            
            if 'time_complete' in df.columns:
                f.write(f"time_complete dtype: {df['time_complete'].dtype}\n")
                f.write(f"time_complete sample: {df['time_complete'].head(1).values}\n")

            # Find specific job
            job_idx = df[df['order_C1'].astype(str) == '3'].index
            if len(job_idx) > 0:
                idx = job_idx[0]
                f.write(f"\n--- Inspecting Job Index {idx} ---\n")
                
                job_date_val = df.at[idx, 'job_date']
                time_comp_val = df.at[idx, 'time_complete']
                
                f.write(f"Raw job_date: {repr(job_date_val)} (type={type(job_date_val)})\n")
                f.write(f"Raw time_complete: {repr(time_comp_val)} (type={type(time_comp_val)})\n")
                
                # Simulate the logic
                job_date_str = str(job_date_val).strip()
                time_comp_str = str(time_comp_val).strip()
                
                f.write(f"Str job_date: '{job_date_str}'\n")
                f.write(f"Str time_complete: '{time_comp_str}'\n")
                
                combined = job_date_str + ' ' + time_comp_str
                f.write(f"Combined: '{combined}'\n")
                
                # Test Parsing
                try:
                    dt = pd.to_datetime(combined, errors='coerce', dayfirst=False)
                    f.write(f"Parsed Result (errors='coerce'): {dt}\n")
                except Exception as e:
                    f.write(f"Parse Error: {e}\n")
                    
                try:
                    dt2 = pd.to_datetime(combined, dayfirst=False)
                    f.write(f"Parsed Result (no coerce): {dt2}\n")
                except Exception as e:
                    f.write(f"Parse Error (no coerce): {e}\n")
                    
if __name__ == "__main__":
    debug_parsing()
