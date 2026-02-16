import pandas as pd
from utils.api import get_token, fetch_jobs
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

def debug_parsing():
    with open('debug_completed_date_results.txt', 'w', encoding='utf-8') as f:
        f.write("Fetching jobs...\n")
        token = get_token()
        jobs = fetch_jobs(token)
        
        if jobs:
            df = pd.DataFrame(jobs)
            
            # Filter for non-empty time_complete
            df['time_complete_str'] = df['time_complete'].astype(str).replace('nan', '').replace('None', '')
            completed_df = df[df['time_complete_str'] != '']
            
            if not completed_df.empty:
                idx = completed_df.index[0]
                f.write(f"\n--- Inspecting COMPLETED Job Index {idx} ---\n")
                
                job_date_val = df.at[idx, 'job_date']
                time_comp_val = df.at[idx, 'time_complete']
                
                f.write(f"Raw job_date: {repr(job_date_val)} (type={type(job_date_val)})\n")
                f.write(f"Raw time_complete: {repr(time_comp_val)} (type={type(time_comp_val)})\n")
                
                job_date_str = str(job_date_val).strip()
                time_comp_str = str(time_comp_val).strip()
                combined = job_date_str + ' ' + time_comp_str
                f.write(f"Combined: '{combined}'\n")
                
                # Test Parsing
                try:
                    dt = pd.to_datetime(combined, errors='coerce', dayfirst=False)
                    f.write(f"Parsed Result (errors='coerce'): {dt}\n")
                except Exception as e:
                    f.write(f"Parse Error: {e}\n")
                    
                # Test with format
                try:
                    dt_fmt = pd.to_datetime(combined, format='%m/%d/%Y %H:%M:%S', errors='coerce')
                    f.write(f"Parsed Result (format): {dt_fmt}\n")
                except Exception as e:
                    f.write(f"Parse Format Error: {e}\n")

            else:
                f.write("No completed jobs found with time_complete!\n")

if __name__ == "__main__":
    debug_parsing()
