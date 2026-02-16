import pandas as pd
from utils.api import get_token, fetch_jobs
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()
sys.stdout.reconfigure(encoding='utf-8')

def debug_parsing():
    print("Fetching jobs...")
    token = get_token()
    jobs = fetch_jobs(token)
    
    if jobs:
        # Find the job with BOL 3 (from previous output)
        job = next((j for j in jobs if j.get('order_C1') == 3 or str(j.get('order_C1')) == '3'), None)
        
        if not job:
            job = jobs[0]
            print("Job 3 not found, using first job")
        
        print("\n--- Inspecting Job Data ---")
        job_date = str(job.get('job_date', ''))
        time_comp = str(job.get('time_complete', ''))
        
        print(f"Original job_date: '{job_date}' (len={len(job_date)})")
        print(f"Original time_complete: '{time_comp}' (len={len(time_comp)})")
        
        # Test cleaning
        job_date_clean = job_date.strip()
        time_comp_clean = time_comp.strip()
        
        print(f"Clean job_date: '{job_date_clean}' (len={len(job_date_clean)})")
        print(f"Clean time_complete: '{time_comp_clean}' (len={len(time_comp_clean)})")
        
        combined = f"{job_date_clean} {time_comp_clean}"
        print(f"Combined string: '{combined}'")
        
        print("\n--- Testing Parsing ---")
        # Test 1: Default
        try:
            dt1 = pd.to_datetime(combined)
            print(f"1. Default pd.to_datetime: {dt1}")
        except Exception as e:
            print(f"1. Default failed: {e}")
            
        # Test 2: Coerce
        try:
            dt2 = pd.to_datetime(combined, errors='coerce')
            print(f"2. Coerce: {dt2}")
        except Exception as e:
            print(f"2. Coerce failed: {e}")
            
        # Test 3: Explicit Format
        try:
            dt3 = pd.to_datetime(combined, format='%m/%d/%Y %H:%M:%S')
            print(f"3. Format %m/%d/%Y %H:%M:%S: {dt3}")
        except Exception as e:
            print(f"3. Format failed: {e}")
            
        # Test 4: Day First False
        try:
            dt4 = pd.to_datetime(combined, dayfirst=False)
            print(f"4. Dayfirst=False: {dt4}")
        except Exception as e:
            print(f"4. Dayfirst=False failed: {e}")

if __name__ == "__main__":
    debug_parsing()
