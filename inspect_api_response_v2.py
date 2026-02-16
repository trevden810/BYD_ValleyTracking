import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import pandas as pd
import json
import sys

# Load environment variables
load_dotenv()

# Set encoding to utf-8 for print
sys.stdout.reconfigure(encoding='utf-8')

def inspect_data():
    print("Fetching jobs...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    # Fetch jobs
    jobs = fetch_jobs(token)
    
    if jobs:
        print(f"✓ Retrieved {len(jobs)} jobs")
        
        # Get the first confirmed/completed job if possible
        completed_jobs = [j for j in jobs if 'complete' in str(j.get('job_status', '')).lower()]
        if completed_jobs:
            job = completed_jobs[0]
            print("\n📋 Inspecting a COMPLETED job:")
        else:
            job = jobs[0]
            print("\n📋 Inspecting the first job (status unknown):")

        # Print all keys sorted
        print("\n🔑 All Keys:")
        for key in sorted(job.keys()):
             print(f"  {key}: {job[key]}")
             
    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    inspect_data()
