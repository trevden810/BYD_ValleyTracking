import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import pandas as pd
import json

# Load environment variables
load_dotenv()

def inspect_data():
    print("Fetching jobs to inspect available fields...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    # Fetch jobs
    jobs = fetch_jobs(token)
    
    if jobs:
        print(f"✓ Retrieved {len(jobs)} jobs")
        
        # Get the first job
        first_job = jobs[0]
        
        with open('api_response_analysis_utf8.txt', 'w', encoding='utf-8') as f:
            f.write("Available Fields (Keys) in first job record:\n")
            for key, value in first_job.items():
                f.write(f"  - {key}: {value}\n")
                
            # Check for potential date/time fields
            f.write("\nPotential Date/Time Fields:\n")
            keywords = ['date', 'time', 'created', 'updated', 'stamp', 'start', 'end', 'complete', 'arrival']
            for key, value in first_job.items():
                if any(k in key.lower() for k in keywords):
                    f.write(f"  - {key}: {value}\n")
                    
            # Check for status fields
            f.write("\nStatus Fields:\n")
            for key, value in first_job.items():
                if 'status' in key.lower():
                    f.write(f"  - {key}: {value}\n")
                    
        print("Analysis complete. Written to api_response_analysis_utf8.txt")

    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    inspect_data()
