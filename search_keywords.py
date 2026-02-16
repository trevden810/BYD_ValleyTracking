import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import json

load_dotenv()

def search_keywords():
    print("Searching 500 jobs for 'BYDo' or 'VALLEYc'...")
    token = get_token()
    if not token:
        return

    query = {"query": [{"_kp_job_id": "*"}], "limit": 500}
    jobs = fetch_jobs(token, query_payload=query)
    print(f"Scanned {len(jobs)} jobs.")
    
    found_count = 0
    targets = ["BYDo", "VALLEYc"]
    
    for job in jobs:
        match = False
        for k, v in job.items():
            if isinstance(v, str) and any(t in v for t in targets):
                print(f"\nMATCH FOUND in Job {job.get('_kp_job_id')}:")
                print(f"Key: {k}, Value: {v}")
                match = True
        if match:
            found_count += 1
            if found_count >= 10:
                break
                
    if found_count == 0:
        print("No matches found in 500 jobs.")

if __name__ == "__main__":
    search_keywords()
