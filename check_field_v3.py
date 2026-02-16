from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import sys
import json
import os

# Load environment variables
load_dotenv()

def check_field():
    print("Writing keys to keys_v3.txt...")
    with open('keys_v3.txt', 'w', encoding='utf-8') as f:
        f.write("Fetching jobs (limit 1)...\n")
        try:
            token = get_token()
            if not token:
                f.write("Authentication failed\n")
                return

            # Custom query to limit to 1 record for speed
            # We need to replicate the default query structure but add limit
            # Default query structure from api.py:
            # query = {
            #     "query": [
            #         { "_kf_client_code_id": "BYDo", ... },
            #         { "_kf_client_code_id": "VALLEYc", ... }
            #     ],
            #     "limit": 5000, ...
            # }
            
            # We'll just ask for *any* job, limit 1 provided layout has data
            # To be safe, use the same criteria but limit 1
            query = {
                "query": [
                    {"_kf_client_code_id": "BYDo"},
                    {"_kf_client_code_id": "VALLEYc"}
                ],
                "limit": 1
            }

            jobs = fetch_jobs(token, query_payload=query)
            if not jobs:
                f.write("No jobs found with custom query.\n")
                # Try default fetch just in case
                f.write("Retrying with default fetch (might be slow)...\n")
                jobs = fetch_jobs(token)
                if not jobs:
                     f.write("Still no jobs found.\n")
                     return

            f.write(f"Retrieved {len(jobs)} jobs\n")
            if jobs:
                job = jobs[0]
                
                # Print all keys to file
                f.write("\nAll Keys:\n")
                keys = sorted(job.keys())
                for k in keys:
                    f.write(f"{k}: {job[k]}\n")
                
                # Specific check
                target = "box_serial_numbers_scanned_received_json"
                if target in job:
                    f.write(f"\n✅ {target} FOUND!\n")
                    f.write(f"Value: {job[target]}\n")
                else:
                    f.write(f"\n❌ {target} NOT FOUND.\n")
                    f.write(f"(Checked layout: {os.getenv('FILEMAKER_JOBS_LAYOUT', 'Jobs')})\n")
                
        except Exception as e:
            f.write(f"Error: {e}\n")
    print("Done.")

if __name__ == "__main__":
    check_field()
