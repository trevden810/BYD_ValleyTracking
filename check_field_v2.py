from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import sys
import json

# Load environment variables
load_dotenv()

def check_field():
    print("Writing keys to keys.txt...")
    with open('keys.txt', 'w', encoding='utf-8') as f:
        f.write("Fetching jobs...\n")
        try:
            token = get_token()
            if not token:
                f.write("Authentication failed\n")
                return

            jobs = fetch_jobs(token)
            if not jobs:
                f.write("No jobs found\n")
                return

            f.write(f"Retrieved {len(jobs)} jobs\n")
            # Try to find a job with relevant fields if possible
            # But just dumping the first one is a good start
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
            else:
                f.write(f"\n❌ {target} NOT FOUND.\n")
                
        except Exception as e:
            f.write(f"Error: {e}\n")
    print("Done.")

if __name__ == "__main__":
    check_field()
