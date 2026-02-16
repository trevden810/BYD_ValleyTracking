from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import sys

# Load environment variables
load_dotenv()

# Set encoding to utf-8
sys.stdout.reconfigure(encoding='utf-8')

def check_field():
    print("Fetching jobs...")
    token = get_token()
    if not token:
        print("Authentication failed")
        return

    jobs = fetch_jobs(token)
    if not jobs:
        print("No jobs found")
        return

    print(f"Retrieved {len(jobs)} jobs")
    job = jobs[0]
    
    target_field = "box_serial_numbers_scanned_received_json"
    
    # Check if field exists
    if target_field in job:
        print(f"✅ FIELD FOUND: {target_field}")
        print(f"Value: {job[target_field]}")
    else:
        print(f"❌ FIELD NOT FOUND: {target_field}")
        
    # Also print all keys that contain 'box' or 'serial' or 'json' to help discover the correct name
    print("\nSimilar fields found:")
    for key in job.keys():
        if any(x in key.lower() for x in ['box', 'serial', 'json', 'scan']):
            print(f"  - {key}: {job[key]}")

if __name__ == "__main__":
    check_field()
