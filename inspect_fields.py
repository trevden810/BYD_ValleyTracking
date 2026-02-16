import os
import json
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs

# Load environment variables explicitly
load_dotenv()

def inspect_all_fields():
    print("Authenticating...")
    token = get_token()
    if not token:
        print("Failed to get token")
        return

    print("Fetching one job...")
    # Fetch just 1 record to inspect structure
    raw_data = fetch_jobs(token, days_back=10, days_forward=1)
    
    if not raw_data:
        print("No jobs found.")
        return

    first_record = raw_data[0]
    with open('fields_dump.txt', 'w', encoding='utf-8') as f:
        f.write("--- Available Fields in First Record ---\n")
        for key, value in sorted(first_record.items()):
            f.write(f"{key}: {value}\n")
    print("Fields written to fields_dump.txt")

if __name__ == "__main__":
    inspect_all_fields()
