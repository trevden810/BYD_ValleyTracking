import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
import pandas as pd

# Load environment variables
load_dotenv()

def test_process():
    print("Testing Data Processing...")
    token = get_token()
    if not token:
        print("Auth failed")
        return

    # Fetch 10 jobs
    query = {"query": [{"_kp_job_id": "*"}], "limit": 10}
    jobs = fetch_jobs(token, query_payload=query)
    print(f"Fetched {len(jobs)} jobs.")

    if not jobs:
        print("No jobs to process.")
        return

    df = process_data(jobs)
    print("DataFrame Head:")
    print(df.head())
    print("\nColumns:")
    print(df.columns)
    print("\nData Types:")
    print(df.dtypes)
    
    # Check for empty Date/Status
    print("\nSample Rows with parsed Data:")
    print(df[['BOL_Number', 'Planned_Date', 'Actual_Date', 'Status', 'Carrier', 'Delay_Days']].head())

if __name__ == "__main__":
    test_process()
