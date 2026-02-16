import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs

# Load environment variables
load_dotenv()

def test_api():
    print("Testing FileMaker API Connection...")
    
    # Check env vars
    required = ["FILEMAKER_BASE_URL", "FILEMAKER_USERNAME", "FILEMAKER_PASSWORD", "FILEMAKER_JOBS_DB"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        print(f"ERROR: Missing environment variables: {missing}")
        return

    # Test Auth
    print("Authenticating...")
    token = get_token()
    if token:
        print("Authentication SUCCESS. Token received.")
    else:
        print("Authentication FAILED.")
        return

    # Test Fetch with default query (BYDo and VALLEYc filter)
    print("Fetching BYD/Valley jobs using default query...")
    jobs = fetch_jobs(token)
    print(f"Fetch SUCCESS. Retrieved {len(jobs)} jobs.")
    
    if jobs:
        print("\nSample Job:")
        import json
        print(json.dumps(jobs[0], indent=2))
    else:
        print("No BYD/Valley jobs found.")
if __name__ == "__main__":
    test_api()
