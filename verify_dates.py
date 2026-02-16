import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
from datetime import datetime

# Load environment variables
load_dotenv()

def verify_dates():
    print("Verifying recent job data fetching...")
    
    # Get token
    token = get_token()
    if not token:
        print("Authentication failed!")
        return
    
    print(f"✓ Authentication successful")
    
    # Fetch jobs with default query (last 90 days)
    print(f"\nFetching jobs (last 90 days)...")
    jobs = fetch_jobs(token)
    print(f"✓ Retrieved {len(jobs)} jobs")
    
    if jobs:
        # Process data
        df = process_data(jobs)
        
        # Check date range
        min_date = df['Planned_Date'].min()
        max_date = df['Planned_Date'].max()
        
        print(f"\n📅 Date Range:")
        print(f"   Earliest: {min_date}")
        print(f"   Latest: {max_date}")
        
        # Show current date for reference
        print(f"   Current: {datetime.now()}")
        
        # Show sample of recent jobs
        print(f"\n📋 Sample of Recent Jobs (Top 5):")
        print(df[['BOL_Number', 'Planned_Date', 'Status', 'Carrier']].head())
        
        # Check if we have 2026 data
        df_2026 = df[df['Planned_Date'].dt.year == 2026]
        df_2022 = df[df['Planned_Date'].dt.year == 2022]
        
        print(f"\n📊 Jobs by Year:")
        print(f"   2026 jobs: {len(df_2026)}")
        print(f"   2022 jobs: {len(df_2022)}")
        
        if len(df_2026) > 0:
            print(f"\n✅ SUCCESS: Fetching recent 2026 jobs!")
        elif len(df_2022) > len(df_2026):
            print(f"\n⚠️  WARNING: Still mostly 2022 jobs!")
    else:
        print("No jobs found!")

if __name__ == "__main__":
    verify_dates()
