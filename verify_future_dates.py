import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

def verify_future_dates():
    print("Verifying future date search capability...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    print(f"✓ Authentication successful")
    
    # Fetch jobs with default query (90 days back to 90 days forward)
    print(f"\nFetching jobs (90 days back + 90 days forward)...")
    jobs = fetch_jobs(token)
    print(f"✓ Retrieved {len(jobs)} jobs")
    
    if jobs:
        # Process data
        df = process_data(jobs)
        
        # Check date range
        min_date = df['Planned_Date'].min()
        max_date = df['Planned_Date'].max()
        today = datetime.now()
        
        print(f"\n📅 Date Range:")
        print(f"   Earliest: {min_date}")
        print(f"   Latest: {max_date}")
        print(f"   Today: {today}")
        
        # Check for future jobs
        df_future = df[df['Planned_Date'] > today]
        df_past = df[df['Planned_Date'] <= today]
        
        print(f"\n📊 Jobs by Timeline:")
        print(f"   Past/Today jobs: {len(df_past)}")
        print(f"   Future jobs: {len(df_future)}")
        
        if len(df_future) > 0:
            print(f"\n✅ SUCCESS: Can fetch FUTURE jobs!")
            print(f"\n📋 Sample Future Jobs (Top 5):")
            future_sample = df_future[['BOL_Number', 'Planned_Date', 'Status', 'Carrier']].head()
            print(future_sample.to_string())
        else:
            print(f"\n⚠️  No future jobs found in database")
            
        # Check if max date is beyond today
        if max_date.date() > today.date():
            print(f"\n✅ Date range extends {(max_date.date() - today.date()).days} days into the future!")
        else:
            print(f"\n⚠️  No jobs scheduled beyond today in the database")
    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    verify_future_dates()
