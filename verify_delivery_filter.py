import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data

# Load environment variables
load_dotenv()

def verify_delivery_filter():
    print("Verifying Delivery job type filter...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    print(f"✓ Authentication successful")
    
    # Fetch jobs (should only be Delivery type now)
    print(f"\nFetching jobs with Delivery filter...")
    jobs = fetch_jobs(token)
    print(f"✓ Retrieved {len(jobs)} jobs")
    
    if jobs:
        # Process data
        df = process_data(jobs)
        
        # Check job types
        if 'job_type' in df.columns:
            job_types = df['job_type'].value_counts()
            print(f"\n📊 Job Types Found:")
            print(job_types.to_string())
            
            delivery_count = df[df['job_type'] == 'Delivery'].shape[0]
            non_delivery_count = df[df['job_type'] != 'Delivery'].shape[0]
            
            print(f"\n📦 Delivery Jobs: {delivery_count}")
            print(f"⚠️  Non-Delivery Jobs: {non_delivery_count}")
            
            if non_delivery_count == 0:
                print(f"\n✅ SUCCESS: Only Delivery jobs are being fetched!")
            else:
                print(f"\n⚠️  WARNING: Still fetching non-Delivery jobs")
                print(f"\nNon-Delivery types found:")
                non_delivery = df[df['job_type'] != 'Delivery']['job_type'].value_counts()
                print(non_delivery.to_string())
        else:
            # Check Status column which might have job_type info
            if 'Status' in df.columns:
                print(f"\n⚠️  job_type column not found, checking Status:")
                print(df['Status'].value_counts().to_string())
            else:
                print(f"\n⚠️  Cannot verify job types - column not found")
        
        # Show sample
        print(f"\n📋 Sample Jobs (Top 5):")
        cols_to_show = ['BOL_Number', 'Planned_Date', 'Status', 'Carrier']
        if 'job_type' in df.columns:
            cols_to_show.append('job_type')
        available_cols = [c for c in cols_to_show if c in df.columns]
        print(df[available_cols].head().to_string())
    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    verify_delivery_filter()
