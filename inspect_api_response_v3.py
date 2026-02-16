import os
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs
import json
import io

# Load environment variables
load_dotenv()

def inspect_data():
    print("Fetching jobs...")
    
    # Get token
    token = get_token()
    if not token:
        print("❌ Authentication failed!")
        return
    
    # Fetch jobs
    jobs = fetch_jobs(token)
    
    if jobs:
        print(f"✓ Retrieved {len(jobs)} jobs")
        
        # Get the first confirmed/completed job if possible
        completed_jobs = [j for j in jobs if 'complete' in str(j.get('job_status', '')).lower()]
        
        output_str = ""
        output_str += f"Total Jobs: {len(jobs)}\n"
        
        if completed_jobs:
            job = completed_jobs[0]
            output_str += "\n📋 Inspecting a COMPLETED job:\n"
        else:
            job = jobs[0]
            output_str += "\n📋 Inspecting the first job (status unknown):\n"

        # Print all keys sorted nicely
        output_str += "\n🔑 All Keys:\n"
        
        # Sort keys but prioritize relevant ones
        keys = sorted(job.keys())
        relevant_keys = [k for k in keys if any(x in k.lower() for x in ['date', 'time', 'status', 'complete', 'arrive', 'start', 'end'])]
        other_keys = [k for k in keys if k not in relevant_keys]
        
        output_str += "--- RELEVANT FIELDS ---\n"
        for key in relevant_keys:
             output_str += f"  {key}: {job[key]}\n"
             
        output_str += "\n--- OTHER FIELDS ---\n"
        for key in other_keys:
             # Truncate long values
             val = str(job[key])
             if len(val) > 100:
                 val = val[:100] + "..."
             output_str += f"  {key}: {val}\n"
             
        # Write to file with explicit utf-8 encoding
        with open('inspection_v3_results.txt', 'w', encoding='utf-8') as f:
            f.write(output_str)
            
        print("✓ Inspection results written to inspection_v3_results.txt")
             
    else:
        print("❌ No jobs found!")

if __name__ == "__main__":
    inspect_data()
