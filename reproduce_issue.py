
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import traceback

def test_supabase():
    print("Testing Supabase Client initialization...")
    load_dotenv()
    
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        print("Skipping test: SUPABASE_URL or SUPABASE_KEY not found in env")
        return

    try:
        print(f"URL: {url[:10]}...")
        client = create_client(url, key)
        print("Client created successfully.")
        
        # Try a query
        print("Attempting to query 'job_snapshots'...")
        response = client.table('job_snapshots').select('*').limit(1).execute()
        print(f"Query successful. Got {len(response.data)} rows.")

    except Exception:
        traceback.print_exc()

    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    test_supabase()
