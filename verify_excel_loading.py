import pandas as pd
from utils.api import fetch_jobs_from_excel, process_data
import os

file_path = "bydhistorical.xlsx"

print(f"Testing load from: {file_path}")

try:
    # 1. Fetch
    raw_data = fetch_jobs_from_excel(file_path)
    print(f"Fetched {len(raw_data)} records.")
    
    if not raw_data:
        print("No data fetched.")
        exit()

    # 2. Process
    print("Processing data...")
    df = process_data(raw_data)
    print("Data processed.")
    print("Columns:", df.columns.tolist())
    
    # 3. Check specific field
    target_col = "Last_Scan_User"
    if target_col in df.columns:
        print(f"✅ {target_col} column exists.")
        
        # Check for non-empty values
        populated = df[df[target_col] != ''].shape[0]
        print(f"Records with {target_col}: {populated} / {len(df)}")
        
        if populated > 0:
            print("\nSample populated records:")
            print(df[df[target_col] != ''][['Job_ID', 'Last_Scan_User', 'Last_Scan_Time']].head().to_string())
        else:
            print(f"⚠️ {target_col} is empty in all records.")
            # Debug: Check the source JSON field in raw data vs processed
            print("\nDebugging source field 'box_serial_numbers_scanned_received_json':")
            if 'box_serial_numbers_scanned_received_json' in df.columns: # It might not be in final df if not preserved, check raw
               pass
            
            # Check raw sample for the json field
            sample_json = [r.get('box_serial_numbers_scanned_received_json', '') for r in raw_data[:5]]
            print("Raw JSON field samples:", sample_json)
            
    else:
        print(f"❌ {target_col} column NOT found in processed DataFrame.")

except Exception as e:
    print(f"Error during verification: {e}")
    import traceback
    traceback.print_exc()
