import sys
sys.stdout = open('verification_results.txt', 'w', encoding='utf-8')

import pandas as pd
import json

# Configuration
file_path = 'byd_fieldtest.xlsx'
required_fields = [
    '_kp_job_id',
    '_kf_client_code_id',
    'job_date',
    'job_status',
    'time_complete',
    'description_product',
    'product_serial_number',
    'box_serial_numbers_scanned_received_json'
]

print(f"Loading {file_path}...")
try:
    df = pd.read_excel(file_path)
    print("✅ File loaded successfully.")
    print(f"Total Records: {len(df)}")
    print(f"Total Columns: {len(df.columns)}")
except Exception as e:
    print(f"❌ Error loading file: {e}")
    exit()

print("\n--- Field Verification ---")
all_found = True
for field in required_fields:
    if field in df.columns:
        # Check for non-null data
        # Clean data first: drop NaNs, convert to string, remove empty strings/whitespace
        sample = df[field].dropna().astype(str)
        sample = sample[sample.str.strip() != '']
        sample = sample[sample.str.lower() != 'nan']
        sample = sample[sample.str.lower() != 'none']
        
        count = len(sample)
        status_icon = "✅" if count > 0 else "⚠️"
        
        print(f"{status_icon} {field}: Found ({count} non-empty records)")
        
        if count > 0:
            print(f"   Sample: {sample.iloc[0][:100]}...")
            
            # Special check for JSON
            if 'json' in field:
                try:
                    json_data = json.loads(sample.iloc[0])
                    print(f"   ✅ Valid JSON structure detected.")
                except:
                    print(f"   ❌ Invalid JSON format in sample.")
    else:
        print(f"❌ Missing: {field}")
        all_found = False

if all_found:
    print("\n✅ All required fields are present.")
else:
    print("\n❌ Some required fields are missing.")
