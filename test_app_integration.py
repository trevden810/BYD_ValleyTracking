"""
Test script to validate app integration with new export fields.
Simulates the app's data processing pipeline.
"""
import pandas as pd
import sys
sys.path.append('.')

from utils.api import process_data

# Load test data
df_raw = pd.read_excel('byd_fieldtest.xlsx')
print(f"Loaded {len(df_raw)} records from byd_fieldtest.xlsx")

# Convert to list of dicts (simulating API response)
jobs_data = df_raw.to_dict('records')

# Process through app pipeline
print("\n--- Processing Data ---")
df_processed = process_data(jobs_data)

print(f"Processed {len(df_processed)} records")
print(f"Total columns: {len(df_processed.columns)}")

# Verify new fields exist
new_fields = ['Product_Name', 'Product_Serial', 'Last_Scan_User', 'Last_Scan_Time', 'Total_Scans']
print("\n--- Field Verification ---")
for field in new_fields:
    if field in df_processed.columns:
        non_empty = df_processed[field].astype(str).str.strip().ne('').sum()
        print(f"✅ {field}: {non_empty} non-empty records")
    else:
        print(f"❌ {field}: MISSING")

# Show sample data
print("\n--- Sample Records ---")
sample = df_processed[['Job_ID', 'Product_Name', 'Product_Serial', 'Last_Scan_User', 'Total_Scans']].head(5)
print(sample.to_string(index=False))

# Check for errors
if df_processed['Product_Name'].isnull().all():
    print("\n⚠️  WARNING: All Product_Name values are null")
if df_processed['Last_Scan_User'].astype(str).str.strip().eq('').all():
    print("\n⚠️  WARNING: All Last_Scan_User values are empty")
    
print("\n✅ Test complete!")
