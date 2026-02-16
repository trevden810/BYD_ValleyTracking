import pandas as pd
import os

# Using the path provided by user
source_dir = r"C:\Users\TrevorBates\OneDrive - PEP\Clients\Desktop\Azure Sync\Daily Standup\BYD_ValleyData"
file_name = "02_16_26.02.xlsx"
file_path = os.path.join(source_dir, file_name)

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    exit()

try:
    print(f"Reading {file_path}...")
    df = pd.read_excel(file_path, nrows=5)
    
    target = "box_serial_numbers_scanned_received_json"
    if target in df.columns:
        print(f"✅ Field '{target}' FOUND.")
        # Check if it has data
        sample = df[target].dropna().head(1)
        if not sample.empty:
             print(f"Sample value: {sample.iloc[0][:50]}...")
        else:
             print("⚠️ Field found but checks empty in first 5 rows.")
    else:
        print(f"❌ Field '{target}' NOT found.")
        print("Columns found:", df.columns.tolist())

except Exception as e:
    print(f"Error: {e}")
