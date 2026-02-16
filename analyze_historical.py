import pandas as pd
import os

file_path = 'bydhistorical.xlsx'

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    exit()

try:
    df = pd.read_excel(file_path, nrows=5) # Read first 5 rows to get columns and samples
    with open('bydhistorical_analysis.txt', 'w', encoding='utf-8') as f:
        f.write("Columns in bydhistorical.xlsx:\n")
        
        target = "box_serial_numbers_scanned_received_json"
        found = False
        
        for col in df.columns:
            f.write(f" - {col}\n")
            if col == target:
                found = True
        
        f.write(f"\nTarget field '{target}' found: {found}\n")
        
        if found:
            f.write(f"\nSample data for '{target}':\n")
            f.write(df[target].astype(str).to_string())

    print("Analysis complete. Written to bydhistorical_analysis.txt")

except Exception as e:
    print(f"Error reading excel file: {e}")
