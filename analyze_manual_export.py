import pandas as pd
import os

file_path = 'scanfieldtest.xlsx'

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    exit()

try:
    df = pd.read_excel(file_path)
    with open('manual_export_analysis_utf8.txt', 'w', encoding='utf-8') as f:
        f.write("Columns in Manual Export:\n")
        for col in df.columns:
            f.write(f" - {col}\n")
        
        f.write("\nFirst 3 rows of data:\n")
        f.write(df.head(3).to_string())
        f.write("\n\nData Types:\n")
        f.write(str(df.dtypes))
        
    print("Analysis complete. Written to manual_export_analysis_utf8.txt")

except Exception as e:
    print(f"Error reading excel file: {e}")
