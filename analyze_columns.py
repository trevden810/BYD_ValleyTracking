import pandas as pd

try:
    df = pd.read_excel('scanfieldtest.xlsx')
    print("Columns found:")
    for col in df.columns:
        if 'state' in col.lower() or 'loc' in col.lower() or 'add' in col.lower() or 'dest' in col.lower():
            print(f"- {col}: {df[col].iloc[0] if not df.empty else 'Empty'}")
            
    print("\n--- All Columns ---")
    print(df.columns.tolist())
except Exception as e:
    print(f"Error: {e}")
