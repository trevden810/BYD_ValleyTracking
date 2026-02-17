import pandas as pd

df = pd.read_excel('bydhistorical.xlsx')

new_fields = ['_kf_state_id', 'piece_total', 'white_glove', 'notification_detail', '_kf_miles_oneway_id']

print("=== NEW FIELD INSPECTION ===\n")

for field in new_fields:
    if field in df.columns:
        print(f"✓ {field}")
        print(f"  Type: {df[field].dtype}")
        print(f"  Sample values: {df[field].dropna().head(3).tolist()}")
        print(f"  Unique count: {df[field].nunique()}")
        print(f"  Null count: {df[field].isnull().sum()} / {len(df)}")
        print()
    else:
        print(f"✗ {field} - MISSING")
        print()
