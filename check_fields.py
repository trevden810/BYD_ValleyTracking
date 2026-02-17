import pandas as pd

df = pd.read_excel('bydhistorical.xlsx')

new_fields = ['_kf_state_id', 'piece_total', 'white_glove', 'notification_detail', '_kf_miles_oneway_id']

print('Field Check:')
for field in new_fields:
    status = "✓ Present" if field in df.columns else "✗ MISSING"
    print(f'  {field}: {status}')

print(f'\nTotal columns: {len(df.columns)}')
print(f'Total rows: {len(df)}')

if '_kf_state_id' in df.columns:
    print(f'\nState values:')
    for state, count in df['_kf_state_id'].value_counts().head(5).items():
        print(f'  {state}: {count}')
