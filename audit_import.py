
import pandas as pd
import os
import sys
from v2.daily_import import find_latest_export
from v2.data_processor import load_manual_export, process_data, deduplicate_jobs

def audit_import():
    print("="*60)
    print("AUDIT: BYD/Valley Data Import")
    print("="*60)
    
    # 1. Find Data
    export_dir = r"C:\Users\TrevorBates\OneDrive - PEP\Clients\Desktop\Azure Sync\Daily Standup\BYD_ValleyData"
    try:
        latest_file = find_latest_export(export_dir)
        print(f"\n[1] Source File: {os.path.basename(latest_file)}")
    except Exception as e:
        print(f"[ERROR] Could not find export file: {e}")
        return

    # 2. Check Raw Data
    try:
        df_raw = pd.read_excel(latest_file)
        print(f"[2] Raw Rows: {len(df_raw)}")
    except Exception as e:
        print(f"[ERROR] Could not read excel: {e}")
        return

    # 3. Check Processed Data (Before Dedup)
    try:
        df_processed_raw = process_data(df_raw)
        print(f"[3] Processed Rows (Pre-Dedup): {len(df_processed_raw)}")
    except Exception as e:
        print(f"[ERROR] Processing failed: {e}")
        return

    # 4. Check Deduplicated Data
    try:
        df_deduped = deduplicate_jobs(df_processed_raw)
        print(f"[4] Deduplicated Rows: {len(df_deduped)}")
        print(f"    -> Dropped {len(df_processed_raw) - len(df_deduped)} rows")
    except Exception as e:
        print(f"[ERROR] Deduplication failed: {e}")
        return

    # 5. Analyze Dropped Rows
    if len(df_processed_raw) > len(df_deduped):
        print("\n[ANALYSIS] Dropped Rows Sample:")
        
        # Identify dropped indices
        dropped_indices = df_processed_raw.index.difference(df_deduped.index)
        dropped_rows = df_processed_raw.loc[dropped_indices]
        
        # Group by Serial to show why they were dropped
        # We need to see the "kept" counterpart for context
        
        for idx, row in dropped_rows.head(5).iterrows():
            serial = row.get('Product_Serial')
            print(f"  - Dropped Job ID: {row.get('Job_ID')} (Serial: {serial}, Planned: {row.get('Planned_Date')})")
            
            # Find the kept one
            kept = df_deduped[df_deduped['Product_Serial'] == serial]
            if not kept.empty:
                print(f"    -> Kept Job ID: {kept.iloc[0]['Job_ID']} (Planned: {kept.iloc[0]['Planned_Date']})")
    
    # 6. Check for "Lost" Data (Non-serial drops?)
    # Logic: process_data should return same len as raw unless it filters?
    # process_data currently does NO filtering, just enrichment.
    
    if len(df_raw) != len(df_processed_raw):
        print(f"\n[WARN] process_data changed row count! ({len(df_raw)} -> {len(df_processed_raw)})")

if __name__ == "__main__":
    audit_import()
