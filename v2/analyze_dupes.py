import pandas as pd
import os

def analyze_duplicates(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    try:
        df = pd.read_excel(filepath)
        print(f"Loaded {len(df)} records from {filepath}")
        
        # Check potential unique identifiers
        check_cols = ['product_serial_number', 'order_C1', 'description_product', 'customer_name_first', 'customer_name_last']
        existing_cols = [c for c in check_cols if c in df.columns]
        
        print(f"Columns found: {existing_cols}")
        
        for col in existing_cols:
            if col == 'description_product': continue # Too generic likely
            
            # Count duplicates
            dupes = df[df.duplicated(subset=[col], keep=False)]
            if not dupes.empty:
                print(f"\n--- Duplicates found in {col} ---")
                print(f"Count: {len(dupes)}")
                
                # Show sample of duplicates with Job ID and Status
                show_cols = ['_kp_job_id', col, 'job_status', 'job_date']
                show_cols = [c for c in show_cols if c in df.columns]
                
                # Group by the col and show first 3 groups
                unique_vals = dupes[col].unique()[:3]
                for val in unique_vals:
                    print(f"\nValue: {val}")
                    print(dupes[dupes[col] == val][show_cols])
            else:
                print(f"\nNo duplicates found in {col}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_duplicates(r"c:\Projects\BYD_ValleyTracking\scanfieldtest.xlsx")
