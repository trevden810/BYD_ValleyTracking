import os
import shutil
import glob
import time

SOURCE_DIR = r"C:\Users\TrevorBates\OneDrive - PEP\Clients\Desktop\Azure Sync\Daily Standup\BYD_ValleyData"
DEST_FILE = r"c:\Projects\BYD_ValleyTracking\bydhistorical.xlsx"

def update_data():
    print(f"Scanning {SOURCE_DIR}...")
    
    if not os.path.exists(SOURCE_DIR):
        print(f"❌ Source directory not found: {SOURCE_DIR}")
        return

    # Get all xlsx files, excluding temp files
    files = [f for f in glob.glob(os.path.join(SOURCE_DIR, "*.xlsx")) if not os.path.basename(f).startswith("~$")]
    
    if not files:
        print("❌ No Excel files found in source directory.")
        return

    # Find latest file
    latest_file = max(files, key=os.path.getmtime)
    print(f"Found {len(files)} files.")
    print(f"Latest file: {os.path.basename(latest_file)}")
    print(f"Modified: {time.ctime(os.path.getmtime(latest_file))}")
    
    try:
        print(f"Copying to {DEST_FILE}...")
        shutil.copy2(latest_file, DEST_FILE)
        print("✅ Data updated successfully!")
        print("Refresh the app to see the changes.")
    except Exception as e:
        print(f"❌ Failed to copy file: {e}")
        return

    # Trigger V2 Pipeline
    print("\n" + "="*60)
    print("Starting V2 Pipeline (Supabase Sync & Comparisons)...")
    print("="*60 + "\n")
    
    try:
        from v2.main import main as process_v2_pipeline
        # Pass the original source file so we have the correct timestamp/metadata
        process_v2_pipeline(export_filepath=latest_file, dry_run=False)
    except ImportError as e:
        print(f"❌ Failed to import V2 pipeline: {e}")
        print("Ensure you are running this from the project root.")
    except Exception as e:
        print(f"❌ Error running V2 pipeline: {e}")

if __name__ == "__main__":
    update_data()
