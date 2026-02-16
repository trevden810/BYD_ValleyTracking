import os
import glob

# Directory containing the pages
pages_dir = r"c:\Projects\BYD_ValleyTracking\pages"

# Files to delete (using part of the name to match if needed, or exact utf-8 names)
# Since we have exact names from list_dir:
files_to_delete = [
    "1_🚨_Action_Required.py",
    "2_📊_Performance.py",
    "3_📋_Job_List.py"
]

print(f"Checking directory: {pages_dir}")
for filename in files_to_delete:
    file_path = os.path.join(pages_dir, filename)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"Deleted: {filename}")
        except Exception as e:
            print(f"Error deleting {filename}: {e}")
    else:
        print(f"File not found: {filename}")

# List remaining files to verify
print("\nRemaining files:")
for f in os.listdir(pages_dir):
    print(f" - {f}")
