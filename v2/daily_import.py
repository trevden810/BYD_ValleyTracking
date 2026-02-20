"""
BYD/Valley Daily Data Import Script

Automatically finds the latest manual export from OneDrive directory
and processes it through the V2.0 pipeline.

Usage:
    python daily_import.py              # Process latest export
    python daily_import.py --dry-run    # Test without sending
    python daily_import.py --launch-app # Also launch Streamlit dashboard
"""

import os
import sys
import glob
from datetime import datetime
from pathlib import Path
import argparse

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v2.main import main as process_export


def _collect_valid_exports(directory: str) -> list:
    """
    Returns a list of valid export files (MM_DD_YY.NN.xlsx) in the given directory.
    Returns an empty list if the directory doesn't exist or has no matching files.
    """
    if not os.path.exists(directory):
        return []

    valid = []
    for filepath in glob.glob(os.path.join(directory, "*.xlsx")):
        filename = os.path.basename(filepath)
        parts = filename.replace('.xlsx', '').split('.')
        if len(parts) == 2:
            date_part = parts[0].split('_')
            if len(date_part) == 3 and all(p.isdigit() for p in date_part):
                if parts[1].isdigit():
                    valid.append(filepath)
    return valid


def find_latest_export(export_dir: str) -> str:
    """
    Finds the latest export file (MM_DD_YY.NN.xlsx) in the OneDrive
    export directory.

    Args:
        export_dir: Path to the OneDrive export directory

    Returns:
        Absolute path to the latest export file

    Raises:
        FileNotFoundError: If no matching files are found
    """
    if not os.path.exists(export_dir):
        raise FileNotFoundError(f"Export directory not found: {export_dir}")

    valid_files = _collect_valid_exports(export_dir)

    if not valid_files:
        raise FileNotFoundError(
            f"No export files (MM_DD_YY.NN.xlsx) found in:\n  {export_dir}"
        )

    # Sort by modification time — most recent first
    valid_files.sort(key=os.path.getmtime, reverse=True)
    return valid_files[0]


def launch_streamlit_dashboard():
    """
    Launches the Streamlit dashboard in the default browser.
    """
    import subprocess
    
    app_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'BYD_Valley_Job_Tracker.py')
    
    print("\n" + "=" * 60)
    print("Launching Streamlit Dashboard...")
    print("=" * 60)
    print("The dashboard will open in your default browser.")
    print("Press CTRL+C in this window to stop the server.")
    print("=" * 60 + "\n")
    
    subprocess.run([sys.executable, "-m", "streamlit", "run", app_path])


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='BYD/Valley Daily Data Import - Process latest export from OneDrive'
    )
    parser.add_argument(
        '--export-dir',
        default=r'C:\Users\TrevorBates\OneDrive - PEP\Clients\Desktop\Azure Sync\Daily Standup\BYD_ValleyData',
        help='Path to OneDrive export directory'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Process data but don\'t send email or store in database'
    )
    parser.add_argument(
        '--launch-app',
        action='store_true',
        help='Launch Streamlit dashboard after processing'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("BYD/Valley Daily Data Import")
    print("=" * 60)
    print(f"\nSearching for latest export in:")
    print(f"  {args.export_dir}")
    
    # Find latest export
    try:
        latest_export = find_latest_export(args.export_dir)
        export_filename = os.path.basename(latest_export)
        export_dir_used = os.path.dirname(latest_export)
        export_time = datetime.fromtimestamp(os.path.getmtime(latest_export))

        # Quick row count so the user can confirm the right file was picked
        try:
            import pandas as pd
            row_count = len(pd.read_excel(latest_export))
        except Exception:
            row_count = '?'

        print(f"\n[OK] Found latest export: {export_filename}")
        print(f"  Location : {export_dir_used}")
        print(f"  Modified : {export_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Row count: {row_count} jobs in file")

    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("\nPlease ensure export files are present in the expected format (MM_DD_YY.NN.xlsx).")
        return 1
    
    # Process the export through V2.0 pipeline
    print("\n" + "=" * 60)
    print("Processing Export Through V2.0 Pipeline")
    print("=" * 60 + "\n")
    
    try:
        success = process_export(export_filepath=latest_export, dry_run=args.dry_run)
        
        if not success:
            print("\n[ERROR] Pipeline processing failed")
            return 1
            
    except Exception as e:
        print(f"\n[ERROR] Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Launch dashboard if requested
    if args.launch_app:
        try:
            launch_streamlit_dashboard()
        except KeyboardInterrupt:
            print("\n\nDashboard stopped by user.")
        except Exception as e:
            print(f"\n[WARNING] Error launching dashboard: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
