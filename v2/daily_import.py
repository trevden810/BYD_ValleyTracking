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


def find_latest_export(export_dir: str) -> str:
    """
    Finds the latest export file from the OneDrive directory.
    
    File naming convention: MM_DD_YY.NN (e.g., 02_16_26.01, 02_16_26.02)
    where NN is a sequential number for multiple exports on the same day.
    
    Args:
        export_dir: Path to OneDrive export directory
        
    Returns:
        Path to the latest export file
        
    Raises:
        FileNotFoundError: If no export files are found
    """
    # Ensure directory exists
    if not os.path.exists(export_dir):
        raise FileNotFoundError(f"Export directory not found: {export_dir}")
    
    # Pattern to match export files: MM_DD_YY.NN.xlsx
    pattern = os.path.join(export_dir, "*.xlsx")
    export_files = glob.glob(pattern)
    
    if not export_files:
        raise FileNotFoundError(f"No export files found in {export_dir}")
    
    # Filter to only files matching the naming convention (MM_DD_YY.NN)
    valid_files = []
    for filepath in export_files:
        filename = os.path.basename(filepath)
        # Check if it matches pattern like "02_16_26.01.xlsx"
        parts = filename.replace('.xlsx', '').split('.')
        if len(parts) == 2:
            # Validate date part (should be MM_DD_YY)
            date_part = parts[0].split('_')
            if len(date_part) == 3 and all(p.isdigit() for p in date_part):
                # Validate sequence part (should be digits)
                if parts[1].isdigit():
                    valid_files.append(filepath)
    
    if not valid_files:
        raise FileNotFoundError(
            f"No files matching naming convention (MM_DD_YY.NN.xlsx) found in {export_dir}"
        )
    
    # Sort by file modification time (most recent first)
    valid_files.sort(key=os.path.getmtime, reverse=True)
    
    latest_file = valid_files[0]
    return latest_file


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
        export_time = datetime.fromtimestamp(os.path.getmtime(latest_export))
        
        print(f"\n[OK] Found latest export: {export_filename}")
        print(f"  Modified: {export_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("\nPlease ensure:")
        print("  1. The OneDrive directory path is correct")
        print("  2. Export files are present in the expected format (MM_DD_YY.NN.xlsx)")
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
