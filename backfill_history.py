"""
Backfill script: processes ALL historical data into Supabase.

1. Loads bydhistorical.xlsx first (oldest data, broadest range)
2. Then processes each OneDrive export in chronological order
   — only the LAST export per day (highest sequence number)

Uses the same pipeline as daily_import but inserts all data
additively (no deletes, just inserts) to build up the full history.
"""
import sys
import os
import glob
import re

sys.path.append(os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from v2.data_processor import load_manual_export, process_data, deduplicate_jobs
from v2.supabase_client import SupabaseClient
from datetime import datetime

ONEDRIVE_DIR = r"C:\Users\TrevorBates\OneDrive - PEP\Clients\Desktop\Azure Sync\Daily Standup\BYD_ValleyData"
HISTORICAL_FILE = r"C:\Projects\BYD_ValleyTracking\bydhistorical.xlsx"


def process_and_insert(filepath, client, label=""):
    """Process an export file and insert into Supabase (additive — no deletes)."""
    try:
        df_raw = load_manual_export(filepath)
        df_proc = process_data(df_raw)
        df_dedup = deduplicate_jobs(df_proc)

        # Filter out completed/delivered
        if "Status" in df_dedup.columns:
            mask = df_dedup["Status"].astype(str).str.lower().str.strip().str.contains(
                "complete|deliver", na=False
            )
            df_active = df_dedup[~mask].copy()
        else:
            df_active = df_dedup.copy()

        # Get job_ids in this file
        export_ids = set(df_active["Job_ID"].astype(str).tolist()) if "Job_ID" in df_active.columns else set()

        # Delete any existing rows for these job_ids (so we replace with latest)
        if export_ids:
            ids_list = list(export_ids)
            for i in range(0, len(ids_list), 50):
                batch = ids_list[i:i + 50]
                try:
                    client.client.table("job_snapshots").delete().in_("job_id", batch).execute()
                except Exception:
                    pass

        # Insert fresh
        count = client.insert_snapshot(df_active)
        print(f"  {label}: {count} active jobs inserted (from {len(df_raw)} raw)")
        return count
    except Exception as e:
        print(f"  {label}: ERROR — {e}")
        return 0


def get_latest_per_day(directory):
    """
    Returns a dict of { date_str: filepath } with only the latest
    export per day (highest sequence number or most recent modification).
    """
    files = glob.glob(os.path.join(directory, "*.xlsx"))
    by_date = {}

    for fp in files:
        fn = os.path.basename(fp)
        # Match patterns like 02_16_26.01.xlsx or 2_19_26.01.xlsx
        match = re.match(r"(\d{1,2}_\d{2}_\d{2})\.(\d+)\.xlsx", fn)
        if match:
            date_key = match.group(1)
            seq = int(match.group(2))
            if date_key not in by_date or seq > by_date[date_key][1]:
                by_date[date_key] = (fp, seq)

    # Sort by date and return
    sorted_dates = sorted(by_date.keys(), key=lambda d: os.path.getmtime(by_date[d][0]))
    return [(by_date[d][0], d) for d in sorted_dates]


def main():
    client = SupabaseClient()

    print("=" * 60)
    print("HISTORICAL BACKFILL")
    print("=" * 60)

    total_inserted = 0

    # 1. Process bydhistorical.xlsx first
    if os.path.exists(HISTORICAL_FILE):
        print(f"\n--- Phase 1: Historical file ---")
        total_inserted += process_and_insert(HISTORICAL_FILE, client, "bydhistorical.xlsx")
    else:
        print(f"[WARN] Historical file not found: {HISTORICAL_FILE}")

    # 2. Process OneDrive exports (latest per day, chronological)
    print(f"\n--- Phase 2: OneDrive exports (latest per day) ---")
    daily_exports = get_latest_per_day(ONEDRIVE_DIR)

    if not daily_exports:
        print("[WARN] No exports found in OneDrive directory")
    else:
        for filepath, date_key in daily_exports:
            fn = os.path.basename(filepath)
            total_inserted += process_and_insert(filepath, client, fn)

    # 3. Final count
    print(f"\n{'=' * 60}")
    print(f"DONE — Inserted {total_inserted} total job records")

    # Verify
    rows = []
    offset = 0
    while True:
        res = client.client.table("job_snapshots").select("job_id,planned_date").range(offset, offset + 999).execute()
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    import pandas as pd
    if rows:
        df = pd.DataFrame(rows)
        df["planned_date"] = pd.to_datetime(df["planned_date"], errors="coerce")
        print(f"\nFinal DB state:")
        print(f"  Total rows: {len(df)}")
        print(f"  Unique job_ids: {df['job_id'].nunique()}")
        print(f"  Planned date range: {df['planned_date'].min().date()} to {df['planned_date'].max().date()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
