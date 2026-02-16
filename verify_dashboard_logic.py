import pandas as pd
from utils.api import get_token, fetch_jobs, process_data
from datetime import datetime
from dotenv import load_dotenv
import os
import sys

# Load environment variables
load_dotenv()

# Set encoding for Windows console compatibility
sys.stdout.reconfigure(encoding='utf-8')

def verify_dashboard_logic():
    with open('verification_results_utf8.txt', 'w', encoding='utf-8') as f:
        def log(msg):
            print(msg)
            f.write(str(msg) + '\n')
            
        log("🚀 Starting Dashboard Logic Verification...")
        
        # 1. Fetch real data
        token = get_token()
        if not token:
            log("❌ Auth failed")
            return
        
        jobs = fetch_jobs(token)
        df_filtered = process_data(jobs)
        
        log(f"✅ Fetched {len(df_filtered)} jobs")
        
        if df_filtered.empty:
            log("⚠️ No data to verify")
            return

        # 2. Mimic KPI Logic
        log("\n📊 Verifying KPI Logic:")
        total_jobs = len(df_filtered)
        
        if 'Delay_Days' in df_filtered.columns and 'Actual_Date' in df_filtered.columns:
            arrived_jobs = df_filtered[df_filtered['Actual_Date'].notna()]
            arrived_count = len(arrived_jobs)
            
            on_time_count = len(arrived_jobs[arrived_jobs['Delay_Days'] <= 0])
            on_time_pct = (on_time_count / arrived_count * 100) if arrived_count > 0 else 0
            
            log(f"   - Total Jobs: {total_jobs}")
            log(f"   - Arrived Jobs: {arrived_count}")
            log(f"   - On-Time (Arrival <= Plan): {on_time_count}")
            log(f"   - On-Time %: {on_time_pct:.1f}%")
        else:
            log("❌ Missing required columns for KPIs")

        # 3. Mimic Watchlist Logic
        log("\n🚨 Verifying Watchlist Logic:")
        today = datetime.now().date()
        if 'Planned_Date' in df_filtered.columns and 'Actual_Date' in df_filtered.columns:
            pending_arrivals = df_filtered[
                (df_filtered['Planned_Date'].dt.date < today) & 
                (df_filtered['Actual_Date'].isna())
            ]
            count = len(pending_arrivals)
            log(f"   - Overdue Arrivals (Plan < Today & No Actual): {count}")
            if count > 0:
                log("   - Sample Overdue:")
                log(pending_arrivals[['BOL_Number', 'Planned_Date', 'Carrier']].head(3).to_string())
        else:
            log("❌ Missing required columns for Watchlist")

        # 4. Mimic Funnel Logic
        log("\nvals for Funnel:")
        manifested = len(df_filtered)
        arrived = len(df_filtered[df_filtered['Actual_Date'].notna()])
        
        # Simple routed check
        if 'Status' in df_filtered.columns:
            routed = len(df_filtered[(df_filtered['Actual_Date'].notna()) & (~df_filtered['Status'].isin(['Imported', 'Scanned', 'Unknown']))])
        else:
            routed = 0
            
        confirmed = len(df_filtered[df_filtered['Confirmation_Status'].str.contains('Confirmed', case=False, na=False)]) if 'Confirmation_Status' in df_filtered.columns else 0
        delivered = len(df_filtered[df_filtered['Status'].isin(['Delivered', 'Complete', 'Completed'])]) if 'Status' in df_filtered.columns else 0

        log(f"   - Manifested: {manifested}")
        log(f"   - Arrived: {arrived}")
        log(f"   - Routed: {routed}")
        log(f"   - Confirmed: {confirmed}")
        log(f"   - Delivered: {delivered}")

        # Check for logical consistency (Funnel should generally shrink, though not strictly required if data incomplete)
        if not (manifested >= arrived):
            log("⚠️ Warning: More arrived than manifested? (Possible if loose filtering)")
        
        log("\n✅ Verification Complete.")

if __name__ == "__main__":
    verify_dashboard_logic()
