"""
BYD/Valley Tracking V2.0 - Main Orchestrator

Coordinates the entire data processing pipeline:
1. Load manual export
2. Process and enrich data
3. Calculate KPIs
4. Store in Supabase
5. Generate HTML email report
6. Send to configured recipients
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from v2.data_processor import load_manual_export, process_data, calculate_kpis
from v2.supabase_client import SupabaseClient
from v2.email_generator import generate_html_report, send_email
from v2.comparator import compare_snapshots
from v2.job_chains import process_job_chains, get_chain_alerts


def main(export_filepath: str = None, dry_run: bool = False):
    """
    Main execution pipeline.
    
    Args:
        export_filepath: Path to manual export file (or from env var)
        dry_run: If True, processes data but doesn't send email or store in DB
    """
    print("=" * 60)
    print("BYD/Valley Tracking V2.0 - Report Generator")
    print("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Get export file path
    if export_filepath is None:
        export_filepath = os.getenv('EXPORT_FILE_PATH', 'scanfieldtest.xlsx')
    
    print(f"\nLoading export: {export_filepath}")
    
    # Step 1: Load and process data
    try:
        df_raw = load_manual_export(export_filepath)
        df_processed = process_data(df_raw)
        print(f"[OK] Processed {len(df_processed)} records")
    except Exception as e:
        print(f"[ERROR] Error loading/processing data: {e}")
        return False
    
    # Step 2: Calculate KPIs
    try:
        kpis = calculate_kpis(df_processed)
        print(f"\nKPI Summary:")
        print(f"  Total Jobs: {kpis['total_jobs']}")
        print(f"  On-Time %: {kpis['on_time_pct']:.1f}%")
        print(f"  Avg Delay: {kpis['avg_delay_days']:.1f} days")
        print(f"  Overdue: {kpis['overdue_count']}")
        print(f"  Ready for Routing: {kpis['ready_for_routing']}")
    except Exception as e:
        print(f"[ERROR] Error calculating KPIs: {e}")
        return False
    
    # Step 3: Supabase integration & Delta Calculation
    trends = {}
    deltas = {}
    
    # Always try to connect to Supabase to get history for Deltas (even in dry run if possible)
    # But if dry_run, we don't write.
    
    try:
        print(f"\nConnecting to Supabase...")
        supabase = SupabaseClient()
        
        # 3a. Get previous snapshot for DELTAS (before inserting new one)
        previous_snapshot = supabase.get_latest_snapshot()
        
        # Calculate deltas
        print("Calculating daily deltas...")
        deltas = compare_snapshots(df_processed, previous_snapshot)
        
        print(f"  New Jobs: {len(deltas['new_jobs'])}")
        print(f"  New Arrivals: {len(deltas['new_arrivals'])}")
        print(f"  New Deliveries: {len(deltas['new_deliveries'])}")
        
        if not dry_run:
            # Insert snapshot
            supabase.insert_snapshot(df_processed)
            
            # Insert KPIs
            supabase.insert_kpis(kpis)
            
            # Process job chains (reschedule tracking)
            print("\nProcessing job chains...")
            chain_stats = process_job_chains(df_processed, supabase.client)
            if chain_stats.get('chains_processed', 0) > 0:
                print(f"  Chains: {chain_stats['chains_processed']} processed, "
                      f"{chain_stats['new_chains_created']} new")
            
            # Get chain alerts
            chain_alerts = get_chain_alerts(supabase.client)
            if chain_alerts:
                critical = len([a for a in chain_alerts if a['severity'] == 'critical'])
                warning = len([a for a in chain_alerts if a['severity'] == 'warning'])
                print(f"  Chain Alerts: {critical} critical, {warning} warnings")
            
            # Get trends
            trends = supabase.compare_with_history(kpis)
            print(f"[OK] Data stored in Supabase with trend analysis")
        else:
            print("\n[WARN] DRY RUN: Skipping Supabase WRITE (Read-only for deltas)")
            # In dry run, we still calculated deltas from history if available
            trends = {key: '->' for key in ['on_time_pct', 'avg_delay_days', 'overdue_count']}
            
    except Exception as e:
        print(f"[WARN] Supabase error (continuing without trends/deltas): {e}")
        trends = {key: '->' for key in ['on_time_pct', 'avg_delay_days', 'overdue_count']}
        deltas = {'new_jobs': [], 'new_arrivals': [], 'new_deliveries': [], 'new_overdue': []}
    
    # Step 4: Generate HTML report
    # SKIPPED per user request (2026-02-16)
    # try:
    #     print(f"\nGenerating HTML email report...")
    #     html_content = generate_html_report(df_processed, kpis, trends, deltas)
    #     print(f"✓ HTML report generated")
    # except Exception as e:
    #     print(f"❌ Error generating report: {e}")
    #     return False
    
    # Step 5: Send email
    # SKIPPED per user request (2026-02-16)
    # if not dry_run:
    #     try:
    #         recipients_str = os.getenv('EMAIL_RECIPIENTS', '')
    #         if not recipients_str:
    #             print("⚠ No EMAIL_RECIPIENTS configured in .env")
    #             return False
    #         
    #         recipients = [email.strip() for email in recipients_str.split(',')]
    #         
    #         # Check if we should display draft or auto-send
    #         display_only = os.getenv('EMAIL_DISPLAY_ONLY', 'true').lower() == 'true'
    #         
    #         if display_only:
    #             print(f"\nOpening email draft in Outlook for {len(recipients)} recipient(s)...")
    #             print("  (Set EMAIL_DISPLAY_ONLY=false in .env to auto-send)")
    #         else:
    #             print(f"\nSending email to {len(recipients)} recipient(s)...")
    #         
    #         send_email(html_content, recipients, display_only=display_only)
    #         
    #         if display_only:
    #             print("✓ Email draft opened - review and click Send when ready")
    #         else:
    #             print("✓ Email sent successfully")
    #     except Exception as e:
    #         print(f"❌ Error with email: {e}")
    #         return False
    # else:
    #     print("\n⚠ DRY RUN: Skipping email send")
    #     # Save HTML to file for preview
    #     with open('temp_report_preview.html', 'w', encoding='utf-8') as f:
    #         f.write(html_content)
    #     print(f"✓ Preview saved to temp_report_preview.html")

    
    print("\n" + "=" * 60)
    print("SUCCESS: V2.0 Report Generation Complete!")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='BYD/Valley Tracking V2.0 Report Generator')
    parser.add_argument('--file', '-f', help='Path to manual export file')
    parser.add_argument('--dry-run', action='store_true', help='Process but don\'t send/store')
    
    args = parser.parse_args()
    
    success = main(export_filepath=args.file, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
