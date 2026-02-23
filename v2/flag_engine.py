"""
Flag Engine — V2.0 Hardwired Business Rules

Evaluates every Entered/Scheduled job and assigns a computed flag:
  - green  : In Progress (scanned + driver assigned)
  - yellow : Scanned, no driver — within 24-hr SLA window
  - red    : Any breach condition (see RULE constants below)
  - none   : Job not yet in a flaggable state (no scan, no driver)

Rule summary
------------
Rule 1 — SCANNED, NO DRIVER (Yellow → Red)
  - 0 to <24 hrs since scan  →  🟡 Yellow
  - ≥48 hrs since scan       →  🔴 Red
  (The 24–48 hr band stays Yellow — escalates only at 48 hrs)

Rule 2 — DRIVER ASSIGNED, NOT SCANNED (immediate Red)
  Operations must contact Warehouse before delivery.

Rule 3 — PEPMOVE LEG DELAY (Red after 1 day past Planned_Date)
  Applies when the delivery stop is PEPMOVE, not the end customer.
  Detected via keyword match on Customer_Name / Delivery_Address.
  (Override: set IS_PEPMOVE_KEYWORDS to [] and use is_pepmove_leg
   column from FileMaker once that field is available.)

Status scoping
--------------
Flags are only evaluated for jobs whose FileMaker status is
Entered or Scheduled (case-insensitive).  Completed / Canceled /
Re-scheduled jobs keep flag = 'none' — they are handled elsewhere.
"""

import pandas as pd
from datetime import datetime, timezone
from typing import List

# ── Configurable Constants ────────────────────────────────────────────────────

# FileMaker statuses that receive flag evaluation
ACTIVE_STATUSES: List[str] = ['entered', 'scheduled']

# Hours thresholds for Rule 1 (Scanned, no driver)
SLA_YELLOW_HOURS: float = 24.0   # warn at 24 hrs
SLA_RED_HOURS: float = 48.0      # breach at 48 hrs

# Keywords for PEPMOVE leg detection (Option B — pattern match)
# Add "PEPMOVE" and common variants; all comparisons are case-insensitive.
IS_PEPMOVE_KEYWORDS: List[str] = ['pepmove', 'pep move', 'pep-move']

# How many days past Planned_Date before a PEPMOVE leg turns Red
PEPMOVE_RED_DAYS: int = 1


# ── Flag Computation ──────────────────────────────────────────────────────────

def _is_active_status(status: str) -> bool:
    """Return True if the job status should be evaluated for flags."""
    if not status or pd.isna(status):
        return False
    return str(status).strip().lower() in ACTIVE_STATUSES


def _is_pepmove_leg(customer_name: str, delivery_address: str) -> bool:
    """
    Detect PEPMOVE-destined legs via keyword matching.

    Override this function once FileMaker exports an explicit field.
    """
    haystack = f"{customer_name or ''} {delivery_address or ''}".lower()
    return any(kw in haystack for kw in IS_PEPMOVE_KEYWORDS)


def _hours_since(ts) -> float:
    """
    Return hours elapsed since *ts* (a pandas Timestamp or datetime).
    Returns infinity if timestamp is NaT/None so comparisons stay safe.
    """
    if ts is None or pd.isna(ts):
        return float('inf')

    now = datetime.now(timezone.utc)

    # Make ts timezone-aware if it is naive
    if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    elif hasattr(ts, 'tz') and ts.tz is None:
        ts = ts.tz_localize('UTC')

    delta = now - ts
    return delta.total_seconds() / 3600.0


def _days_past_planned(planned_date) -> int:
    """
    Return how many full calendar days today is past planned_date.
    Negative means planned date is in the future.
    """
    if planned_date is None or pd.isna(planned_date):
        return 0
    today = datetime.now(timezone.utc).date()
    p = pd.Timestamp(planned_date).date()
    return (today - p).days


def evaluate_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main entry point.  Adds computed flag columns to *df* in place.

    New columns added
    -----------------
    computed_flag    : 'green' | 'yellow' | 'red' | 'none'
    flag_reason      : human-readable explanation string
    sla_hours_elapsed: float — hours since SLA clock started (NaN if N/A)
    sla_breach_level : 'ok' | 'warning_24hr' | 'breach_48hr' | 'n/a'
    is_pepmove_leg   : bool

    Args:
        df: Processed DataFrame (output of data_processor.process_data)

    Returns:
        Same DataFrame with flag columns appended.
    """
    results = []

    # Normalise column presence — default to empty if missing
    def _col(row, name, default=''):
        val = row.get(name, default)
        return '' if pd.isna(val) else val

    for _, row in df.iterrows():
        status = str(_col(row, 'Status', '')).strip()

        # ── Non-flaggable statuses ──────────────────────────────────────────
        if not _is_active_status(status):
            results.append({
                'computed_flag': 'none',
                'flag_reason': f'Status "{status}" not flagged',
                'sla_hours_elapsed': float('nan'),
                'sla_breach_level': 'n/a',
                'is_pepmove_leg': False,
            })
            continue

        # Gather key facts about this job
        has_scan      = False
        has_driver    = False
        scan_ts       = None
        planned_date  = None
        customer_name = str(_col(row, 'Customer_Name', ''))
        delivery_addr = str(_col(row, 'Delivery_Address', ''))

        # Scan presence
        scan_user  = str(_col(row, 'Scan_User', ''))
        scan_count = pd.to_numeric(row.get('Scan_Count', 0), errors='coerce')
        if (scan_user and scan_user.lower() not in ['', 'nan', 'none']) or \
           (scan_count and scan_count > 0):
            has_scan = True
            scan_ts  = row.get('Scan_Timestamp', None)

        # Driver presence
        driver = str(_col(row, 'Assigned_Driver', ''))
        if driver and driver.lower() not in ['', 'nan', 'none']:
            has_driver = True

        # Planned date
        planned_date = row.get('Planned_Date', None)

        # PEPMOVE leg detection
        pepmove = _is_pepmove_leg(customer_name, delivery_addr)

        # ── Green: both conditions met ──────────────────────────────────────
        if has_scan and has_driver:
            results.append({
                'computed_flag': 'green',
                'flag_reason': 'In Progress: scanned and driver assigned',
                'sla_hours_elapsed': float('nan'),
                'sla_breach_level': 'ok',
                'is_pepmove_leg': pepmove,
            })
            continue

        # ── Rule 2: Driver assigned but NOT scanned → immediate Red ────────
        if has_driver and not has_scan:
            results.append({
                'computed_flag': 'red',
                'flag_reason': 'Driver assigned but unit NOT scanned — '
                               'contact Warehouse to scan BEFORE delivery',
                'sla_hours_elapsed': float('nan'),
                'sla_breach_level': 'n/a',
                'is_pepmove_leg': pepmove,
            })
            continue

        # ── Rule 1: Scanned, no driver → Yellow or Red ─────────────────────
        if has_scan and not has_driver:
            hrs = _hours_since(scan_ts)

            if hrs >= SLA_RED_HOURS:
                flag         = 'red'
                breach_level = 'breach_48hr'
                reason       = (f'Driver SLA BREACHED: {hrs:.1f} hrs since scan '
                                f'(limit: {SLA_RED_HOURS:.0f} hrs)')
            else:
                flag         = 'yellow'
                breach_level = 'warning_24hr'
                remaining    = SLA_RED_HOURS - hrs
                reason       = (f'Assign driver: {hrs:.1f} hrs since scan, '
                                f'{remaining:.1f} hrs until breach')

            results.append({
                'computed_flag': flag,
                'flag_reason': reason,
                'sla_hours_elapsed': round(hrs, 2) if hrs != float('inf') else float('nan'),
                'sla_breach_level': breach_level,
                'is_pepmove_leg': pepmove,
            })
            continue

        # ── Rule 3: PEPMOVE leg delay ───────────────────────────────────────
        # Only applies when there is no scan and no driver yet
        # (i.e., the unit hasn't arrived at PEPMOVE's dock)
        if pepmove:
            days_late = _days_past_planned(planned_date)
            if days_late >= PEPMOVE_RED_DAYS:
                results.append({
                    'computed_flag': 'red',
                    'flag_reason': (f'PEPMOVE delivery overdue by {days_late} day(s) — '
                                   'daily tracking required'),
                    'sla_hours_elapsed': float(days_late * 24),
                    'sla_breach_level': 'breach_48hr',
                    'is_pepmove_leg': True,
                })
                continue
            else:
                # On-time PEPMOVE leg — still no scan/driver, just waiting
                results.append({
                    'computed_flag': 'none',
                    'flag_reason': 'PEPMOVE leg — on schedule, awaiting arrival',
                    'sla_hours_elapsed': float('nan'),
                    'sla_breach_level': 'ok',
                    'is_pepmove_leg': True,
                })
                continue

        # ── No scan, no driver, not PEPMOVE — neutral / manifested ─────────
        results.append({
            'computed_flag': 'none',
            'flag_reason': 'Manifested — awaiting scan and driver assignment',
            'sla_hours_elapsed': float('nan'),
            'sla_breach_level': 'n/a',
            'is_pepmove_leg': False,
        })

    flag_df = pd.DataFrame(results, index=df.index)
    for col in flag_df.columns:
        df[col] = flag_df[col]

    return df


# ── Convenience Summariser ────────────────────────────────────────────────────

def flag_summary(df: pd.DataFrame) -> dict:
    """
    Returns a count dict for each flag level, for KPI cards.

    Example output:
      {'green': 12, 'yellow': 3, 'red': 5, 'none': 8,
       'red_no_scan': 2, 'red_driver_sla': 1, 'red_pepmove': 2}
    """
    if 'computed_flag' not in df.columns:
        return {'green': 0, 'yellow': 0, 'red': 0, 'none': 0}

    summary = {
        'green':  int((df['computed_flag'] == 'green').sum()),
        'yellow': int((df['computed_flag'] == 'yellow').sum()),
        'red':    int((df['computed_flag'] == 'red').sum()),
        'none':   int((df['computed_flag'] == 'none').sum()),
    }

    # Break red into sub-types for the KPI cards
    if 'flag_reason' in df.columns:
        red_rows = df[df['computed_flag'] == 'red']['flag_reason'].astype(str)
        summary['red_no_scan']   = int(red_rows.str.contains('NOT scanned', case=False).sum())
        summary['red_driver_sla'] = int(red_rows.str.contains('Driver SLA BREACHED', case=False).sum())
        summary['red_pepmove']   = int(red_rows.str.contains('PEPMOVE', case=False).sum())

    return summary


# ── Standalone Test ───────────────────────────────────────────────────────────

if __name__ == '__main__':
    from datetime import timedelta

    print("Flag Engine — Unit Test")
    print("=" * 50)

    now = datetime.now(timezone.utc)

    # Build a minimal test DataFrame
    test_data = [
        # Green: scanned + driver
        {'Status': 'Entered', 'Scan_User': 'Derek', 'Scan_Count': 1,
         'Scan_Timestamp': now - timedelta(hours=10),
         'Assigned_Driver': 'John D',
         'Customer_Name': 'Smith Residence', 'Delivery_Address': '123 Main St',
         'Planned_Date': now.date()},

        # Yellow: scanned 30 hrs ago, no driver
        {'Status': 'Scheduled', 'Scan_User': 'Derek', 'Scan_Count': 1,
         'Scan_Timestamp': now - timedelta(hours=30),
         'Assigned_Driver': '',
         'Customer_Name': 'Jones Home', 'Delivery_Address': '456 Oak Ave',
         'Planned_Date': now.date()},

        # Red: driver assigned, not scanned
        {'Status': 'Entered', 'Scan_User': '', 'Scan_Count': 0,
         'Scan_Timestamp': None,
         'Assigned_Driver': 'Mike R',
         'Customer_Name': 'Brown Corp', 'Delivery_Address': '789 Elm St',
         'Planned_Date': now.date()},

        # Red: driver SLA breached (50 hrs since scan)
        {'Status': 'Entered', 'Scan_User': 'Derek', 'Scan_Count': 1,
         'Scan_Timestamp': now - timedelta(hours=50),
         'Assigned_Driver': '',
         'Customer_Name': 'Taylor Home', 'Delivery_Address': '321 Pine Rd',
         'Planned_Date': now.date()},

        # Red: PEPMOVE leg overdue (1 day late)
        {'Status': 'Scheduled', 'Scan_User': '', 'Scan_Count': 0,
         'Scan_Timestamp': None,
         'Assigned_Driver': '',
         'Customer_Name': 'PEPMOVE Warehouse', 'Delivery_Address': '1 PEPMOVE Blvd',
         'Planned_Date': (now - timedelta(days=2)).date()},

        # None: Completed — not flagged
        {'Status': 'Complete', 'Scan_User': 'Derek', 'Scan_Count': 2,
         'Scan_Timestamp': now - timedelta(days=3),
         'Assigned_Driver': 'John D',
         'Customer_Name': 'Done Customer', 'Delivery_Address': '999 Final Ave',
         'Planned_Date': (now - timedelta(days=3)).date()},
    ]

    df_test = pd.DataFrame(test_data)
    df_result = evaluate_flags(df_test)

    for i, row in df_result.iterrows():
        print(f"\n[{i}] Status: {row['Status']}")
        print(f"    Flag:   {row['computed_flag'].upper()}")
        print(f"    Reason: {row['flag_reason']}")
        print(f"    SLA hrs: {row['sla_hours_elapsed']}")
        print(f"    PEPMOVE: {row['is_pepmove_leg']}")

    print("\n\nSummary:", flag_summary(df_result))
    print("\n[OK] Flag engine test complete.")
