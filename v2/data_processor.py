"""
V2 Data Processor Module

Processes manual FileMaker exports with enhanced field support.
Reuses V1 logic while adding scan validation and enriched product data.
"""

import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional


def load_manual_export(filepath: str) -> pd.DataFrame:
    """
    Loads manual Excel export from FileMaker.
    
    Args:
        filepath: Path to .xlsx file (fieldtest2.xlsx format — 34 columns)
        
    Returns:
        Raw DataFrame
    """
    try:
        df = pd.read_excel(filepath)
        print(f"[OK] Loaded {len(df)} records with {len(df.columns)} columns")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"Export file not found: {filepath}")
    except Exception as e:
        raise Exception(f"Error loading export: {e}")


def parse_scan_data(json_str: str) -> List[Dict]:
    """
    Parses box_serial_numbers_scanned_received_json field.
    
    JSON Format:
    {
        "6302168": {
            "latitude": "",
            "longitude": "",
            "manual": 0,
            "timestamp": "2/11/2026 8:37:58 AM",
            "username": "Derek Wyant"
        },
        ...
    }
    
    Args:
        json_str: Raw JSON string from Excel
        
    Returns:
        List of scan events with parsed data
    """
    if pd.isna(json_str) or not json_str or str(json_str).strip() == '':
        return []
    
    try:
        data = json.loads(json_str)
        scans = []
        
        for serial, details in data.items():
            scans.append({
                'serial_number': serial,
                'username': details.get('username', 'Unknown'),
                'timestamp': details.get('timestamp', ''),
                'manual': details.get('manual', 0) == 1,
                'latitude': details.get('latitude', ''),
                'longitude': details.get('longitude', '')
            })
        
        return scans
    except json.JSONDecodeError:
        print(f"[WARN] Unable to parse scan JSON: {json_str[:50]}...")
        return []


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes raw export data into standardized format.
    Reuses V1 logic with additions for scan validation and product info.
    
    Args:
        df: Raw DataFrame from manual export
        
    Returns:
        Processed DataFrame with calculated fields
    """
    processed = df.copy()
    
    # === REUSED FROM V1 === #
    
    # 1. Date Handling
    if 'job_date' in processed.columns:
        processed['Planned_Date'] = pd.to_datetime(processed['job_date'], errors='coerce')
    else:
        processed['Planned_Date'] = pd.NaT

    # 2. Actual Date (job_date + time_complete)
    def parse_actual_date(row):
        if 'job_date' not in row or 'time_complete' not in row:
            return pd.NaT
            
        d = str(row['job_date']).strip()
        t = str(row['time_complete']).strip()
        
        if not t or t in ['nan', 'None', '']:
            return pd.NaT
            
        if not d or d in ['nan', 'None', '']:
            return pd.NaT
            
        combined = f"{d} {t}"
        try:
            return pd.to_datetime(combined, dayfirst=False)
        except:
            return pd.NaT

    if 'time_complete' in processed.columns and 'job_date' in processed.columns:
        processed['Actual_Date'] = processed.apply(parse_actual_date, axis=1)
    else:
        processed['Actual_Date'] = pd.NaT

    # 3. Job ID and Stop Number
    if '_kp_job_id' in processed.columns:
        processed['Job_ID'] = processed['_kp_job_id'].astype(str)
    else:
        processed['Job_ID'] = ''
    
    if 'order_C1' in processed.columns:
        processed['Stop_Number'] = processed['order_C1'].astype(str).replace('nan', '').replace('None', '')
    else:
        processed['Stop_Number'] = ''

    # 4. Status, State, Carrier
    if 'job_status' in processed.columns:
        processed['Status'] = processed['job_status'].astype(str)
    else:
        processed['Status'] = 'Unknown'

    if '_kf_state_id' in processed.columns:
        processed['State'] = processed['_kf_state_id'].astype(str).replace('nan', 'Unknown')
    else:
        processed['State'] = 'Unknown'

    if '_kf_client_code_id' in processed.columns:
        processed['Carrier'] = processed['_kf_client_code_id'].astype(str)
    else:
        processed['Carrier'] = 'Unknown'

    # 5. Delay Days
    if 'Planned_Date' in processed.columns and 'Actual_Date' in processed.columns:
        processed['Delay_Days'] = (processed['Actual_Date'] - processed['Planned_Date']).dt.days
    else:
        processed['Delay_Days'] = None

    # 6. Assigned Driver / Is Routed
    if '_kf_lead_id' in processed.columns:
        processed['Assigned_Driver'] = processed['_kf_lead_id'].astype(str).replace('nan', '')
    else:
        processed['Assigned_Driver'] = ''
        
    processed['Is_Routed'] = processed['Assigned_Driver'].apply(
        lambda x: True if x and x.lower() != 'unknown' and x != '' else False
    )

    # 7. Confirmation Status
    if '_kf_notification_id' in processed.columns:
        processed['Confirmation_Status'] = processed['_kf_notification_id'].astype(str).replace('nan', '')
    else:
        processed['Confirmation_Status'] = 'Unknown'

    # === NEW FOR V2 === #
    
    # 8. Product Description (now available!)
    if 'description_product' in processed.columns:
        processed['Product_Description'] = processed['description_product'].astype(str).replace('nan', '')
    else:
        processed['Product_Description'] = ''
    
    # 9. Product Serial Number
    if 'product_serial_number' in processed.columns:
        processed['Product_Serial'] = processed['product_serial_number'].astype(str).replace('nan', '')
    else:
        processed['Product_Serial'] = ''

    # 10. Parse Scan Validation Data
    if 'box_serial_numbers_scanned_received_json' in processed.columns:
        processed['Scan_Events'] = processed['box_serial_numbers_scanned_received_json'].apply(parse_scan_data)
        
        # Extract scan summary
        processed['Scan_Count'] = processed['Scan_Events'].apply(len)
        processed['Scan_User'] = processed['Scan_Events'].apply(
            lambda scans: scans[0]['username'] if scans else ''
        )
        processed['Scan_Timestamp'] = processed['Scan_Events'].apply(
            lambda scans: pd.to_datetime(scans[0]['timestamp']) if scans else pd.NaT
        )
    else:
        processed['Scan_Events'] = [[] for _ in range(len(processed))]
        processed['Scan_Count'] = 0
        processed['Scan_User'] = ''
        processed['Scan_Timestamp'] = pd.NaT

    # === NEW FIELDS (Feb 2026 Export Update) === #
    
    # 11. Piece Count
    if 'piece_total' in processed.columns:
        processed['Piece_Count'] = pd.to_numeric(processed['piece_total'], errors='coerce').fillna(0).astype(int)
    else:
        processed['Piece_Count'] = 0
    
    # 12. White Glove Service
    if 'white_glove' in processed.columns:
        # Convert to boolean - handle various formats (1/0, Yes/No, True/False)
        processed['White_Glove'] = processed['white_glove'].apply(
            lambda x: str(x).strip().lower() in ['1', 'yes', 'true', 'y'] if pd.notna(x) else False
        )
    else:
        processed['White_Glove'] = False
    
    # 13. Notification Detail
    if 'notification_detail' in processed.columns:
        processed['Notification_Detail'] = processed['notification_detail'].astype(str).replace('nan', '')
    else:
        processed['Notification_Detail'] = ''
    
    # 14. Miles (Distance from warehouse to delivery)
    if '_kf_miles_oneway_id' in processed.columns:
        processed['Miles_OneWay'] = pd.to_numeric(processed['_kf_miles_oneway_id'], errors='coerce').fillna(0).round(1)
    else:
        processed['Miles_OneWay'] = 0.0

    # ================================================================
    # NEW FIELDS — fieldtest2.xlsx additions
    # ================================================================

    # 15. Market (geographic region)
    if '_kf_market_id' in processed.columns:
        processed['Market'] = processed['_kf_market_id'].astype(str).replace('nan', 'Unknown')
    else:
        processed['Market'] = 'Unknown'

    # 16. City
    if '_kf_city_id' in processed.columns:
        processed['City'] = processed['_kf_city_id'].astype(str).replace('nan', '')
    else:
        processed['City'] = ''

    # 17. Customer Name
    if 'Customer_C1' in processed.columns:
        processed['Customer_Name'] = processed['Customer_C1'].astype(str).replace('nan', '')
    else:
        processed['Customer_Name'] = ''

    # 18. Delivery Address
    if 'address_C1' in processed.columns:
        processed['Delivery_Address'] = processed['address_C1'].astype(str).replace('nan', '')
    else:
        processed['Delivery_Address'] = ''

    # 19. Date Received (when BYD received the order)
    if 'date_received' in processed.columns:
        processed['Date_Received'] = pd.to_datetime(processed['date_received'], errors='coerce')
    else:
        processed['Date_Received'] = pd.NaT

    # 20. Job Created At (FileMaker creation timestamp)
    if 'timestamp_create' in processed.columns:
        processed['Job_Created_At'] = pd.to_datetime(processed['timestamp_create'], errors='coerce')
    else:
        processed['Job_Created_At'] = pd.NaT

    # 21. Client Order Number (BYD/Valley PO reference)
    if 'client_order_number' in processed.columns:
        processed['Client_Order_Number'] = processed['client_order_number'].astype(str).replace('nan', '')
    else:
        processed['Client_Order_Number'] = ''

    # 22. Prior Job ID (direct reschedule reference — replaces serial inference)
    if 'job_reference_prior' in processed.columns:
        processed['Prior_Job_ID'] = processed['job_reference_prior'].apply(
            lambda x: str(int(x)) if pd.notna(x) else ''
        )
    else:
        processed['Prior_Job_ID'] = ''

    # 23. Signed By (proof-of-delivery signature)
    if 'signed_by' in processed.columns:
        processed['Signed_By'] = processed['signed_by'].astype(str).replace('nan', '')
    else:
        processed['Signed_By'] = ''

    # 24. Delivery Scan Events (GPS-tagged delivery confirmation)
    if 'box_serial_numbers_scanned_delivered_json' in processed.columns:
        processed['Delivery_Scan_Events'] = processed['box_serial_numbers_scanned_delivered_json'].apply(parse_scan_data)
        processed['Delivery_Scan_Count'] = processed['Delivery_Scan_Events'].apply(len)
    else:
        processed['Delivery_Scan_Events'] = [[] for _ in range(len(processed))]
        processed['Delivery_Scan_Count'] = 0

    # 25. Product Weight (lbs)
    if '_kf_product_weight_id' in processed.columns:
        processed['Product_Weight_Lbs'] = pd.to_numeric(processed['_kf_product_weight_id'], errors='coerce').fillna(0).astype(int)
    else:
        processed['Product_Weight_Lbs'] = 0

    # 26. Crew Required
    if 'people_required' in processed.columns:
        processed['Crew_Required'] = pd.to_numeric(processed['people_required'], errors='coerce').fillna(1).astype(int)
    else:
        processed['Crew_Required'] = 1

    # 27. Driver Notes
    if 'notes_driver' in processed.columns:
        processed['Driver_Notes'] = processed['notes_driver'].astype(str).replace('nan', '')
    else:
        processed['Driver_Notes'] = ''

    # 28. Job Type (Delivery / Pickup / Out)
    if 'job_type' in processed.columns:
        processed['Job_Type'] = processed['job_type'].astype(str).replace('nan', 'Delivery')
    else:
        processed['Job_Type'] = 'Delivery'

    # ================================================================
    # CALCULATED FIELDS
    # ================================================================

    # Arrival Time (job_date + time_arival → full timestamp)
    def parse_arrival_time(row):
        d = str(row.get('job_date', '')).strip()
        t = str(row.get('time_arival', '')).strip()
        if not t or t in ['nan', 'None', ''] or not d or d in ['nan', 'None', '']:
            return pd.NaT
        try:
            return pd.to_datetime(f"{d} {t}", dayfirst=False)
        except:
            return pd.NaT

    if 'time_arival' in processed.columns and 'job_date' in processed.columns:
        processed['Arrival_Time'] = processed.apply(parse_arrival_time, axis=1)
    else:
        processed['Arrival_Time'] = pd.NaT

    # Dwell Minutes: time from arrival to completion (on-site duration)
    if 'Arrival_Time' in processed.columns and 'Actual_Date' in processed.columns:
        dwell = (processed['Actual_Date'] - processed['Arrival_Time']).dt.total_seconds() / 60
        processed['Dwell_Minutes'] = dwell.where(dwell >= 0, other=None).round(1)
    else:
        processed['Dwell_Minutes'] = None

    # Lead Time Days: days from order received to planned delivery
    if 'Date_Received' in processed.columns and 'Planned_Date' in processed.columns:
        lead = (processed['Planned_Date'] - processed['Date_Received']).dt.days
        processed['Lead_Time_Days'] = lead.where(lead >= 0, other=None)
    else:
        processed['Lead_Time_Days'] = None

    return processed


def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate jobs based on Product Serial Number.
    Keeps only the most recent job for each serial (based on Planned_Date).
    
    Args:
        df: Processed DataFrame
        
    Returns:
        DataFrame with duplicates removed
    """
    if df.empty or 'Product_Serial' not in df.columns:
        return df
        
    # Filter for valid serials
    # (We only dedup if there is a valid serial, otherwise we keep them)
    # Actually, if there is no serial, we can't dedup, so we leave them alone.
    
    # Separate records with no serial (keep all of them)
    no_serial = df[
        (df['Product_Serial'].isna()) | 
        (df['Product_Serial'] == '') | 
        (df['Product_Serial'].astype(str).str.lower() == 'nan') |
        (df['Product_Serial'].astype(str).str.lower() == 'none')
    ].copy()
    
    # Records with serial (deduplicate these)
    has_serial = df[~df.index.isin(no_serial.index)].copy()
    
    if has_serial.empty:
        return df
        
    # Sort by Job_Created_At first (most reliable), fall back to Planned_Date
    if 'Job_Created_At' in has_serial.columns and has_serial['Job_Created_At'].notna().any():
        has_serial = has_serial.sort_values('Job_Created_At', ascending=False, na_position='last')
    elif 'Planned_Date' in has_serial.columns:
        has_serial = has_serial.sort_values('Planned_Date', ascending=False, na_position='last')
        
    # Drop duplicates, keeping the first (latest)
    deduped = has_serial.drop_duplicates(subset=['Product_Serial'], keep='first')
    
    # Combine back
    result = pd.concat([deduped, no_serial])
    
    # Report
    removed_count = len(df) - len(result)
    if removed_count > 0:
        print(f"[INFO] Deduplicated jobs: Removed {removed_count} redundant records based on Product Serial")
        
    return result


def calculate_kpis(df: pd.DataFrame) -> Dict:
    """
    Calculates key performance indicators from processed data.
    
    Args:
        df: Processed DataFrame
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    
    # Total jobs
    kpis['total_jobs'] = len(df)
    
    # Arrivals
    arrived = df[df['Actual_Date'].notna()]
    kpis['arrived_count'] = len(arrived)
    
    # On-time percentage
    if len(arrived) > 0:
        on_time = len(arrived[arrived['Delay_Days'] <= 0])
        kpis['on_time_pct'] = (on_time / len(arrived)) * 100
    else:
        kpis['on_time_pct'] = 0
    
    # Average delay (only for late arrivals)
    late_arrivals = arrived[arrived['Delay_Days'] > 0]
    kpis['avg_delay_days'] = late_arrivals['Delay_Days'].mean() if len(late_arrivals) > 0 else 0
    
    # Overdue (planned before today, not arrived)
    today = datetime.now().date()
    overdue = df[(df['Planned_Date'].dt.date < today) & (df['Actual_Date'].isna())]
    kpis['overdue_count'] = len(overdue)
    
    # Ready for routing
    ready = df[(df['Actual_Date'].notna()) & (df['Is_Routed'] == False)]
    kpis['ready_for_routing'] = len(ready)
    
    # Scan validation
    kpis['avg_scans_per_job'] = df['Scan_Count'].mean()
    kpis['jobs_with_scans'] = len(df[df['Scan_Count'] > 0])
    
    return kpis


def calculate_carrier_kpis(df: pd.DataFrame) -> List[Dict]:
    """
    Calculates KPIs broken down by carrier.
    
    Args:
        df: Processed DataFrame (active jobs)
        
    Returns:
        List of dicts, one per carrier, each with KPI values
    """
    if df.empty or 'Carrier' not in df.columns:
        return []
    
    today = datetime.now().date()
    carrier_kpis = []
    
    for carrier, group in df.groupby('Carrier'):
        if not carrier or str(carrier).lower() in ['unknown', 'nan', 'none', '']:
            continue
        
        kpi = {'carrier': str(carrier)}
        kpi['total_jobs'] = len(group)
        
        # On-time percentage
        arrived = group[group['Actual_Date'].notna()]
        if len(arrived) > 0:
            on_time = len(arrived[arrived['Delay_Days'] <= 0])
            kpi['on_time_pct'] = round((on_time / len(arrived)) * 100, 1)
        else:
            kpi['on_time_pct'] = 0
        
        # Average delay (late arrivals only)
        late = arrived[arrived['Delay_Days'] > 0]
        kpi['avg_delay_days'] = round(float(late['Delay_Days'].mean()), 1) if len(late) > 0 else 0
        
        # Overdue count
        overdue = group[(group['Planned_Date'].dt.date < today) & (group['Actual_Date'].isna())]
        kpi['overdue_count'] = len(overdue)
        
        # Ready for routing
        ready = group[(group['Actual_Date'].notna()) & (group['Is_Routed'] == False)]
        kpi['ready_for_routing'] = len(ready)

        # Average dwell time (on-site minutes) — new
        if 'Dwell_Minutes' in group.columns:
            valid_dwell = group['Dwell_Minutes'].dropna()
            kpi['avg_dwell_minutes'] = round(float(valid_dwell.mean()), 1) if len(valid_dwell) > 0 else None
        else:
            kpi['avg_dwell_minutes'] = None

        # Average lead time (received → delivery days) — new
        if 'Lead_Time_Days' in group.columns:
            valid_lead = group['Lead_Time_Days'].dropna()
            kpi['avg_lead_time_days'] = round(float(valid_lead.mean()), 1) if len(valid_lead) > 0 else None
        else:
            kpi['avg_lead_time_days'] = None

        carrier_kpis.append(kpi)
    
    return carrier_kpis


def calculate_driver_kpis(df: pd.DataFrame) -> List[Dict]:
    """
    Calculates KPIs broken down by driver (Assigned_Driver).
    Requires _kf_lead_id to be populated (95% in fieldtest2.xlsx).

    Args:
        df: Processed DataFrame

    Returns:
        List of dicts, one per driver, each with KPI values
    """
    if df.empty or 'Assigned_Driver' not in df.columns:
        return []

    today = datetime.now().date()
    driver_kpis = []

    for driver, group in df.groupby('Assigned_Driver'):
        if not driver or str(driver).lower() in ['unknown', 'nan', 'none', '']:
            continue

        kpi = {'driver': str(driver)}
        kpi['total_jobs'] = len(group)

        # On-time percentage
        arrived = group[group['Actual_Date'].notna()]
        if len(arrived) > 0:
            on_time = len(arrived[arrived['Delay_Days'] <= 0])
            kpi['on_time_pct'] = round((on_time / len(arrived)) * 100, 1)
        else:
            kpi['on_time_pct'] = 0

        # Average delay (late arrivals only)
        late = arrived[arrived['Delay_Days'] > 0]
        kpi['avg_delay_days'] = round(float(late['Delay_Days'].mean()), 1) if len(late) > 0 else 0

        # Overdue count
        overdue = group[(group['Planned_Date'].dt.date < today) & (group['Actual_Date'].isna())]
        kpi['overdue_count'] = len(overdue)

        # Average dwell time
        if 'Dwell_Minutes' in group.columns:
            valid_dwell = group['Dwell_Minutes'].dropna()
            kpi['avg_dwell_minutes'] = round(float(valid_dwell.mean()), 1) if len(valid_dwell) > 0 else None
        else:
            kpi['avg_dwell_minutes'] = None

        # Signature rate (proof of delivery)
        if 'Signed_By' in group.columns:
            signed = group['Signed_By'].astype(str).str.strip()
            kpi['signature_rate_pct'] = round((signed.ne('').sum() / len(group)) * 100, 1)
        else:
            kpi['signature_rate_pct'] = 0

        # Market(s) this driver covers
        if 'Market' in group.columns:
            markets = group['Market'].dropna().unique().tolist()
            kpi['markets'] = [m for m in markets if str(m).lower() not in ['unknown', 'nan', '']]
        else:
            kpi['markets'] = []

        driver_kpis.append(kpi)

    # Sort by total jobs descending
    driver_kpis.sort(key=lambda x: x['total_jobs'], reverse=True)
    return driver_kpis


def calculate_historical_kpis(history_df: pd.DataFrame) -> Dict:
    """
    Calculates analytics from archived completed-job records.
    
    Args:
        history_df: DataFrame from job_history table
        
    Returns:
        Dictionary with historical analytics
    """
    if history_df is None or history_df.empty:
        return {
            'total_completed': 0,
            'avg_delivery_time_days': 0,
            'on_time_delivery_pct': 0,
            'carrier_breakdown': {}
        }
    
    stats = {}
    stats['total_completed'] = len(history_df)
    
    # Average delivery time (planned -> actual)
    if 'delay_days' in history_df.columns:
        valid_delays = history_df[history_df['delay_days'].notna()]
        stats['avg_delivery_time_days'] = round(float(valid_delays['delay_days'].mean()), 1) if len(valid_delays) > 0 else 0
    else:
        stats['avg_delivery_time_days'] = 0
    
    # On-time delivery percentage
    if 'delay_days' in history_df.columns:
        valid = history_df[history_df['delay_days'].notna()]
        if len(valid) > 0:
            on_time = len(valid[valid['delay_days'] <= 0])
            stats['on_time_delivery_pct'] = round((on_time / len(valid)) * 100, 1)
        else:
            stats['on_time_delivery_pct'] = 0
    else:
        stats['on_time_delivery_pct'] = 0
    
    # Per-carrier breakdown
    carrier_breakdown = {}
    if 'carrier' in history_df.columns:
        for carrier, group in history_df.groupby('carrier'):
            if not carrier or str(carrier).lower() in ['unknown', 'nan', 'none', '']:
                continue
            valid = group[group['delay_days'].notna()] if 'delay_days' in group.columns else group
            carrier_breakdown[str(carrier)] = {
                'count': len(group),
                'avg_delay': round(float(valid['delay_days'].mean()), 1) if len(valid) > 0 else 0,
                'on_time_pct': round((len(valid[valid['delay_days'] <= 0]) / len(valid)) * 100, 1) if len(valid) > 0 else 0
            }
    stats['carrier_breakdown'] = carrier_breakdown
    
    return stats


if __name__ == "__main__":
    # Test the processor
    print("Testing V2 Data Processor...")
    
    try:
        df = load_manual_export('../scanfieldtest.xlsx')
        processed = process_data(df)
        kpis = calculate_kpis(processed)
        
        print("\n[KPI Summary]:")
        for key, value in kpis.items():
            print(f"  {key}: {value}")
        
        print("\n[OK] Data processor test complete!")
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
