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
        filepath: Path to .xlsx file (scanfieldtest.xlsx format)
        
    Returns:
        Raw DataFrame with all 169 columns
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
        
    # Sort by Planned_Date descending so the first one is the "latest"
    # If Planned_Date is missing, maybe sort by creation? But we don't have creation.
    # We'll rely on Planned_Date.
    if 'Planned_Date' in has_serial.columns:
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
