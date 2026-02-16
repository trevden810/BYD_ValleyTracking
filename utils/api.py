import requests
import os
from requests.auth import HTTPBasicAuth
import streamlit as st
from datetime import datetime
import pandas as pd

def get_token():
    """Authenticates with FileMaker and returns a session token."""
    base_url = os.getenv("FILEMAKER_BASE_URL")
    username = os.getenv("FILEMAKER_USERNAME")
    password = os.getenv("FILEMAKER_PASSWORD")
    database = os.getenv("FILEMAKER_JOBS_DB")
    
    if not all([base_url, username, password, database]):
        st.error("Missing FileMaker credentials in .env file (looked for FILEMAKER_BASE_URL, etc).")
        return None

    auth_url = f"{base_url}/fmi/data/vLatest/databases/{database}/sessions"
    
    try:
        response = requests.post(
            auth_url, 
            auth=HTTPBasicAuth(username, password),
            headers={"Content-Type": "application/json"},
            json={}
        )
        response.raise_for_status()
        return response.json()['response']['token']
    except requests.exceptions.RequestException as e:
        st.error(f"Authentication failed: {e}")
        return None

def fetch_jobs(token, query_payload=None, days_back=90, days_forward=90):
    """Fetches job records from FileMaker using the session token."""
    base_url = os.getenv("FILEMAKER_BASE_URL")
    database = os.getenv("FILEMAKER_JOBS_DB")
    layout = os.getenv("FILEMAKER_JOBS_LAYOUT", "Jobs") # Default to "Jobs" if not set

    if not token:
        return []

    data_url = f"{base_url}/fmi/data/vLatest/databases/{database}/layouts/{layout}/_find"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    if query_payload:
        query = query_payload
    else:
        # Calculate date range for jobs (past + future)
        # Default: 90 days back to 90 days forward to capture both historical and planned jobs
        today = datetime.now()
        start_date = today - pd.Timedelta(days=days_back)
        end_date = today + pd.Timedelta(days=days_forward)
        
        # Format dates as MM/DD/YYYY for FileMaker
        start_date_str = start_date.strftime("%m/%d/%Y")
        end_date_str = end_date.strftime("%m/%d/%Y")
        
        # Default query to fetch jobs in date range
        # Filter for BYD and Valley carriers AND date range AND Delivery job type only
        # Carrier field identified as _kf_client_code_id
        # Codes: BYDo, VALLEYc
        # Job type: Delivery (excludes outs, drops, pickups, recoveries, etc.)
        query = {
            "query": [
                {
                    "_kf_client_code_id": "BYDo",
                    "job_date": f"{start_date_str}...{end_date_str}",
                    "job_type": "Delivery"
                },
                {
                    "_kf_client_code_id": "VALLEYc",
                    "job_date": f"{start_date_str}...{end_date_str}",
                    "job_type": "Delivery"
                }
            ],
            "limit": 5000,
            "sort": [
                {
                    "fieldName": "job_date",
                    "sortOrder": "descend"
                }
            ]
        }

    try:
        response = requests.post(data_url, headers=headers, json=query)
        if response.status_code == 401:
             st.error("Authentication failed or token expired.")
             return []
        if response.status_code == 500: 
             return []

        response_json = response.json()
        if 'messages' in response_json and response_json['messages'][0]['code'] != '0':
            # Code 401 in message means no records found
            if response_json['messages'][0]['code'] == '401':
                return []
            print(f"FM Error: {response_json['messages'][0]['message']} (Code: {response_json['messages'][0]['code']})")
            return []

        data = response_json['response']['data']
        return [record['fieldData'] for record in data]
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")
        return []
    except KeyError:
        print(f"Unexpected response format: {response.text}")
        return []

def fetch_jobs_from_excel(file_path):
    """Fetches job records from a local Excel file."""
    if not os.path.exists(file_path):
        st.error(f"Excel file not found: {file_path}")
        return []
        
    try:
        # Read Excel file
        # Convert all to string initially to match API behavior for process_data
        df = pd.read_excel(file_path, dtype=str)
        
        # Replace 'nan' string (from pandas reading empty cells as NaN then converting to str) with empty string
        df = df.replace('nan', '')
        
        # Convert to list of dictionaries
        return df.to_dict('records')
        
    except Exception as e:
        st.error(f"Failed to read Excel file: {e}")
        return []


def process_data(jobs_data):
    """Processes raw job data into a pandas DataFrame."""
    if not jobs_data:
        return pd.DataFrame()

    df = pd.DataFrame(jobs_data)
    
    # CRITICAL: Normalize all data types to prevent PyArrow errors
    # Convert all object columns to proper strings first
    for col in df.columns:
        if df[col].dtype == 'object':
            # Convert to string and handle NaN/None values
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '').replace('<NA>', '')
    
    # Map FM fields to Dashboard columns
    # FM Field -> Dashboard Column
    # job_date -> Planned_Date
    # job_status -> Status
    # job_status -> Status
    # _kp_job_id -> Job_ID (Used to be BOL_Number)
    # order_C1 -> Stop_Number
    # location_load -> Market
    # _kf_client_code_id -> Carrier (Proxy)
    # _kf_lead_id -> Assigned_Driver (Used to be Ops_Manager)
    
    # Date Handling
    if 'job_date' in df.columns:
        df['Planned_Date'] = pd.to_datetime(df['job_date'], errors='coerce')
    else:
        df['Planned_Date'] = pd.NaT

    # Actual Date handling
    # Use row-wise processing for reliability with mixed formats
    def parse_actual_date(row):
        # Check if columns exist
        if 'job_date' not in row or 'time_complete' not in row:
            return pd.NaT
            
        # Get values and clean them
        d = str(row['job_date']).strip().replace('nan', '').replace('None', '').replace('<NA>', '')
        t = str(row['time_complete']).strip().replace('nan', '').replace('None', '').replace('<NA>', '')
        
        # If time is missing, job isn't complete -> Pending
        if not t:
            return pd.NaT
            
        # If date is missing but time exists (unlikely), can't make date
        if not d:
            return pd.NaT
            
        combined = f"{d} {t}"
        try:
            return pd.to_datetime(combined, dayfirst=False)
        except:
            return pd.NaT

    if 'time_complete' in df.columns and 'job_date' in df.columns:
         df['Actual_Date'] = df.apply(parse_actual_date, axis=1)
    else:
        df['Actual_Date'] = pd.NaT

    # Stop Number from order_C1
    if 'order_C1' in df.columns:
        df['Stop_Number'] = df['order_C1'].astype(str).replace('nan', '').replace('None', '')
    else:
        df['Stop_Number'] = ''

    # Job ID (formerly BOL_Number) using _kp_job_id
    if '_kp_job_id' in df.columns:
        df['Job_ID'] = df['_kp_job_id'].astype(str)
    else:
        df['Job_ID'] = ''
    
    # Ensure Job_ID and Stop_Number are string type
    df['Job_ID'] = df['Job_ID'].astype(str)
    df['Stop_Number'] = df['Stop_Number'].astype(str)
    
    # Status
    if 'job_status' in df.columns:
        df['Status'] = df['job_status'].astype(str)
    else:
         df['Status'] = 'Unknown'

    # Market
    if 'location_load' in df.columns:
        df['Market'] = df['location_load'].astype(str)
    else:
        df['Market'] = 'Unknown'

    # Carrier
    if '_kf_client_code_id' in df.columns:
        df['Carrier'] = df['_kf_client_code_id'].astype(str)
    else:
        df['Carrier'] = 'Unknown'
        
    # Calculate Delay Days
    if 'Planned_Date' in df.columns and 'Actual_Date' in df.columns:
        df['Delay_Days'] = (df['Actual_Date'] - df['Planned_Date']).dt.days
    else:
        df['Delay_Days'] = None
    
    # Notes
    if 'notes_driver' in df.columns:
        df['Customer_Notes'] = df['notes_driver'].astype(str).replace('nan', '')
    elif 'notes_call_ahead' in df.columns:
         df['Customer_Notes'] = df['notes_call_ahead'].astype(str).replace('nan', '')
    else:
        df['Customer_Notes'] = ''

    # Ops Manager / Assigned Driver
    # _kf_lead_id is the Driver ID or Name
    if '_kf_lead_id' in df.columns:
        df['Assigned_Driver'] = df['_kf_lead_id'].astype(str).replace('nan', '')
    else:
        df['Assigned_Driver'] = ''
        
    # Is_Routed logic:
    # If Assigned_Driver is populated (not empty/unknown), then it is routed.
    df['Is_Routed'] = df['Assigned_Driver'].apply(lambda x: True if x and x.lower() != 'unknown' else False)
        
    # Confirmation Status / Customer Notified
    if '_kf_notification_id' in df.columns:
        df['Confirmation_Status'] = df['_kf_notification_id'].astype(str).replace('nan', '')
    else:
        df['Confirmation_Status'] = 'Unknown'
    
    # Product Information (NEW)
    if 'description_product' in df.columns:
        df['Product_Name'] = df['description_product'].astype(str).replace('nan', '')
    else:
        df['Product_Name'] = ''
    
    if 'product_serial_number' in df.columns:
        df['Product_Serial'] = df['product_serial_number'].astype(str).replace('nan', '')
    else:
        df['Product_Serial'] = ''
    
    # Scan Validation (NEW) - Parse JSON
    def parse_scan_json(json_str):
        """Extract scan info from JSON: last user, last timestamp, total scans"""
        import json
        
        # Default values
        result = {
            'Last_Scan_User': '',
            'Last_Scan_Time': '',
            'Total_Scans': 0
        }
        
        # Clean and validate
        json_str = str(json_str).strip()
        if not json_str or json_str in ['nan', 'None', '']:
            return result
        
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                return result
            
            # Count total scans
            result['Total_Scans'] = len(data)
            
            # Get most recent scan (last entry)
            if data:
                # Get last key (serial number)
                last_serial = list(data.keys())[-1]
                last_scan = data[last_serial]
                
                if isinstance(last_scan, dict):
                    result['Last_Scan_User'] = last_scan.get('username', '')
                    result['Last_Scan_Time'] = last_scan.get('timestamp', '')
        except:
            # Silently fail - don't break pipeline for bad JSON
            pass
        
        return result
    
    if 'box_serial_numbers_scanned_received_json' in df.columns:
        # Parse JSON and expand to columns
        scan_data = df['box_serial_numbers_scanned_received_json'].apply(parse_scan_json)
        df['Last_Scan_User'] = scan_data.apply(lambda x: x['Last_Scan_User'])
        df['Last_Scan_Time'] = scan_data.apply(lambda x: x['Last_Scan_Time'])
        df['Total_Scans'] = scan_data.apply(lambda x: x['Total_Scans'])
    else:
        df['Last_Scan_User'] = ''
        df['Last_Scan_Time'] = ''
        df['Total_Scans'] = 0

    return df
