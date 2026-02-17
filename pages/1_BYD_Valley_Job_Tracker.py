import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.api import get_token, fetch_jobs, process_data, fetch_jobs_from_excel
from utils.ui import render_sidebar
from v2.supabase_client import SupabaseClient # Import Supabase Client

# Page config
st.set_page_config(page_title="BYD/Valley Job Tracker", page_icon="🚚", layout="wide")

st.title("🚚 BYD/Valley Job Tracker")

# Sidebar - Snapshot Selection
st.sidebar.header("📅 Data Source")
today = datetime.now().date()
selected_date = st.sidebar.date_input("Snapshot Date", value=today, max_value=today)

# Authentication & Data Loading
@st.cache_data(ttl=900)
def load_data(target_date):
    # If today, use local file (Real-time/Latest)
    if target_date == datetime.now().date():
        file_path = "bydhistorical.xlsx"
        if not os.path.exists(file_path):
            st.error(f"Data file not found: {file_path}")
            return pd.DataFrame()
        
        raw_data = fetch_jobs_from_excel(file_path)
        df = process_data(raw_data)
        return df
    
    # If past date, fetch from Supabase
    else:
        try:
            client = SupabaseClient()
            df = client.get_snapshot_by_date(target_date)
            if df is None or df.empty:
               return pd.DataFrame()
            return df
        except Exception as e:
            st.error(f"Error fetching historical data: {e}")
            return pd.DataFrame()

if selected_date == today:
    spinner_msg = 'Loading latest job data...'
else:
    spinner_msg = f'Loading historical data for {selected_date}...'

with st.spinner(spinner_msg):
    df_main = load_data(selected_date)

if df_main.empty:
    st.warning("No data found.")
    st.stop()

# Render Sidebar filters and get filtered dataframe
df_filtered = render_sidebar(df_main)

# --- Action Required Sections ---

# 1. Overdue Arrivals (Watchlist)
today = datetime.now().date()
pending_arrivals = df_filtered[
    (df_filtered['Planned_Date'].dt.date < today) & 
    (df_filtered['Actual_Date'].isna())
]

# 2. Ready for Routing
ready_for_routing = df_filtered[
    (df_filtered['Actual_Date'].notna()) & 
    (df_filtered['Is_Routed'] == False) 
]

# Display Overdue Arrivals if any
if not pending_arrivals.empty:
    st.subheader("⚠️ Overdue Arrivals (Watchlist)")
    st.caption("Jobs that were planned to arrive but have not yet been scanned at the dock.")
    
    watchlist_df = pending_arrivals.copy()
    watchlist_df['Days_Overdue'] = (pd.to_datetime(today) - watchlist_df['Planned_Date'].dt.normalize()).dt.days
    
    wl_cols = ['Job_ID', 'Stop_Number', 'Carrier', 'Planned_Date', 'Days_Overdue', 'Market']
    wl_cols = [c for c in wl_cols if c in watchlist_df.columns]
    
    st.dataframe(
        watchlist_df[wl_cols].sort_values('Days_Overdue', ascending=False).style.format({'Planned_Date': '{:%Y-%m-%d}'}),
        use_container_width=True,
        hide_index=True
    )
    st.divider()

# Display Ready for Routing if any
if not ready_for_routing.empty:
    st.subheader("🚛 Ready for Routing")
    st.caption("Jobs that have arrived at the dock but have not been assigned to a driver/Ops Manager.")
    
    routing_cols = ['Job_ID', 'Stop_Number', 'Carrier', 'Actual_Date', 'Market', 'Status']
    routing_cols = [c for c in routing_cols if c in ready_for_routing.columns]
    
    st.dataframe(
        ready_for_routing[routing_cols].sort_values('Actual_Date', ascending=False).style.format({'Actual_Date': '{:%Y-%m-%d %H:%M}'}),
        use_container_width=True,
        hide_index=True
    )
    st.divider()


# --- Tabs for Views ---
tab1, tab2 = st.tabs(["📋 Job List", "📊 Insights"])

with tab1:
    # --- Main Table ---
    st.subheader(f"Job List ({len(df_filtered)})")
    
    # Search box
    col_search, col_filter = st.columns([3, 1])
    with col_search:
        search_term = st.text_input("🔍 Search Job ID, Notes, or Assigned Driver", "")
    with col_filter:
        show_unscanned = st.checkbox("⚠️ Unscanned Only")
    
    # Filter Logic (Applying Search & Checkbox)
    if search_term:
        search_term = search_term.lower()
        mask = (
            df_filtered['Job_ID'].str.lower().str.contains(search_term, na=False) |
            df_filtered['Customer_Notes'].str.lower().str.contains(search_term, na=False) |
            df_filtered['Assigned_Driver'].str.lower().str.contains(search_term, na=False) |
            df_filtered['Product_Name'].str.lower().str.contains(search_term, na=False) |
            df_filtered['Product_Serial'].str.lower().str.contains(search_term, na=False)
        )
        df_filtered = df_filtered[mask]
        
    # Apply "Unscanned Routed" Filter
    # Definition: Routed (Driver Assigned) AND Scan_Count == 0 (or Last_Scan_User empty)
    # We must ensure 'Last_Scan_User' exists.
    if show_unscanned:
        if 'Last_Scan_User' in df_filtered.columns and 'Assigned_Driver' in df_filtered.columns:
            # Check for routed
            is_routed = df_filtered['Assigned_Driver'].str.strip().str.lower().replace('nan', '') != ''
            # Check for NO scan
            no_scan = (df_filtered['Last_Scan_User'].isna()) | (df_filtered['Last_Scan_User'] == '')
            
            df_filtered = df_filtered[is_routed & no_scan]
        else:
            st.warning("Cannot filter: Missing scan/driver columns.")

    # Display Logic
    df_display = df_filtered.copy()

    # 1. Map Status to Visual Categories (and Emoji for simple visual)
    def get_status_emoji(row):
        status = str(row.get('Status', '')).lower()
        driver = str(row.get('Assigned_Driver', '')).strip()
        last_scan = str(row.get('Last_Scan_User', '')).strip()
        is_routed = driver and driver.lower() != 'nan' and driver.lower() != 'none' and driver.lower() != ''
        has_scan = last_scan and last_scan.lower() != 'nan' and last_scan.lower() != ''
        
        # Critical Flag: Routed but NOT Scanned
        if is_routed and not has_scan and status not in ['delivered', 'complete', 'completed']:
            return "⚠️ Unscanned"
            
        # Completed
        if status in ['delivered', 'complete', 'completed']: return "🟢 Complete"
        
        # Issues
        if row.get('Delay_Days', 0) > 0 and status not in ['delivered', 'complete', 'completed']: return "🔴 Delayed"
        
        # In Progress (Arrived at Dock)
        if pd.notna(row.get('Actual_Date')) and status not in ['delivered', 'complete', 'completed']: return "🔵 In Progress"

        # Routed (Driver Assigned but not Arrived/Complete)
        if is_routed: return "🟡 Routed"
            
        # Planned
        if status in ['created', 'manifested', 'planned', 'unknown', '']: return "⚪ Scheduled"
            
        return "⚪ Unknown"

    df_display['Visual_Status'] = df_display.apply(get_status_emoji, axis=1)

    # Format dates
    if 'Planned_Date' in df_display.columns:
        df_display['Planned_Date'] = df_display['Planned_Date'].dt.strftime('%Y-%m-%d')
    if 'Actual_Date' in df_display.columns:
        df_display['Actual_Date'] = df_display['Actual_Date'].dt.strftime('%Y-%m-%d %H:%M')

    # Column Mapping
    display_cols = {
        'Visual_Status': 'Status',
        'Job_ID': 'Job ID',
        'Stop_Number': 'Stop #',
        'Product_Name': 'Product',
        'Product_Serial': 'Serial #',
        'Carrier': 'Carrier',
        'Planned_Date': 'Planned',
        'Actual_Date': 'Actual Arrival',
        'Delay_Days': 'Delay (Days)',
        'Last_Scan_User': 'Scanned By',
        'State': 'State',
        'Assigned_Driver': 'Driver',
        'Customer_Notes': 'Notes'
    }
    
    final_cols = [c for c in display_cols.keys() if c in df_display.columns]
    df_view = df_display[final_cols].rename(columns=display_cols)

    def style_status(val):
        if "Complete" in val: return 'color: green; font-weight: bold'
        elif "Delayed" in val: return 'color: red; font-weight: bold'
        elif "In Progress" in val: return 'color: blue; font-weight: bold'
        elif "Routed" in val: return 'color: #D4AF37; font-weight: bold'
        else: return 'color: gray'

    st.dataframe(
        df_view.style.map(style_status, subset=['Status']),
        use_container_width=True,
        hide_index=True
    )

with tab2:
    st.header("📊 Operational Insights")
    
    # --- TOP LEVEL METRICS ---
    # Calculate Routed but Unscanned
    # Definition: Assigned Driver + No Scan User + Not Complete
    if 'Assigned_Driver' in df_filtered.columns and 'Last_Scan_User' in df_filtered.columns:
        routed = df_filtered['Assigned_Driver'].str.strip().str.lower().replace('nan', '') != ''
        not_scanned = (df_filtered['Last_Scan_User'].isna()) | (df_filtered['Last_Scan_User'] == '')
        not_complete = ~df_filtered['Status'].str.lower().isin(['delivered', 'complete', 'completed'])
        
        unscanned_count = len(df_filtered[routed & not_scanned & not_complete])
    else:
        unscanned_count = 0
        
    m1, m2, m3 = st.columns(3)
    m1.metric("⚠️ Routed / Not Scanned", f"{unscanned_count}", help="Jobs assigned to a driver but missing a scan record.")
    
    col1, col2 = st.columns(2)
    
    # --- 1. BOTTLENECK ANALYSIS ---
    with col1:
        st.subheader("⏳ Bottleneck Detector")
        st.caption("How long jobs sit in 'Ready for Routing' (Arrived but not Assigned)")
        
        # Filter for Arrived but NOT Routed and NOT Complete
        # Assuming 'Is_Routed' exists and Actual_Date is present
        bottleneck_df = df_filtered[
            (df_filtered['Actual_Date'].notna()) & 
            (df_filtered['Is_Routed'] == False) &
            (~df_filtered['Status'].str.lower().isin(['delivered', 'complete', 'completed']))
        ].copy()
        
        if not bottleneck_df.empty:
            # Calculate days waiting
            # Use snapshot date (selected_date) or today for calculation
            ref_date = pd.to_datetime(selected_date)
            bottleneck_df['Days_Waiting'] = (ref_date - bottleneck_df['Actual_Date'].dt.normalize()).dt.days
            
            # Simple Bar Chart of Counts by Waiting Days
            wait_counts = bottleneck_df['Days_Waiting'].value_counts().sort_index()
            st.bar_chart(wait_counts)
            
            avg_wait = bottleneck_df['Days_Waiting'].mean()
            st.metric("Avg Wait Time (Days)", f"{avg_wait:.1f} Days")
        else:
            st.info("No active bottlenecks found! (All arrived jobs are routed or completed)")

    # --- 2. SCANNER LEADERBOARD ---
    with col2:
        st.subheader("🏆 Scanning Leaderboard")
        st.caption("Top active scanners in current view")
        
        if 'Last_Scan_User' in df_filtered.columns:
            # Filter empty users
            scans = df_filtered[df_filtered['Last_Scan_User'] != '']['Last_Scan_User'].value_counts()
            
            if not scans.empty:
                st.dataframe(scans, use_container_width=True)
            else:
                st.info("No scan data found in current selection.")
        else:
            st.warning("Last_Scan_User column missing.")

    st.divider()

    # --- 3. HISTORICAL TIMELINE ---
    st.subheader("📈 Scan Compliance History (Last 30 Days)")
    
    # Fetch historical data explicitly for this chart
    try:
        # We need a client instance. If load_data used it, we can create one here safely.
        # But we need to handle if secrets are missing (local vs cloud).
        # We wrap in try block.
        api_client = SupabaseClient()
        history_df = api_client.get_historical_kpis(days=30)
        
        if history_df is not None and not history_df.empty:
            # Create a simple line chart
            # We want to show 'avg_scans_per_job' (Engagement) and 'total_jobs' (Volume)
            
            chart_data = history_df[['report_date', 'avg_scans_per_job', 'total_jobs']].set_index('report_date')
            
            # Use Altair or Streamlit native chart
            # Streamlit native line_chart is easiest for quick win
            st.line_chart(chart_data['avg_scans_per_job'])
            st.caption("Average Scans Per Job Trend")
            
        else:
            st.info("Not enough historical data collected yet (Started tracking today).")
            
    except Exception as e:
        st.warning(f"Could not load history: {e}")
