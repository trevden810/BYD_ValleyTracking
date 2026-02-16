import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.api import get_token, fetch_jobs, process_data, fetch_jobs_from_excel
from utils.ui import render_sidebar

# Page config
st.set_page_config(page_title="Version 3.0", page_icon="✨", layout="wide")

st.title("✨ Version 3.0")

# Authentication & Data Loading
@st.cache_data(ttl=900)
def load_data():
    # Use local Excel file instead of API
    file_path = "bydhistorical.xlsx"
    if not os.path.exists(file_path):
        st.error(f"Data file not found: {file_path}")
        return pd.DataFrame()
    
    # Use the new excel fetch function
    # We import it inside or ensure it's imported at top
    # But since we can't easily change imports with replace_file_content in one go if they are far apart,
    # we'll assume we can fix imports or just use the function if available.
    # Actually, let's just update the import line first or use full path if needed.
    # Wait, I should check imports first.
    # Let's just do the function body change here, and I'll update imports in a separate call if needed or include it.
    
    # Note: imports were: from utils.api import get_token, fetch_jobs, process_data
    # I need to change that to include fetch_jobs_from_excel
    
    raw_data = fetch_jobs_from_excel(file_path)
    df = process_data(raw_data)
    return df

with st.spinner('Loading job data from Excel...'):
    df_main = load_data()

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


# --- Main Table ---
st.subheader(f"Job List ({len(df_filtered)})")

# Search box
search_term = st.text_input("🔍 Search Job ID, Notes, or Assigned Driver", "")

if search_term:
    search_term = search_term.lower()
    df_filtered = df_filtered[
        df_filtered['Job_ID'].str.lower().str.contains(search_term, na=False) |
        df_filtered['Customer_Notes'].str.lower().str.contains(search_term, na=False) |
        df_filtered['Assigned_Driver'].str.lower().str.contains(search_term, na=False) |
        df_filtered['Product_Name'].str.lower().str.contains(search_term, na=False) |
        df_filtered['Product_Serial'].str.lower().str.contains(search_term, na=False)
    ]

# Display Logic
df_display = df_filtered.copy()

# 1. Map Status to Visual Categories (and Emoji for simple visual)
def get_status_emoji(row):
    status = str(row.get('Status', '')).lower()
    
    # Completed
    if status in ['delivered', 'complete', 'completed']:
        return "🟢 Complete"
    
    # Issues
    if row.get('Delay_Days', 0) > 0 and status not in ['delivered', 'complete', 'completed']:
        return "🔴 Delayed"
    
    # In Progress (Arrived at Dock)
    if pd.notna(row.get('Actual_Date')) and status not in ['delivered', 'complete', 'completed']:
        return "🔵 In Progress"

    # Routed (Driver Assigned but not Arrived/Complete)
    # Check if Assigned_Driver column exists and has a value
    driver = str(row.get('Assigned_Driver', '')).strip()
    if driver and driver.lower() != 'nan' and driver.lower() != 'none' and driver.lower() != '':
         return "🟡 Routed"
        
    # Planned
    if status in ['created', 'manifested', 'planned', 'unknown', '']:
        return "⚪ Scheduled"
        
    return "⚪ Unknown"

df_display['Visual_Status'] = df_display.apply(get_status_emoji, axis=1)

# Format dates for display
if 'Planned_Date' in df_display.columns:
    df_display['Planned_Date'] = df_display['Planned_Date'].dt.strftime('%Y-%m-%d')
if 'Actual_Date' in df_display.columns:
    df_display['Actual_Date'] = df_display['Actual_Date'].dt.strftime('%Y-%m-%d %H:%M')

# Select and Rename Columns for View
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
    'Market': 'Market',
    'Assigned_Driver': 'Driver',
    'Customer_Notes': 'Notes'
}

# Filter for available columns
final_cols = [c for c in display_cols.keys() if c in df_display.columns]
df_view = df_display[final_cols].rename(columns=display_cols)

# Define color styling function for the whole row or specific columns
def style_status(val):
    if "Complete" in val:
        return 'color: green; font-weight: bold'
    elif "Delayed" in val:
        return 'color: red; font-weight: bold'
    elif "In Progress" in val:
        return 'color: blue; font-weight: bold'
    elif "Routed" in val:
        return 'color: #D4AF37; font-weight: bold' # Gold/Dark Yellow
    else:
        return 'color: gray'

st.dataframe(
    df_view.style.map(style_status, subset=['Status']),
    use_container_width=True,
    hide_index=True
)
