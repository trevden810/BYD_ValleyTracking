import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.api import get_token, fetch_jobs, process_data, fetch_jobs_from_excel
from v2.supabase_client import SupabaseClient 

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="BYD/Valley Job Tracker", page_icon="🚚", layout="wide")

def load_css(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    else:
        # Fallback or simple warning (optional)
        pass

load_css("assets/style.css")

# --- HEADER (Tier 0) ---
col_header_1, col_header_2 = st.columns([2, 3])
with col_header_1:
     st.markdown("""
        <div style="display: flex; align-items: center; gap: 12px; padding: 8px 0;">
            <div style="background-color: #3B82F6; width: 36px; height: 36px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: bold; color: white; box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.4);">V</div>
            <div>
                <h3 style="margin: 0; color: #E2E8F0; font-size: 1.1rem; line-height: 1.2;">Inbound Logistics</h3>
                <div style="color: #94A3B8; font-size: 0.8rem; font-weight: 400;">Dock Operations Dashboard</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

# --- DATA LOADING ---
today = datetime.now().date()

@st.cache_data(ttl=900)
def load_data(target_date):
    # Logic to load data (similar to original)
    if target_date == datetime.now().date():
        file_path = "bydhistorical.xlsx"
        if not os.path.exists(file_path):
            return pd.DataFrame()
        try:
            raw_data = fetch_jobs_from_excel(file_path)
            df = process_data(raw_data)
            return df
        except Exception as e:
            st.error(f"Error reading local file: {e}")
            return pd.DataFrame()
    else:
        try:
            client = SupabaseClient()
            df = client.get_snapshot_by_date(target_date)
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

# Default to today
selected_date = today 
df_main = load_data(selected_date)

if df_main.empty:
    st.warning("Data source not found (bydhistorical.xlsx) or empty.")
    st.stop()

# --- TIER 4: FILTER UX (Global Filters) ---
# Replacing Sidebar with Top Filter Bar
with st.container():
    # We use a container with a custom class/style if needed, but standard columns work well
    f_col1, f_col2, f_col3, f_col4, f_col5 = st.columns([2, 1.5, 1.5, 1, 1])
    
    with f_col1:
        search_term = st.text_input("🔍 Search", placeholder="Job ID, BOL, or Product...", label_visibility="collapsed")
    
    with f_col2:
        # Carrier
        carriers = ["All Carriers"] + sorted(df_main['Carrier'].dropna().unique().tolist()) if 'Carrier' in df_main.columns else ["All Carriers"]
        selected_carrier = st.selectbox("Carrier", carriers, label_visibility="collapsed")
        
    with f_col3:
        # State
        states = ["All States"] + sorted(df_main['State'].dropna().unique().tolist()) if 'State' in df_main.columns else ["All States"]
        selected_state = st.selectbox("State", states, label_visibility="collapsed")
        
    with f_col4:
        show_white_glove = st.checkbox("White Glove", value=False)
        
    with f_col5:
        # "Unscanned" Toggle -> "Action Req"
        show_unscanned_only = st.checkbox("Action Req", value=False)

st.markdown("<div style='margin-bottom: 16px; border-bottom: 1px solid rgba(255,255,255,0.06);'></div>", unsafe_allow_html=True)

# --- FILTER LOGIC ---
df_filtered = df_main.copy()

# 1. Search
if search_term:
    s = search_term.lower()
    mask = (
        df_filtered['Job_ID'].astype(str).str.lower().str.contains(s, na=False) |
        df_filtered['Product_Name'].astype(str).str.lower().str.contains(s, na=False) |
        df_filtered['Customer_Notes'].astype(str).str.lower().str.contains(s, na=False)
    )
    df_filtered = df_filtered[mask]

# 2. Dropdowns
if selected_carrier != "All Carriers":
    df_filtered = df_filtered[df_filtered['Carrier'] == selected_carrier]

if selected_state != "All States":
    df_filtered = df_filtered[df_filtered['State'] == selected_state]

# 3. Checkboxes
if show_white_glove and 'White_Glove' in df_filtered.columns:
    df_filtered = df_filtered[df_filtered['White_Glove'] == True]

# Unscanned / Action Req Logic
# Definition: Routed (Driver Assigned) AND Scan_Count == 0 (or Last_Scan_User empty)
# Also includes "SLA Risk" (Arrival > Planned Date)
if 'Last_Scan_User' in df_filtered.columns:
    has_scan = (df_filtered['Last_Scan_User'].notna()) & (df_filtered['Last_Scan_User'] != '')
    
    # Calculate counters for header
    scanned_count = len(df_filtered[has_scan])
    total_count = len(df_filtered)
    
    if show_unscanned_only:
        # "Action Req" View: Show Unscanned OR Late
        # But commonly just "Not Scanned"
        df_filtered = df_filtered[~has_scan]

# --- TIER 1: DOCK INTAKE HEALTH BAR ---
# KPIs
total_arrivals = len(df_main) # Baseline from full dataset (or filtered? usually filtered view context)
# Let's use filtered context for "Page View" metrics, but maybe global for "Dock Health"
# Prompt implies "Dock Intake Health Bar" is top level. Let's use df_main for "Health" context usually, 
# but users might confuse if filters don't affect it. Let's stick to df_filtered for consistency with UI actions, 
# OR provide global context if it's "Dock Health". Let's use df_filtered.

current_view_count = len(df_filtered)

# Calculate Health Metrics
# 1. Scanned vs Unscanned
if 'Last_Scan_User' in df_filtered.columns:
    scanned_mask = (df_filtered['Last_Scan_User'].notna()) & (df_filtered['Last_Scan_User'] != '')
    scanned_n = len(df_filtered[scanned_mask])
    unscanned_n = current_view_count - scanned_n
else:
    scanned_n = 0
    unscanned_n = current_view_count

# 2. SLA At Risk 
# (Planned Date < Today AND Not Scanned)
sla_risk_n = 0
if 'Planned_Date' in df_filtered.columns:
    # Ensure datetime
    safe_planned = pd.to_datetime(df_filtered['Planned_Date'], errors='coerce')
    # Risk: Planned < Today (Overdue) AND Not Scanned
    # Or just "Late to Dock Scan"
    
    # "Late to Dock Scan": Arrived? Or just Planned?
    # Usually "SLA At Risk" means we are past planned date.
    overdue_mask = (safe_planned.dt.date < today) & (~scanned_mask if 'Last_Scan_User' in df_filtered.columns else True)
    sla_risk_n = len(df_filtered[overdue_mask])

# 3. White Glove Pending
wg_pending_n = 0
if 'White_Glove' in df_filtered.columns:
    wg_mask = (df_filtered['White_Glove'] == True) & (~scanned_mask if 'Last_Scan_User' in df_filtered.columns else True)
    wg_pending_n = len(df_filtered[wg_mask])


# Display Metrics with Custom HTML/CSS Cards for better visual
# Streamlit metric is okay, but we want the "colored border" look.
# We will use st.metric for simplicity and reliability but style them with CSS we added.

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total In View", f"{current_view_count}", "Jobs")
m2.metric("Scanned", f"{scanned_n}", f"{(scanned_n/current_view_count*100):.0f}%" if current_view_count else "0%")
m3.metric("Unscanned", f"{unscanned_n}", "Action Req", delta_color="inverse")
m4.metric("SLA Risk", f"{sla_risk_n}", "Critical", delta_color="inverse")
m5.metric("White Glove", f"{wg_pending_n}", "Pending", delta_color="normal")

st.markdown("<br>", unsafe_allow_html=True)

# --- TIER 2: EXCEPTION-FIRST WATCHLIST ---
# "Priority-sorted exception panel"
# Logic: Show items that are SLA At Risk OR White Glove Pending
# Sort by: Late + White Glove first

if sla_risk_n > 0 or wg_pending_n > 0:
    st.markdown("### ⚠️ Exception Watchlist")
    
    # Filter for watchlist
    # (Same masks as above)
    watchlist_mask = overdue_mask | wg_mask
    df_watchlist = df_filtered[watchlist_mask].copy()
    
    if not df_watchlist.empty:
        # Enrich for display
        df_watchlist['Days_Overdue'] = (pd.to_datetime(today) - pd.to_datetime(df_watchlist['Planned_Date'], errors='coerce').dt.normalize()).dt.days
        df_watchlist['Days_Overdue'] = df_watchlist['Days_Overdue'].fillna(0).astype(int)
        
        # Sort: Descending Days_Overdue, then White Glove
        df_watchlist.sort_values(by=['Days_Overdue', 'White_Glove'], ascending=[False, False], inplace=True)
        
        # Columns
        # Job ID, Product, Carrier, Planned, Days Late, White Glove
        wl_cols = {
            'Job_ID': 'Job ID',
            'Product_Name': 'Product',
            'Carrier': 'Carrier',
            'Planned_Date': 'Planned',
            'Days_Overdue': 'Days Late',
            'White_Glove': 'White Glove',
            'State': 'State'
        }
        
        # Filter columns that exist
        final_wl_cols = {k: v for k, v in wl_cols.items() if k in df_watchlist.columns}
        
        df_wl_display = df_watchlist[list(final_wl_cols.keys())].rename(columns=final_wl_cols)
        
        # Custom Styler
        def highlight_watchlist(row):
            styles = []
            # Highlight White Glove
            if 'White Glove' in row and row['White Glove']:
                styles.append('background-color: rgba(59, 130, 246, 0.1); color: #60A5FA;')
            # Highlight Late
            elif 'Days Late' in row and row['Days Late'] > 0:
                styles.append('background-color: rgba(239, 68, 68, 0.1); color: #F87171;')
            else:
                styles.append('')
            return styles
        
        st.dataframe(
            df_wl_display.style.format({'Planned': '{:%Y-%m-%d}'}), 
            use_container_width=True,
            hide_index=True
        )
    st.divider()


# --- TIER 3: INTAKE READINESS BOARD ---
# "Intake Readiness Board" -> Kanban logic
# Buckets:
# 1. Not Scanned (Arrived but no Scan)
# 2. Scanned / In Progress (Scanned, but not Completed/Delivered)
# 3. Completed (Delivered)

st.subheader("📋 Intake & Routing Board")

if 'Actual_Date' in df_filtered.columns and 'Last_Scan_User' in df_filtered.columns:
    # 1. Arrived (Actual_Date not NaT)
    # 2. Scanned (Last_Scan_User not Empty)
    
    arrived_mask = df_filtered['Actual_Date'].notna()
    scanned_mask = (df_filtered['Last_Scan_User'].notna()) & (df_filtered['Last_Scan_User'] != '')
    
    # Bucket 1: Arrived but NOT Scanned (Needs Intake)
    # Also include Planned < Today (Late Arrival/Scan)
    # Simplification: Just "Pending Scan" (Arrived but not scanned)
    bucket_intake = df_filtered[arrived_mask & ~scanned_mask]
    
    # Bucket 2: Scanned (In Progress)
    # Filter out "Delivered" or "Completed" status if we have a status column
    if 'Status' in df_filtered.columns:
        completed_mask = df_filtered['Status'].str.lower().isin(['delivered', 'complete', 'completed'])
    else:
        completed_mask = False
        
    bucket_progress = df_filtered[scanned_mask & ~completed_mask]
    
    # Layout
    b_col1, b_col2 = st.columns(2)
    
    with b_col1:
        st.markdown(f"**📥 Ready for Scan ({len(bucket_intake)})**")
        st.caption("Arrived at dock. Action required.")
        if not bucket_intake.empty:
            cols_i = ['Job_ID', 'Carrier', 'Product_Name', 'Actual_Date']
            # Limit to top 20 for board view to avoid lag
            st.dataframe(bucket_intake[cols_i].head(50), use_container_width=True, hide_index=True)
            
    with b_col2:
        st.markdown(f"**🚛 Routed / In Progress ({len(bucket_progress)})**")
        st.caption("Scanned. Awaiting delivery.")
        if not bucket_progress.empty:
            cols_p = ['Job_ID', 'Assigned_Driver', 'Last_Scan_User', 'Status']
            st.dataframe(bucket_progress[cols_p].head(50), use_container_width=True, hide_index=True)


st.divider()

# --- MAIN LIST / SEARCH TABLE ---
with st.expander("📝 Full Job List"):
    # Show all columns (mapped)
    # Visual Status mapping
    
    df_list = df_filtered.copy()
    
    # Helper for status emoji
    def get_status_emoji(row):
        s = str(row.get('Status', '')).lower()
        if s in ['delivered', 'complete', 'completed']: return "🟢 Complete"
        if s in ['manifested', 'created']: return "⚪ Scheduled"
        
        # Logic for derived status
        driver = str(row.get('Assigned_Driver', '')).strip()
        scan = str(row.get('Last_Scan_User', '')).strip()
        
        if driver and driver != 'nan' and driver != 'None':
            # Routed
            if not scan or scan == 'nan':
                 return "⚠️ Routed (No Scan)"
            return "🟡 Routed"
            
        if scan and scan != 'nan':
            return "🔵 Scanned"
            
        return "⚪ Planned"

    df_list['Visual_Status'] = df_list.apply(get_status_emoji, axis=1)
    
    # Select cols
    target_cols = {
        'Visual_Status': 'Status',
        'Job_ID': 'Job ID',
        'Carrier': 'Carrier',
        'Product_Name': 'Product',
        'Planned_Date': 'Planned',
        'Actual_Date': 'Actual Arrival',
        'Assigned_Driver': 'Driver',
        'Last_Scan_User': 'Scanner'
    }
    
    final_list_cols = {k: v for k, v in target_cols.items() if k in df_list.columns}
    df_list_view = df_list[list(final_list_cols.keys())].rename(columns=final_list_cols)
    
    st.dataframe(df_list_view, use_container_width=True, hide_index=True)
