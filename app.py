import streamlit as st

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="BYD/Valley Job Tracker",
    page_icon="🚚",
    layout="wide"
)

st.title("🚚 BYD/Valley Job Tracker")

st.markdown("""
Welcome to the Job Tracking Dashboard. Use the sidebar to navigate between views.
- **🚚 BYD/Valley Job Tracker**: The main dashboard for tracking job status, arrivals, and deliveries.
""")

st.divider()

# --- High Level KPIs (Home Page Version) ---
# We load data here just to show the top-level summary numbers

@st.cache_data(ttl=900)
def load_data():
    token = get_token()
    if not token:
        return pd.DataFrame()
    
    raw_data = fetch_jobs(token)
    df = process_data(raw_data)
    return df

with st.spinner('Loading dashboard summary...'):
    df = load_data()

if not df.empty:
    st.header("At a Glance (Last 90 Days)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # 1. Total Active Jobs
    total_jobs = len(df)
    col1.metric("Total Jobs", total_jobs)
    
    # 2. Action Items (Overdue Arrivals)
    today = datetime.now().date()
    if 'Planned_Date' in df.columns and 'Actual_Date' in df.columns:
        overdue_count = len(df[
            (df['Planned_Date'].dt.date < today) & 
            (df['Actual_Date'].isna())
        ])
    else:
        overdue_count = 0
    
    col2.metric("Overdue Arrivals", overdue_count, delta_color="inverse")
    
    # 3. Ready to Route
    ready_count = 0
    # 3. Ready to Route
    ready_count = 0
    if 'Is_Routed' in df.columns and 'Actual_Date' in df.columns:
        ready_count = len(df[
            (df['Actual_Date'].notna()) & 
            (df['Is_Routed'] == False) 
        ])
    col3.metric("Ready for Routing", ready_count)

    # 4. On-Time %
    on_time_pct = 0
    if 'Delay_Days' in df.columns:
         arrived_jobs = df[df['Actual_Date'].notna()]
         if not arrived_jobs.empty:
             on_time = len(arrived_jobs[arrived_jobs['Delay_Days'] <= 0])
             on_time_pct = (on_time / len(arrived_jobs)) * 100
    
    col4.metric("On-Time Arrival %", f"{on_time_pct:.1f}%")

else:
    st.info("No data available. Please check your connection or filemaker status.")
