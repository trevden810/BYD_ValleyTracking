import streamlit as st
from datetime import datetime, timedelta
import pandas as pd

def render_sidebar(df):
    """
    Renders the shared sidebar filters and returns the filtered dataframe.
    """
    st.sidebar.header("Filters")
    
    # 1. Date Range
    st.sidebar.subheader("📅 Date Range")
    
    if 'Planned_Date' in df.columns:
        # Determine min/max from data, default to today/future if empty
        min_date = df['Planned_Date'].min().date() if not df['Planned_Date'].isnull().all() else datetime.now().date()
        max_date_data = df['Planned_Date'].max().date() if not df['Planned_Date'].isnull().all() else datetime.now().date()
        
        # Allow searching up to 90 days in the future
        max_date = max(max_date_data, (datetime.now() + timedelta(days=90)).date())
        
        # Default to current date (Today)
        # User requested specific default to avoid 30-day lookback
        today = datetime.now().date()
        default_start = today
        default_end = today
        
        # Ensure start is valid (if min_date is in future)
        if default_start < min_date:
            default_start = min_date
        
        if default_start > default_end:
             default_start = default_end
        
        st.sidebar.caption(f"Available: {min_date} to {max_date}")
        
        c1, c2 = st.sidebar.columns(2)
        start_date = c1.date_input("From", value=default_start, min_value=min_date, max_value=max_date)
        end_date = c2.date_input("To", value=default_end, min_value=min_date, max_value=max_date)
        
        if start_date > end_date:
            st.sidebar.error("Start date must be before end date")
            start_date = end_date
            
        date_range = (start_date, end_date)
    else:
        date_range = None

    # 2. State
    state_options = df['State'].unique().tolist() if 'State' in df.columns else []
    selected_states = st.sidebar.multiselect("State", state_options, default=state_options)

    # 3. Status
    status_options = df['Status'].unique().tolist() if 'Status' in df.columns else []
    selected_statuses = st.sidebar.multiselect("Status", status_options, default=status_options)

    # 4. Carrier
    carrier_options = df['Carrier'].unique().tolist() if 'Carrier' in df.columns else []
    selected_carriers = st.sidebar.multiselect("Carrier", carrier_options, default=carrier_options)

    # --- Apply Filters ---
    df_filtered = df.copy()

    # Date
    if 'Planned_Date' in df_filtered.columns and date_range:
        s, e = date_range
        df_filtered = df_filtered[
            (df_filtered['Planned_Date'].dt.date >= s) & 
            (df_filtered['Planned_Date'].dt.date <= e)
        ]

    # State
    if selected_states:
        df_filtered = df_filtered[df_filtered['State'].isin(selected_states)]

    # Status
    if selected_statuses:
        df_filtered = df_filtered[df_filtered['Status'].isin(selected_statuses)]

    # Carrier
    if selected_carriers and 'Carrier' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Carrier'].isin(selected_carriers)]
        
    return df_filtered
