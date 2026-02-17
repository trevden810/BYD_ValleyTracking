# Debug: Data File Verification
import pandas as pd
import streamlit as st

st.title("🔍 Data File Debug Info")

try:
    df = pd.read_excel('bydhistorical.xlsx')
    
    st.metric("Total Rows", len(df))
    st.metric("Total Columns", len(df.columns))
    
    new_fields = ['_kf_state_id', 'piece_total', 'white_glove', 'notification_detail', '_kf_miles_oneway_id']
    
    st.subheader("New Fields Check")
    for field in new_fields:
        if field in df.columns:
            st.success(f"✓ {field}")
        else:
            st.error(f"✗ {field} MISSING")
    
    if '_kf_state_id' in df.columns:
        st.subheader("State Distribution")
        st.bar_chart(df['_kf_state_id'].value_counts())
        
except Exception as e:
    st.error(f"Error loading file: {e}")
