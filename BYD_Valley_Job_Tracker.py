import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from utils.api import process_data, fetch_jobs_from_excel
from v2.supabase_client import SupabaseClient
from v2.job_chains import get_chain_alerts, JobChainManager

load_dotenv()

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PEPMOVE | Dock Operations",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Inline CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Base */
[data-testid="stAppViewContainer"] { background: #13161C; }
[data-testid="stHeader"] { background: transparent; }

/* KPI Cards */
.kpi-card {
    background: #1C2030;
    border: 1px solid #2A2F3E;
    border-radius: 10px;
    padding: 18px 20px;
    text-align: center;
}
.kpi-label { font-size: 0.72rem; color: #808285; text-transform: uppercase; letter-spacing: .1em; font-weight: 600; margin-bottom: 6px; }
.kpi-value { font-size: 2.2rem; font-weight: 800; line-height: 1; }
.kpi-sub   { font-size: 0.72rem; color: #60657A; margin-top: 4px; }
.kpi-green  { color: #8DC63F; }
.kpi-red    { color: #E05A5A; }
.kpi-amber  { color: #F5A623; }
.kpi-blue   { color: #4A9EFF; }
.kpi-white  { color: #F0F2F5; }

/* Bucket headers */
.bucket-header {
    border-radius: 8px;
    padding: 10px 16px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.bucket-red   { background: rgba(224,90,90,.15);  border-left: 3px solid #E05A5A; }
.bucket-amber { background: rgba(245,166,35,.12); border-left: 3px solid #F5A623; }
.bucket-green { background: rgba(141,198,63,.12); border-left: 3px solid #8DC63F; }
.bucket-blue  { background: rgba(74,158,255,.12); border-left: 3px solid #4A9EFF; }

.bucket-title { font-size: 0.85rem; font-weight: 700; color: #F0F2F5; }
.bucket-count { font-size: 1.1rem; font-weight: 800; margin-left: auto; }
.bucket-desc  { font-size: 0.68rem; color: #808285; margin-top: 2px; }

/* Divider */
.green-divider { border-bottom: 2px solid #8DC63F; margin: 12px 0 22px 0; }

/* Alert row */
.alert-row {
    background: rgba(224,90,90,.1);
    border: 1px solid rgba(224,90,90,.3);
    border-radius: 6px;
    padding: 8px 14px;
    margin-bottom: 6px;
    font-size: 0.8rem;
    color: #F0F2F5;
}
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
logo_path = "assets/Banner Size.png"
col_logo, col_title = st.columns([1, 4])
with col_logo:
    if os.path.exists(logo_path):
        st.image(logo_path, width=220)
with col_title:
    st.markdown("""
        <div style="padding: 10px 0 0 8px;">
            <div style="font-size: 0.72rem; color: #808285; text-transform: uppercase;
                        letter-spacing: 0.1em; font-weight: 600; margin-bottom: 4px;">
                Precision Equipment Placement
            </div>
            <div style="font-size: 1.4rem; color: #F0F2F5; font-weight: 700;
                        letter-spacing: -0.01em; line-height: 1.2;">
                Dock Operations Dashboard
            </div>
            <div style="font-size: 0.8rem; color: #8DC63F; margin-top: 4px; font-weight: 500;">
                BYD &amp; Valley Tracking Board
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<div class='green-divider'></div>", unsafe_allow_html=True)


# ── Data Loading ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=900)
def load_data():
    """
    Load from Supabase (latest snapshot date).
    Falls back to local Excel if Supabase is unavailable.
    Data is already clean — completed jobs are filtered at import time.
    """
    # 1. Try Supabase — fetch ALL active jobs (table is kept clean by upsert)
    try:
        client = SupabaseClient()

        # Fetch all rows with pagination
        all_records = []
        offset = 0
        batch_size = 1000
        while True:
            res = client.client.table('job_snapshots') \
                .select('*') \
                .range(offset, offset + batch_size - 1) \
                .execute()
            if not res.data:
                break
            all_records.extend(res.data)
            if len(res.data) < batch_size:
                break
            offset += batch_size

        if all_records:
            df = pd.DataFrame(all_records)

            # Map DB columns to app columns
            column_map = {
                'job_id': 'Job_ID',
                'planned_date': 'Planned_Date',
                'actual_date': 'Actual_Date',
                'delay_days': 'Delay_Days',
                'status': 'Status',
                'carrier': 'Carrier',
                'state': 'State',
                'scan_user': 'Last_Scan_User',
                'scan_timestamp': 'Scan_Timestamp',
                'product_description': 'Product_Name',
                'piece_count': 'Piece_Count',
                'white_glove': 'White_Glove',
                'notification_detail': 'Notification_Detail',
                'miles_oneway': 'Miles_OneWay'
            }
            df = df.rename(columns=column_map)

            # Parse date columns
            for col in ['Planned_Date', 'Actual_Date', 'Scan_Timestamp']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Ensure expected columns exist
            for col in ['Stop_Number', 'Product_Serial', 'Assigned_Driver', 'Customer_Notes', 'Is_Routed']:
                if col not in df.columns:
                    df[col] = '' if col != 'Is_Routed' else False

            # Use latest snapshot_date as the data date
            if 'snapshot_date' in df.columns:
                data_date = pd.to_datetime(df['snapshot_date'], format='ISO8601').max().date()
            else:
                data_date = datetime.now().date()

            return df, data_date

    except Exception as e:
        print(f"[WARN] Supabase unavailable: {e}")

    # 2. Fallback: local Excel
    file_path = "bydhistorical.xlsx"
    if os.path.exists(file_path):
        try:
            raw = fetch_jobs_from_excel(file_path)
            df = process_data(raw)
            # Apply safety filter even on local fallback
            if 'Status' in df.columns:
                mask = df['Status'].astype(str).str.lower().str.strip().str.contains(
                    'complete|deliver', na=False
                )
                df = df[~mask]
            return df, datetime.now().date()
        except Exception as e:
            print(f"[ERROR] Local fallback failed: {e}")

    return pd.DataFrame(), None


df_raw, data_date = load_data()

if df_raw.empty:
    st.warning("⚠️ No data found. Run the daily import first.")
    st.stop()

# ── Global Filters ─────────────────────────────────────────────────────────────
with st.expander("🔍 Filters", expanded=True):
    fc1, fc2, fc3, fc4, fc5, fc6 = st.columns([2.5, 1.5, 1.5, 2, 1, 1])

    with fc1:
        search = st.text_input("Search", placeholder="🔍  Job ID, Product, or Notes...",
                               label_visibility="collapsed")
    with fc2:
        carriers = (["All Carriers"] + sorted(df_raw['Carrier'].dropna().unique().tolist())
                    if 'Carrier' in df_raw.columns else ["All Carriers"])
        sel_carrier = st.selectbox("Carrier", carriers, label_visibility="collapsed")
    with fc3:
        states = (["All States"] + sorted(df_raw['State'].dropna().unique().tolist())
                  if 'State' in df_raw.columns else ["All States"])
        sel_state = st.selectbox("State", states, label_visibility="collapsed")
    with fc4:
        today_date     = datetime.now().date()
        future_limit   = today_date + pd.Timedelta(days=60)
        date_range = st.date_input("Date Range", value=(today_date, future_limit),
                                   format="MM/DD/YYYY", label_visibility="collapsed")
    with fc5:
        show_wg = st.checkbox("White Glove", value=False)
    with fc6:
        show_action = st.checkbox("Action Req", value=False)


# ── Apply Filters ──────────────────────────────────────────────────────────────
df = df_raw.copy()

if len(date_range) == 2 and 'Planned_Date' in df.columns:
    s, e = date_range
    mask = (pd.to_datetime(df['Planned_Date'], errors='coerce').dt.date >= s) & \
           (pd.to_datetime(df['Planned_Date'], errors='coerce').dt.date <= e)
    df = df[mask]

if sel_carrier != "All Carriers" and 'Carrier' in df.columns:
    df = df[df['Carrier'] == sel_carrier]

if sel_state != "All States" and 'State' in df.columns:
    df = df[df['State'] == sel_state]

if show_wg and 'White_Glove' in df.columns:
    df = df[df['White_Glove'] == True]

if search:
    s = search.lower()
    mask = pd.Series([False] * len(df), index=df.index)
    for col in ['Job_ID', 'Product_Name', 'Notification_Detail', 'Stop_Number']:
        if col in df.columns:
            mask |= df[col].astype(str).str.lower().str.contains(s, na=False)
    df = df[mask]


# ── Status Masks ───────────────────────────────────────────────────────────────
# Scanned = has a Last_Scan_User value
scanned_mask = (
    df.get('Last_Scan_User', pd.Series([''] * len(df), index=df.index))
    .astype(str).str.strip().replace('nan', '').ne('')
) if 'Last_Scan_User' in df.columns else pd.Series([False] * len(df), index=df.index)

# Also check Scan_Count > 0 if available
if 'Scan_Count' in df.columns:
    scanned_mask = scanned_mask | (pd.to_numeric(df['Scan_Count'], errors='coerce').fillna(0) > 0)

# Arrived = has Actual_Date
arrived_mask = pd.to_datetime(df.get('Actual_Date'), errors='coerce').notna() \
    if 'Actual_Date' in df.columns else pd.Series([False] * len(df), index=df.index)

# Routed = has Assigned_Driver
routed_mask = (
    df.get('Is_Routed', pd.Series([False] * len(df), index=df.index))
    .astype(bool)
) if 'Is_Routed' in df.columns else pd.Series([False] * len(df), index=df.index)

# Buckets
bucket_exception   = df[routed_mask & ~scanned_mask]                   # 🔴 Routed but NOT Scanned
bucket_ready_scan  = df[arrived_mask & ~scanned_mask & ~routed_mask]   # 📦 Arrived, not scanned, not routed
bucket_ready_route = df[scanned_mask & ~routed_mask]                   # 🟡 Scanned, needs routing
bucket_in_transit  = df[scanned_mask & routed_mask]                    # 🟢 Scanned + Routed

if show_action:
    df = bucket_exception if not bucket_exception.empty else df[pd.Series([False]*len(df), index=df.index)]


# ── TABS ───────────────────────────────────────────────────────────────────────
tab_overview, tab_board, tab_reschedules, tab_full = st.tabs(
    ["📊 Overview", "📋 Job Board", "🔁 Reschedules", "📄 Full Job List"]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:

    # ── KPI Cards ──
    k1, k2, k3, k4, k5 = st.columns(5)

    def kpi(col, label, value, sub, color):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value {color}">{value}</div>
                <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    kpi(k1, "Active Jobs",          len(df),                   "in date range",              "kpi-white")
    kpi(k2, "Routed Exception 🔴",  len(bucket_exception),     "routed but not scanned",     "kpi-red"   if len(bucket_exception) > 0 else "kpi-green")
    kpi(k3, "Ready for Scan 📦",    len(bucket_ready_scan),    "arrived, awaiting scan",     "kpi-amber" if len(bucket_ready_scan) > 0 else "kpi-green")
    kpi(k4, "Ready for Routing 🟡", len(bucket_ready_route),   "scanned, needs driver",      "kpi-amber" if len(bucket_ready_route) > 0 else "kpi-green")
    kpi(k5, "In Transit 🟢",        len(bucket_in_transit),    "scanned + driver assigned",  "kpi-green")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Data Info Bar ──
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.markdown(f"**📅 Data Date:** {data_date.strftime('%m/%d/%Y') if data_date else 'Unknown'}")
    with col_info2:
        st.markdown(f"**🕐 Cache refreshes every:** 15 minutes")
    with col_info3:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

    # ── Exception Watchlist ──
    if not bucket_exception.empty:
        st.markdown("---")
        st.markdown("### 🚨 Exception Watchlist — Routed but Not Scanned")
        st.markdown("*These jobs have a driver assigned but no scan recorded. Confirm status immediately.*")
        disp_cols = [c for c in ['Job_ID', 'Product_Name', 'Planned_Date', 'Carrier', 'State',
                                  'Assigned_Driver', 'Stop_Number'] if c in bucket_exception.columns]
        _ex = bucket_exception[disp_cols].reset_index(drop=True).copy()
        for _dc in ['Planned_Date', 'Actual_Date']:
            if _dc in _ex.columns:
                _ex[_dc] = pd.to_datetime(_ex[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
        st.dataframe(
            _ex,
            use_container_width=True, hide_index=True
        )

    # ── Overdue Arrivals ──
    today = datetime.now().date()
    if 'Planned_Date' in df.columns and 'Actual_Date' in df.columns:
        overdue = df[
            (pd.to_datetime(df['Planned_Date'], errors='coerce').dt.date < today) &
            (pd.to_datetime(df['Actual_Date'], errors='coerce').isna())
        ]
        if not overdue.empty:
            st.markdown("---")
            st.markdown(f"### ⚠️ Overdue Arrivals ({len(overdue)})")
            st.markdown("*Planned date has passed — not yet arrived at dock.*")
            disp_cols = [c for c in ['Job_ID', 'Product_Name', 'Planned_Date', 'Carrier', 'State'] if c in overdue.columns]
            _od = overdue[disp_cols].reset_index(drop=True).copy()
            for _dc in ['Planned_Date', 'Actual_Date']:
                if _dc in _od.columns:
                    _od[_dc] = pd.to_datetime(_od[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
            st.dataframe(_od,
                         use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — JOB BOARD
# ══════════════════════════════════════════════════════════════════════════════
with tab_board:
    st.markdown("### Intake & Routing Board")
    col_dock, col_dispatch = st.columns(2)

    def bucket_table(bucket_df, cols):
        """Render a compact table for a bucket."""
        display_cols = [c for c in cols if c in bucket_df.columns]
        if bucket_df.empty:
            st.markdown("<p style='color:#60657A; font-size:0.8rem; padding: 8px 0;'>No items.</p>",
                        unsafe_allow_html=True)
        else:
            _bt = bucket_df[display_cols].reset_index(drop=True).copy()
            for _dc in ['Planned_Date', 'Actual_Date']:
                if _dc in _bt.columns:
                    _bt[_dc] = pd.to_datetime(_bt[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
            st.dataframe(_bt, use_container_width=True, hide_index=True, height=300)

    DOCK_COLS     = ['Job_ID', 'Product_Name', 'Planned_Date', 'Carrier', 'Stop_Number']
    DISPATCH_COLS = ['Job_ID', 'Product_Name', 'Last_Scan_User', 'Planned_Date', 'Carrier', 'Stop_Number']
    TRANSIT_COLS  = ['Job_ID', 'Product_Name', 'Last_Scan_User', 'Assigned_Driver', 'Planned_Date', 'Carrier']

    with col_dock:
        st.markdown("**Dock & Intake Operations**")

        # 🔴 Routed Exception
        st.markdown(f"""
        <div class="bucket-header bucket-red">
            <span>🔴</span>
            <div>
                <div class="bucket-title">Routed Exception</div>
                <div class="bucket-desc">Driver assigned — scan missing</div>
            </div>
            <span class="bucket-count kpi-red">{len(bucket_exception)}</span>
        </div>""", unsafe_allow_html=True)
        bucket_table(bucket_exception, DOCK_COLS)

        st.markdown("<br>", unsafe_allow_html=True)

        # 📦 Ready for Scan
        st.markdown(f"""
        <div class="bucket-header bucket-green">
            <span>📦</span>
            <div>
                <div class="bucket-title">Ready for Scan</div>
                <div class="bucket-desc">Arrived at dock — awaiting scan</div>
            </div>
            <span class="bucket-count kpi-green">{len(bucket_ready_scan)}</span>
        </div>""", unsafe_allow_html=True)
        bucket_table(bucket_ready_scan, DOCK_COLS)

    with col_dispatch:
        st.markdown("**Dispatch & Outbound**")

        # 🟡 Ready for Routing
        st.markdown(f"""
        <div class="bucket-header bucket-amber">
            <span>🟡</span>
            <div>
                <div class="bucket-title">ACTION: Ready for Routing</div>
                <div class="bucket-desc">Scanned — needs driver assignment</div>
            </div>
            <span class="bucket-count kpi-amber">{len(bucket_ready_route)}</span>
        </div>""", unsafe_allow_html=True)
        bucket_table(bucket_ready_route, DISPATCH_COLS)

        st.markdown("<br>", unsafe_allow_html=True)

        # 🟢 In Transit
        st.markdown(f"""
        <div class="bucket-header bucket-blue">
            <span>🟢</span>
            <div>
                <div class="bucket-title">In Transit</div>
                <div class="bucket-desc">Scanned + driver assigned</div>
            </div>
            <span class="bucket-count kpi-blue">{len(bucket_in_transit)}</span>
        </div>""", unsafe_allow_html=True)
        bucket_table(bucket_in_transit, TRANSIT_COLS)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RESCHEDULES
# ══════════════════════════════════════════════════════════════════════════════
with tab_reschedules:
    st.markdown("### 🔁 Rescheduled Jobs")
    st.markdown("*Jobs with a 'Re-scheduled' status in FileMaker — still active but date has changed.*")

    if 'Status' not in df.columns:
        st.info("Status column not available in this dataset.")
    else:
        rescheduled = df[
            df['Status'].astype(str).str.lower().str.strip().str.contains('re-schedul|reschedul', na=False)
        ]

        if rescheduled.empty:
            st.success("✅ No re-scheduled jobs in the current date range.")
        else:
            st.warning(f"⚠️ {len(rescheduled)} re-scheduled job(s) found.")
            disp_cols = [c for c in ['Job_ID', 'Product_Name', 'Product_Serial',
                                      'Planned_Date', 'Status', 'Carrier', 'State',
                                      'Last_Scan_User', 'Assigned_Driver']
                         if c in rescheduled.columns]
            _rs = rescheduled[disp_cols].reset_index(drop=True).copy()
            for _dc in ['Planned_Date', 'Actual_Date']:
                if _dc in _rs.columns:
                    _rs[_dc] = pd.to_datetime(_rs[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
            st.dataframe(_rs,
                         use_container_width=True, hide_index=True)




# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — FULL JOB LIST
# ══════════════════════════════════════════════════════════════════════════════
with tab_full:
    st.markdown(f"### 📄 Full Job List — {len(df)} active jobs")

    # Visual status column
    def visual_status(row):
        is_scanned = False
        if 'Last_Scan_User' in row and str(row.get('Last_Scan_User', '')).strip() not in ['', 'nan']:
            is_scanned = True
        if 'Scan_Count' in row and pd.to_numeric(row.get('Scan_Count', 0), errors='coerce') > 0:
            is_scanned = True

        is_routed  = bool(row.get('Is_Routed', False))
        is_arrived = pd.notna(row.get('Actual_Date')) if 'Actual_Date' in row.index else False

        if is_routed and not is_scanned:   return "🔴 Routed Exception"
        if is_scanned and is_routed:       return "🟢 In Transit"
        if is_scanned and not is_routed:   return "🟡 Ready for Routing"
        if is_arrived and not is_scanned:  return "📦 Ready for Scan"
        return "⬜ Manifested"

    df_display = df.copy()
    # Format date columns to MM/DD/YYYY
    for _dc in ['Planned_Date', 'Actual_Date']:
        if _dc in df_display.columns:
            df_display[_dc] = pd.to_datetime(df_display[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
    df_display['Status_Visual'] = df_display.apply(visual_status, axis=1)

    display_cols = [c for c in [
        'Status_Visual', 'Job_ID', 'Product_Name', 'Product_Serial',
        'Planned_Date', 'Actual_Date', 'Carrier', 'State',
        'Last_Scan_User', 'Assigned_Driver', 'White_Glove', 'Stop_Number'
    ] if c in df_display.columns]

    st.dataframe(
        df_display[display_cols].reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
        height=600
    )

    # Download button
    csv = df_display[display_cols].to_csv(index=False)
    st.download_button(
        label="⬇️ Download CSV",
        data=csv,
        file_name=f"active_jobs_{datetime.now().strftime('%m%d%Y')}.csv",
        mime="text/csv"
    )
