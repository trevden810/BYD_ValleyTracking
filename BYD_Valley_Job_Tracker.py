import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from utils.api import process_data, fetch_jobs_from_excel
from v2.supabase_client import SupabaseClient
from v2.job_chains import get_chain_alerts, JobChainManager
from v2.flag_engine import flag_summary

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

/* Flag badges */
.flag-red    { color: #E05A5A; font-weight: 700; }
.flag-yellow { color: #F5A623; font-weight: 700; }
.flag-green  { color: #8DC63F; font-weight: 700; }
.flag-none   { color: #60657A; }

/* Reschedule watch tile */
.watch-tile {
    background: #1C2030;
    border: 1px solid rgba(245,166,35,.4);
    border-left: 4px solid #F5A623;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 8px;
}
.watch-serial { font-size: 1.0rem; font-weight: 700; color: #F5A623; }
.watch-meta   { font-size: 0.72rem; color: #808285; margin-top: 4px; }
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
                BYD &amp; Valley Tracking Board — V2.0
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
    Flag columns (computed_flag, flag_reason, etc.) are populated by the import pipeline.
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
                'scan_user': 'Scan_User',
                'scan_timestamp': 'Scan_Timestamp',
                'product_description': 'Product_Name',
                'piece_count': 'Piece_Count',
                'white_glove': 'White_Glove',
                'notification_detail': 'Notification_Detail',
                'miles_oneway': 'Miles_OneWay',
                'customer_name': 'Customer_Name',
                'delivery_address': 'Delivery_Address',
                'market': 'Market',
                'city': 'City',
                'product_serial': 'Product_Serial',
            }
            df = df.rename(columns=column_map)

            # Parse date columns
            for col in ['Planned_Date', 'Actual_Date', 'Scan_Timestamp']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Map assigned_driver from DB column
            if 'assigned_driver' in df.columns and 'Assigned_Driver' not in df.columns:
                df['Assigned_Driver'] = df['assigned_driver']

            # Also expose Last_Scan_User for backward compat
            if 'Scan_User' in df.columns:
                df['Last_Scan_User'] = df['Scan_User']

            # Ensure expected columns exist
            for col in ['Stop_Number', 'Product_Serial', 'Assigned_Driver',
                        'Customer_Notes', 'computed_flag', 'flag_reason',
                        'sla_hours_elapsed', 'sla_breach_level', 'is_pepmove_leg']:
                if col not in df.columns:
                    df[col] = '' if col not in ['sla_hours_elapsed'] else None

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
            # Compute flags on local data too
            try:
                from v2.flag_engine import evaluate_flags
                df = evaluate_flags(df)
            except Exception:
                for col in ['computed_flag', 'flag_reason']:
                    df[col] = 'none'
            return df, datetime.now().date()
        except Exception as e:
            print(f"[ERROR] Local fallback failed: {e}")

    return pd.DataFrame(), None


@st.cache_data(ttl=900)
def load_reschedule_watch():
    """Fetch unresolved reschedule watch tiles."""
    try:
        client = SupabaseClient()
        return client.get_reschedule_watch()
    except Exception:
        return []


df_raw, data_date = load_data()
reschedule_tiles = load_reschedule_watch()

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
        today_date   = datetime.now().date()
        future_limit = today_date + pd.Timedelta(days=60)
        date_range = st.date_input("Date Range", value=(today_date, future_limit),
                                   format="MM/DD/YYYY", label_visibility="collapsed")
    with fc5:
        show_wg = st.checkbox("White Glove", value=False)
    with fc6:
        show_red_only = st.checkbox("Red Flags Only", value=False)


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
    for col in ['Job_ID', 'Product_Name', 'Notification_Detail', 'Stop_Number',
                'Customer_Name', 'flag_reason']:
        if col in df.columns:
            mask |= df[col].astype(str).str.lower().str.contains(s, na=False)
    df = df[mask]

if show_red_only and 'computed_flag' in df.columns:
    df = df[df['computed_flag'] == 'red']


# ── Flag Masks ─────────────────────────────────────────────────────────────────
def _flag_col(frame):
    return frame.get('computed_flag', pd.Series(['none'] * len(frame), index=frame.index)).astype(str)

flags        = _flag_col(df)
red_jobs     = df[flags == 'red']
yellow_jobs  = df[flags == 'yellow']
green_jobs   = df[flags == 'green']

# Legacy scan / driver masks (kept for Job Board tab)
scanned_mask = (
    df.get('Scan_User', pd.Series([''] * len(df), index=df.index))
    .astype(str).str.strip().replace('nan', '').ne('')
) if 'Scan_User' in df.columns else pd.Series([False] * len(df), index=df.index)

if 'Scan_Count' in df.columns:
    scanned_mask = scanned_mask | (pd.to_numeric(df['Scan_Count'], errors='coerce').fillna(0) > 0)

arrived_mask = pd.to_datetime(df.get('Actual_Date'), errors='coerce').notna() \
    if 'Actual_Date' in df.columns else pd.Series([False] * len(df), index=df.index)

routed_mask = (
    df.get('Assigned_Driver', pd.Series([''] * len(df), index=df.index))
    .astype(str).str.strip().replace('nan', '').ne('')
)

# Job board buckets
bucket_exception   = df[routed_mask & ~scanned_mask]
bucket_ready_scan  = df[arrived_mask & ~scanned_mask & ~routed_mask]
bucket_ready_route = df[scanned_mask & ~routed_mask]
bucket_in_transit  = df[scanned_mask & routed_mask]

# ── Flag summary from full (unfiltered) data for KPI cards
full_flags = flag_summary(df)


# ── TABS ───────────────────────────────────────────────────────────────────────
tab_overview, tab_board, tab_flags, tab_reschedules, tab_full = st.tabs(
    ["📊 Overview", "📋 Job Board", "🚨 Flags", "🔁 Reschedule Watch", "📄 Full Job List"]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:

    # ── KPI Cards ──
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    def kpi(col, label, value, sub, color):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value {color}">{value}</div>
                <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    kpi(k1, "Active Jobs",          len(df),                          "in date range",                "kpi-white")
    kpi(k2, "🟢 In Progress",       full_flags.get('green', 0),       "scanned + driver assigned",   "kpi-green" if full_flags.get('green', 0) > 0 else "kpi-white")
    kpi(k3, "🟡 Driver SLA",        full_flags.get('yellow', 0),      "assign driver within 48 hrs", "kpi-amber" if full_flags.get('yellow', 0) > 0 else "kpi-green")
    kpi(k4, "🔴 SLA Breach",        full_flags.get('red_driver_sla', 0), "driver 48+ hrs overdue",   "kpi-red"   if full_flags.get('red_driver_sla', 0) > 0 else "kpi-green")
    kpi(k5, "🔴 Unscanned/Routed",  full_flags.get('red_no_scan', 0), "driver set, no scan",        "kpi-red"   if full_flags.get('red_no_scan', 0) > 0 else "kpi-green")
    kpi(k6, "🔴 PEPMOVE Overdue",   full_flags.get('red_pepmove', 0), "past scheduled date",         "kpi-red"   if full_flags.get('red_pepmove', 0) > 0 else "kpi-green")

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

    # ── Red Flag Summary ──
    if not red_jobs.empty:
        st.markdown("---")
        st.markdown(f"### 🔴 Active Red Flags ({len(red_jobs)})")
        _rc = [c for c in ['Job_ID', 'Product_Name', 'Piece_Count', 'Planned_Date', 'Carrier', 'State',
                            'Assigned_Driver', 'flag_reason'] if c in red_jobs.columns]
        _red = red_jobs[_rc].reset_index(drop=True).copy()
        if 'Planned_Date' in _red.columns:
            _red['Planned_Date'] = pd.to_datetime(_red['Planned_Date'], errors='coerce').dt.strftime('%m/%d/%Y')
        st.dataframe(_red, width='stretch', hide_index=True)

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
            disp_cols = [c for c in ['Job_ID', 'Product_Name', 'Piece_Count', 'Planned_Date', 'Carrier', 'State'] if c in overdue.columns]
            _od = overdue[disp_cols].reset_index(drop=True).copy()
            if 'Planned_Date' in _od.columns:
                _od['Planned_Date'] = pd.to_datetime(_od['Planned_Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            st.dataframe(_od, width='stretch', hide_index=True)


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
            st.dataframe(_bt, width='stretch', hide_index=True, height=300)

    DOCK_COLS     = ['Job_ID', 'Product_Name', 'Piece_Count', 'Planned_Date', 'Carrier', 'State', 'Stop_Number']
    DISPATCH_COLS = ['Job_ID', 'Product_Name', 'Piece_Count', 'Scan_User', 'Planned_Date', 'Carrier', 'State', 'Stop_Number']
    TRANSIT_COLS  = ['Job_ID', 'Product_Name', 'Piece_Count', 'Scan_User', 'Assigned_Driver', 'Planned_Date', 'Carrier', 'State']

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
# TAB 3 — FLAGS
# ══════════════════════════════════════════════════════════════════════════════
with tab_flags:
    st.markdown("### 🚨 Active Flag Dashboard")
    st.markdown("*Hardwired SLA rules evaluated on every import. Flags reflect the current state of each job.*")

    # ── Red Flags ──────────────────────────────────────────────────────────────
    st.markdown(f"#### 🔴 Red Flags — Immediate Action Required ({len(red_jobs)})")
    if red_jobs.empty:
        st.success("✅ No red flags. All rules are satisfied.")
    else:
        # Sub-group by reason type
        if 'flag_reason' in red_jobs.columns:
            no_scan_r  = red_jobs[red_jobs['flag_reason'].str.contains('NOT scanned', case=False, na=False)]
            sla_breach = red_jobs[red_jobs['flag_reason'].str.contains('BREACHED', case=False, na=False)]
            pepmove_r  = red_jobs[red_jobs['flag_reason'].str.contains('PEPMOVE', case=False, na=False)]
            
            # Use .union() to combine indices, as bitwise OR (|) can fail if shapes/types differ
            combined_index = no_scan_r.index.union(sla_breach.index).union(pepmove_r.index)
            other_r    = red_jobs[~red_jobs.index.isin(combined_index)]
        else:
            no_scan_r = sla_breach = pepmove_r = other_r = pd.DataFrame()

        FLAG_COLS = ['Job_ID', 'Product_Name', 'Piece_Count', 'Planned_Date', 'Carrier', 'State',
                     'Assigned_Driver', 'Scan_User', 'sla_hours_elapsed', 'flag_reason']

        def flag_table(frame, title, icon, desc):
            if frame.empty:
                return
            st.markdown(f"**{icon} {title}** — {len(frame)} job(s)")
            show = [c for c in FLAG_COLS if c in frame.columns]
            _ft = frame[show].reset_index(drop=True).copy()
            if 'Planned_Date' in _ft.columns:
                _ft['Planned_Date'] = pd.to_datetime(_ft['Planned_Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            if 'sla_hours_elapsed' in _ft.columns:
                _ft['sla_hours_elapsed'] = _ft['sla_hours_elapsed'].apply(
                    lambda x: f"{x:.1f} hrs" if pd.notna(x) else '—'
                )
            st.dataframe(_ft, width='stretch', hide_index=True)
            st.markdown(f"<div style='font-size:0.7rem;color:#808285;margin-bottom:12px;'>{desc}</div>",
                        unsafe_allow_html=True)

        flag_table(no_scan_r,  "Unscanned / Routed",    "🔴",
                   "Driver has been assigned but the unit has NOT been scanned. Warehouse must scan BEFORE delivery.")
        flag_table(sla_breach, "Driver SLA Breach",      "🔴",
                   "Unit was scanned 48+ hours ago. Driver must be assigned immediately.")
        flag_table(pepmove_r,  "PEPMOVE Leg Overdue",    "🔴",
                   "Delivery was scheduled to arrive at PEPMOVE but is past its planned date.")
        flag_table(other_r,    "Other Red Flags",         "🔴", "")

    st.markdown("---")

    # ── Yellow Flags ───────────────────────────────────────────────────────────
    st.markdown(f"#### 🟡 Yellow Flags — Driver Assignment SLA ({len(yellow_jobs)})")
    if yellow_jobs.empty:
        st.success("✅ No yellow flags.")
    else:
        st.info("These units were scanned within the last 48 hours but have no driver assigned. "
                "Assign a driver before the 48-hr breach window.")

        Y_COLS = ['Job_ID', 'Product_Name', 'Piece_Count', 'Planned_Date', 'Carrier', 'State',
                  'Scan_User', 'sla_hours_elapsed', 'sla_breach_level', 'flag_reason']
        show = [c for c in Y_COLS if c in yellow_jobs.columns]
        _yt = yellow_jobs[show].reset_index(drop=True).copy()
        if 'Planned_Date' in _yt.columns:
            _yt['Planned_Date'] = pd.to_datetime(_yt['Planned_Date'], errors='coerce').dt.strftime('%m/%d/%Y')
        if 'sla_hours_elapsed' in _yt.columns:
            _yt['sla_hours_elapsed'] = _yt['sla_hours_elapsed'].apply(
                lambda x: f"{x:.1f} hrs" if pd.notna(x) else '—'
            )
        st.dataframe(_yt, width='stretch', hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — RESCHEDULE WATCH
# ══════════════════════════════════════════════════════════════════════════════
with tab_reschedules:
    st.markdown("### 🔁 Reschedule Watch")
    st.markdown(
        "*Tiles are created when a job is **Re-scheduled** in FileMaker and cleared automatically "
        "once a new job with the same serial number reaches Entered or Scheduled status.*"
    )

    # Live tiles from DB
    if reschedule_tiles:
        today_str = datetime.now().date()
        st.warning(f"⚠️ {len(reschedule_tiles)} serial number(s) awaiting re-assignment")
        tile_cols = st.columns(min(3, len(reschedule_tiles)))
        for i, tile in enumerate(reschedule_tiles):
            col_i = tile_cols[i % len(tile_cols)]
            with col_i:
                resched_date = tile.get('rescheduled_at', 'Unknown')
                days_str = ""
                if resched_date and resched_date != 'Unknown':
                    try:
                        d = datetime.strptime(str(resched_date), '%Y-%m-%d').date()
                        days = (today_str - d).days
                        days_str = f" — watching {days} day(s)"
                    except Exception:
                        pass

                st.markdown(f"""
                <div class="watch-tile">
                    <div class="watch-serial">📦 {tile.get('product_serial', 'N/A')}</div>
                    <div class="watch-meta">
                        Original Job: {tile.get('original_job_id', '—')}<br>
                        Carrier: {tile.get('carrier', '—')}<br>
                        Rescheduled: {resched_date}{days_str}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.success("✅ No active reschedule watches. All re-scheduled serials have been re-assigned.")

    st.markdown("---")

    # Fallback: also show FileMaker "Re-scheduled" status rows
    st.markdown("#### FileMaker Re-Scheduled Status Jobs")
    st.markdown("*Jobs currently in Re-scheduled status in FileMaker (from current export).*")

    if 'Status' not in df.columns:
        st.info("Status column not available in this dataset.")
    else:
        rescheduled_fm = df[
            df['Status'].astype(str).str.lower().str.strip().str.contains('re-schedul|reschedul', na=False)
        ]
        if rescheduled_fm.empty:
            st.success("✅ No re-scheduled jobs in the current date range.")
        else:
            disp_cols = [c for c in ['Job_ID', 'Product_Name', 'Piece_Count', 'Product_Serial',
                                      'Planned_Date', 'Status', 'Carrier', 'State',
                                      'Scan_User', 'Assigned_Driver']
                         if c in rescheduled_fm.columns]
            _rs = rescheduled_fm[disp_cols].reset_index(drop=True).copy()
            if 'Planned_Date' in _rs.columns:
                _rs['Planned_Date'] = pd.to_datetime(_rs['Planned_Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            st.dataframe(_rs, width='stretch', hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — FULL JOB LIST
# ══════════════════════════════════════════════════════════════════════════════
with tab_full:
    st.markdown(f"### 📄 Full Job List — {len(df)} active jobs")

    # Visual flag badge
    def visual_flag(row):
        flag = str(row.get('computed_flag', 'none')).lower()
        if flag == 'red':    return "🔴 Red"
        if flag == 'yellow': return "🟡 Yellow"
        if flag == 'green':  return "🟢 In Progress"
        # Fallback to legacy status logic
        is_scanned = str(row.get('Scan_User', '')).strip() not in ['', 'nan']
        is_routed  = str(row.get('Assigned_Driver', '')).strip() not in ['', 'nan']
        is_arrived = pd.notna(row.get('Actual_Date')) if 'Actual_Date' in row.index else False
        if is_routed and not is_scanned:   return "🔴 Routed Exception"
        if is_scanned and is_routed:       return "🟢 In Progress"
        if is_scanned and not is_routed:   return "🟡 Ready for Routing"
        if is_arrived and not is_scanned:  return "📦 Ready for Scan"
        return "⬜ Manifested"

    df_display = df.copy()
    for _dc in ['Planned_Date', 'Actual_Date']:
        if _dc in df_display.columns:
            df_display[_dc] = pd.to_datetime(df_display[_dc], errors='coerce').dt.strftime('%m/%d/%Y')
    df_display['Flag'] = df_display.apply(visual_flag, axis=1)

    display_cols = [c for c in [
        'Flag', 'Job_ID', 'Product_Name', 'Piece_Count', 'Product_Serial',
        'Planned_Date', 'Actual_Date', 'Carrier', 'State',
        'Scan_User', 'Assigned_Driver', 'White_Glove', 'Stop_Number',
        'flag_reason'
    ] if c in df_display.columns]

    st.dataframe(
        df_display[display_cols].reset_index(drop=True),
        width='stretch',
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
