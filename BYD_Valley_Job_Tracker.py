import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from utils.api import get_token, fetch_jobs, process_data, fetch_jobs_from_excel
from v2.supabase_client import SupabaseClient
from v2.job_chains import get_chain_alerts, JobChainManager

# Load environment variables from .env file
load_dotenv()

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PEPMOVE | Dock Operations",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Load CSS ──────────────────────────────────────────────────────────────────
def load_css(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

load_css("assets/style.css")

# ── PEPMOVE Header ────────────────────────────────────────────────────────────
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

st.markdown(
    "<div style='border-bottom: 2px solid #8DC63F; margin: 12px 0 20px 0;'></div>",
    unsafe_allow_html=True
)

# ── Data Loading ──────────────────────────────────────────────────────────────
today = datetime.now().date()

@st.cache_data(ttl=900)
def load_data(target_date):
    # 1. Try Supabase (Primary Source for ALL dates)
    try:
        client = SupabaseClient()
        df = client.get_snapshot_by_date(target_date)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"Error fetching from Supabase: {e}")

    # 2. Fallback to local file (Only for Today)
    # Note: This is a fallback if Supabase is unreachable or empty
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
            
    return pd.DataFrame()

selected_date = today
df_main = load_data(selected_date)

if df_main.empty:
    st.warning("⚠️ Data source not found (`bydhistorical.xlsx`) or empty. Run the daily import first.")
    st.stop()

# ── Global Filters (collapsible) ──────────────────────────────────────────────
with st.expander("🔍 Filters", expanded=True):
    f_col1, f_col2, f_col3, f_col4, f_col5, f_col6 = st.columns([2.5, 1.5, 1.5, 2, 1, 1])

    with f_col1:
        search_term = st.text_input(
            "Search", placeholder="🔍  Job ID, Product, or Notes...",
            label_visibility="collapsed"
        )

    with f_col2:
        carriers = (["All Carriers"] + sorted(df_main['Carrier'].dropna().unique().tolist())
                    if 'Carrier' in df_main.columns else ["All Carriers"])
        selected_carrier = st.selectbox("Carrier", carriers, label_visibility="collapsed")

    with f_col3:
        states = (["All States"] + sorted(df_main['State'].dropna().unique().tolist())
                  if 'State' in df_main.columns else ["All States"])
        selected_state = st.selectbox("State", states, label_visibility="collapsed")

    with f_col4:
        start_of_year = datetime(today.year, 1, 1).date()
        future_limit  = today + pd.Timedelta(days=60)
        date_range = st.date_input(
            "Date Range",
            value=(start_of_year, future_limit),
            format="MM/DD/YYYY",
            label_visibility="collapsed"
        )

    with f_col5:
        show_white_glove = st.checkbox("White Glove", value=False)

    with f_col6:
        show_unscanned_only = st.checkbox("Action Req", value=False)

# ── Filter Logic ──────────────────────────────────────────────────────────────
df_filtered = df_main.copy()

# Exclude Completed Jobs (Delivered / Complete)
# User Request: These should be stored in DB but removed from active counts.
if 'Status' in df_filtered.columns:
    # Normalize status to lower case for comparison
    status_lower = df_filtered['Status'].astype(str).str.lower().str.strip()
    # Filter OUT completed statuses
    df_filtered = df_filtered[~status_lower.isin(['delivered', 'complete', 'completed'])]

# Date range
if len(date_range) == 2:
    start_date, end_date = date_range
    if 'Planned_Date' in df_filtered.columns:
        mask_date = (
            (pd.to_datetime(df_filtered['Planned_Date'], errors='coerce').dt.date >= start_date) &
            (pd.to_datetime(df_filtered['Planned_Date'], errors='coerce').dt.date <= end_date)
        )
        df_filtered = df_filtered[mask_date]

# Search
if search_term:
    s = search_term.lower()
    mask = (
        df_filtered['Job_ID'].astype(str).str.lower().str.contains(s, na=False) |
        df_filtered['Product_Name'].astype(str).str.lower().str.contains(s, na=False) |
        df_filtered['Customer_Notes'].astype(str).str.lower().str.contains(s, na=False)
    )
    df_filtered = df_filtered[mask]

# Dropdowns
if selected_carrier != "All Carriers":
    df_filtered = df_filtered[df_filtered['Carrier'] == selected_carrier]
if selected_state != "All States":
    df_filtered = df_filtered[df_filtered['State'] == selected_state]

# White Glove
if show_white_glove and 'White_Glove' in df_filtered.columns:
    df_filtered = df_filtered[df_filtered['White_Glove'] == True]

# Scan masks (computed once, used across sections)
if 'Last_Scan_User' in df_filtered.columns:
    scanned_mask = (df_filtered['Last_Scan_User'].notna()) & (df_filtered['Last_Scan_User'] != '')
else:
    scanned_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)

if show_unscanned_only:
    df_filtered = df_filtered[~scanned_mask]
    scanned_mask = scanned_mask[df_filtered.index]  # realign after filter

# ── Computed KPIs ─────────────────────────────────────────────────────────────
current_view_count = len(df_filtered)
scanned_n   = int(scanned_mask.sum()) if len(scanned_mask) == len(df_filtered) else 0
unscanned_n = current_view_count - scanned_n

# SLA Risk: Planned < Today AND not scanned
sla_risk_n = 0
overdue_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)
if 'Planned_Date' in df_filtered.columns:
    safe_planned = pd.to_datetime(df_filtered['Planned_Date'], errors='coerce')
    overdue_mask = (safe_planned.dt.date < today) & (~scanned_mask)
    sla_risk_n   = int(overdue_mask.sum())

# White Glove Pending
wg_pending_n = 0
wg_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)
if 'White_Glove' in df_filtered.columns:
    wg_mask      = (df_filtered['White_Glove'] == True) & (~scanned_mask)
    wg_pending_n = int(wg_mask.sum())

# ── Tab Navigation ────────────────────────────────────────────────────────────
tab_dash, tab_board, tab_reschedule, tab_list = st.tabs([
    "📊  Dashboard",
    "📋  Job Board",
    "🔄  Reschedules",
    "📝  Full Job List"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_dash:

    # KPI Cards
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total In View",  f"{current_view_count}", "Jobs")
    m2.metric("Scanned",        f"{scanned_n}",
              f"{(scanned_n / current_view_count * 100):.0f}%" if current_view_count else "0%")
    m3.metric("Unscanned",      f"{unscanned_n}", "Action Req",   delta_color="inverse")
    m4.metric("SLA Risk",       f"{sla_risk_n}",  "Critical",     delta_color="inverse")
    m5.metric("White Glove",    f"{wg_pending_n}", "Pending",     delta_color="normal")

    st.markdown("<br>", unsafe_allow_html=True)

    # Exception Watchlist
    if sla_risk_n > 0 or wg_pending_n > 0:
        st.markdown("""
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">
                <div style="width:4px; height:22px; background:#EF4444; border-radius:2px;"></div>
                <h3 style="margin:0; color:#F0F2F5;">Exception Watchlist</h3>
            </div>
        """, unsafe_allow_html=True)

        watchlist_mask = overdue_mask | wg_mask
        df_watchlist   = df_filtered[watchlist_mask].copy()

        if not df_watchlist.empty:
            df_watchlist['Days_Overdue'] = (
                pd.to_datetime(today) -
                pd.to_datetime(df_watchlist['Planned_Date'], errors='coerce').dt.normalize()
            ).dt.days.fillna(0).astype(int)

            df_watchlist.sort_values(
                by=['Days_Overdue', 'White_Glove'], ascending=[False, False], inplace=True
            )

            wl_cols = {
                'Job_ID': 'Job ID', 'Product_Name': 'Product', 'Carrier': 'Carrier',
                'Planned_Date': 'Planned', 'Days_Overdue': 'Days Late',
                'White_Glove': 'White Glove', 'State': 'State'
            }
            final_wl_cols = {k: v for k, v in wl_cols.items() if k in df_watchlist.columns}
            df_wl_display = df_watchlist[list(final_wl_cols.keys())].rename(columns=final_wl_cols)

            st.dataframe(
                df_wl_display.style.format({'Planned': '{:%Y-%m-%d}'}),
                use_container_width=True,
                hide_index=True
            )
        st.divider()
    else:
        st.success("✅ No exceptions — all jobs are on track within the selected filters.")

    # Quick summary bar
    scan_pct = (scanned_n / current_view_count * 100) if current_view_count else 0
    st.markdown(f"""
        <div style="background:{('#1E2124')}; border:1px solid rgba(255,255,255,0.07);
                    border-radius:8px; padding:14px 20px; margin-top:8px;
                    display:flex; gap:40px; flex-wrap:wrap;">
            <div>
                <div style="font-size:0.7rem; color:#808285; text-transform:uppercase;
                            letter-spacing:0.06em;">Scan Rate</div>
                <div style="font-size:1.3rem; font-weight:700; color:#8DC63F;">{scan_pct:.1f}%</div>
            </div>
            <div>
                <div style="font-size:0.7rem; color:#808285; text-transform:uppercase;
                            letter-spacing:0.06em;">Date Range</div>
                <div style="font-size:1rem; font-weight:600; color:#F0F2F5;">
                    {date_range[0].strftime('%b %d') if len(date_range) == 2 else '—'}
                    &nbsp;→&nbsp;
                    {date_range[1].strftime('%b %d, %Y') if len(date_range) == 2 else '—'}
                </div>
            </div>
            <div>
                <div style="font-size:0.7rem; color:#808285; text-transform:uppercase;
                            letter-spacing:0.06em;">Last Refreshed</div>
                <div style="font-size:1rem; font-weight:600; color:#F0F2F5;">
                    {datetime.now().strftime('%I:%M %p')}
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — JOB BOARD
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — JOB BOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_board:

    st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:16px;">
            <div style="width:4px; height:22px; background:#8DC63F; border-radius:2px;"></div>
            <h3 style="margin:0; color:#F0F2F5;">Intake &amp; Routing Board</h3>
        </div>
    """, unsafe_allow_html=True)

    if 'Actual_Date' in df_filtered.columns and 'Last_Scan_User' in df_filtered.columns:
        # masks
        arrived_mask  = df_filtered['Actual_Date'].notna()
        scanned_mask2 = (df_filtered['Last_Scan_User'].notna()) & (df_filtered['Last_Scan_User'] != '')
        routed_mask   = df_filtered['Assigned_Driver'].apply(lambda x: True if str(x).lower() not in ['nan','none','','unknown'] else False)

        if 'Status' in df_filtered.columns:
            completed_mask = df_filtered['Status'].str.lower().isin(['delivered', 'complete', 'completed'])
        else:
            completed_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)
        
        # 1. Routed Exception (Routed + NOT Scanned) -> CRITICAL
        # Note: Must be arrived or planned? Usually check if it's "active". 
        # For now, let's assume if it's visible in this filtered view (e.g. date range), it's fair game.
        # But commonly we only care if it Arrived AND Routed but no scan? Or just Routed no scan?
        # User request: "routed and not scanned" -> Red Flag.
        bucket_routed_noscan = df_filtered[routed_mask & ~scanned_mask2 & ~completed_mask]
        
        # 2. Ready for Scan (Arrived + NOT Scanned + NOT Routed) -> Standard Intake
        bucket_intake = df_filtered[arrived_mask & ~scanned_mask2 & ~routed_mask & ~completed_mask]
        
        # 3. Ready for Routing (Scanned + NOT Routed) -> CTA / Action Req
        bucket_ready_routing = df_filtered[scanned_mask2 & ~routed_mask & ~completed_mask]
        
        # 4. In Transit (Scanned + Routed) -> Good
        bucket_transit = df_filtered[scanned_mask2 & routed_mask & ~completed_mask]

        # ── Layout ──
        # Left Column: Dock / Intake Issues
        # Right Column: Outbound / Dispatch Flow
        b_col1, b_col2 = st.columns(2)

        with b_col1:
            st.caption("Dock & Intake Operations")
            
            # [A] Routed Exception (Critical)
            count_exception = len(bucket_routed_noscan)
            if count_exception > 0:
                st.markdown(f"""
                    <div style="background:#1E2124; border:1px solid rgba(239, 68, 68, 0.3);
                                border-left:3px solid #EF4444; border-radius:8px;
                                padding:12px 16px; margin-bottom:12px;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <div>
                                <div style="font-size:0.72rem; color:#EF4444; text-transform:uppercase;
                                            letter-spacing:0.06em; font-weight:700;">⚠️ Routed Exception</div>
                                <div style="font-size:1.8rem; font-weight:700; color:#EF4444;">{count_exception}</div>
                                <div style="font-size:0.78rem; color:#9EA3A8;">Router assigned but NOT scanned</div>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                cols_exc = [c for c in ['Job_ID', 'Assigned_Driver', 'Planned_Date', 'Product_Name'] if c in bucket_routed_noscan.columns]
                st.dataframe(bucket_routed_noscan[cols_exc].head(50), use_container_width=True, hide_index=True)
                st.divider()
            
            # [B] Ready for Scan (Standard)
            count_intake = len(bucket_intake)
            st.markdown(f"""
                <div style="background:#1E2124; border:1px solid rgba(255,255,255,0.07);
                            border-left:3px solid #8DC63F; border-radius:8px;
                            padding:12px 16px; margin-bottom:12px;">
                    <div style="font-size:0.72rem; color:#808285; text-transform:uppercase;
                                letter-spacing:0.06em;">Ready for Scan</div>
                    <div style="font-size:1.8rem; font-weight:700; color:#8DC63F;">{count_intake}</div>
                    <div style="font-size:0.78rem; color:#9EA3A8;">Arrived at dock — awaiting scan</div>
                </div>
            """, unsafe_allow_html=True)
            if not bucket_intake.empty:
                cols_i = [c for c in ['Job_ID', 'Carrier', 'Product_Name', 'Actual_Date'] if c in bucket_intake.columns]
                st.dataframe(bucket_intake[cols_i].head(50), use_container_width=True, hide_index=True)
            elif count_exception == 0:
                st.info("No items awaiting scan.")

        with b_col2:
            st.caption("Dispatch & Outbound")

            # [C] Ready for Routing (Action Req)
            count_routing = len(bucket_ready_routing)
            color_routing = "#F59E0B" if count_routing > 0 else "#808285"
            st.markdown(f"""
                <div style="background:#1E2124; border:1px solid rgba(245, 158, 11, 0.2);
                            border-left:3px solid {color_routing}; border-radius:8px;
                            padding:12px 16px; margin-bottom:12px;">
                    <div style="font-size:0.72rem; color:{color_routing}; text-transform:uppercase;
                                letter-spacing:0.06em; font-weight:600;">Action: Ready for Routing</div>
                    <div style="font-size:1.8rem; font-weight:700; color:#F0F2F5;">{count_routing}</div>
                    <div style="font-size:0.78rem; color:#9EA3A8;">Scanned — needs driver assignment</div>
                </div>
            """, unsafe_allow_html=True)
            if not bucket_ready_routing.empty:
                cols_r = [c for c in ['Job_ID', 'Product_Name', 'Last_Scan_User', 'Scanned_Date'] if c in bucket_ready_routing.columns]
                st.dataframe(bucket_ready_routing[cols_r].head(50), use_container_width=True, hide_index=True)
                st.divider()

            # [D] In Transit (Good)
            count_transit = len(bucket_transit)
            st.markdown(f"""
                <div style="background:#1E2124; border:1px solid rgba(255,255,255,0.07);
                            border-left:3px solid #10B981; border-radius:8px;
                            padding:12px 16px; margin-bottom:12px;">
                    <div style="font-size:0.72rem; color:#808285; text-transform:uppercase;
                                letter-spacing:0.06em;">In Transit / Route</div>
                    <div style="font-size:1.8rem; font-weight:700; color:#10B981;">{count_transit}</div>
                    <div style="font-size:0.78rem; color:#9EA3A8;">Scanned & Assigned</div>
                </div>
            """, unsafe_allow_html=True)
            if not bucket_transit.empty:
                cols_t = [c for c in ['Job_ID', 'Assigned_Driver', 'Last_Scan_User', 'Status'] if c in bucket_transit.columns]
                st.dataframe(bucket_transit[cols_t].head(50), use_container_width=True, hide_index=True)

    else:
        st.info("Required data (Arrival/Scan/Driver) not available for Job Board.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RESCHEDULE TRACKER
# ═══════════════════════════════════════════════════════════════════════════════
with tab_reschedule:

    st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:16px;">
            <div style="width:4px; height:22px; background:#F59E0B; border-radius:2px;"></div>
            <h3 style="margin:0; color:#F0F2F5;">Reschedule Tracker</h3>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
        <p style="color:#9EA3A8; font-size:0.85rem; margin-bottom:16px;">
            Track jobs that have been rescheduled. Products are linked by serial number 
            to show the complete history of reschedules.
        </p>
    """, unsafe_allow_html=True)

    # Try to load chain data from Supabase
    chain_alerts = []
    active_chains = []
    
    try:
        supabase_client = SupabaseClient()
        chain_alerts = get_chain_alerts(supabase_client.client)
        
        # Get all active chains
        manager = JobChainManager(supabase_client.client)
        active_chains = manager.get_active_chains()
        
    except Exception as e:
        st.warning(f"⚠️ Unable to load chain data from database: {str(e)[:50]}...")
        chain_alerts = []
        active_chains = []

    # KPI Cards for Reschedules
    r_col1, r_col2, r_col3, r_col4 = st.columns(4)
    
    total_chains = len(active_chains)
    critical_count = len([a for a in chain_alerts if a['severity'] == 'critical'])
    warning_count = len([a for a in chain_alerts if a['severity'] == 'warning'])
    high_freq_count = len([c for c in active_chains if c.get('reschedule_count', 0) >= 2])
    
    r_col1.metric("Active Chains", f"{total_chains}", "Products tracked")
    r_col2.metric("Critical Alerts", f"{critical_count}", "3+ reschedules", delta_color="inverse")
    r_col3.metric("Warnings", f"{warning_count}", "2 reschedules or 14+ days", delta_color="inverse")
    r_col4.metric("High Frequency", f"{high_freq_count}", "2+ reschedules")
    
    st.markdown("<br>", unsafe_allow_html=True)

    # Alerts Section
    if chain_alerts:
        st.markdown("""
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:12px;">
                <div style="width:4px; height:18px; background:#EF4444; border-radius:2px;"></div>
                <h4 style="margin:0; color:#F0F2F5;">⚠️ Chain Alerts</h4>
            </div>
        """, unsafe_allow_html=True)
        
        for alert in chain_alerts[:10]:  # Show top 10 alerts
            severity_color = "#EF4444" if alert['severity'] == 'critical' else "#F59E0B"
            severity_icon = "🚨" if alert['severity'] == 'critical' else "⚠️"
            
            st.markdown(f"""
                <div style="background:#1E2124; border:1px solid rgba(255,255,255,0.07);
                            border-left:3px solid {severity_color}; border-radius:6px;
                            padding:10px 14px; margin-bottom:8px;">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-size:1.1rem;">{severity_icon}</span>
                            <span style="font-weight:600; color:#F0F2F5; margin-left:8px;">
                                {alert.get('product_serial', 'N/A')}
                            </span>
                            <span style="color:#808285; margin-left:12px; font-size:0.8rem;">
                                {alert.get('carrier', '')}
                            </span>
                        </div>
                        <div style="text-align:right;">
                            <span style="background:{severity_color}; color:white; padding:2px 8px; 
                                        border-radius:4px; font-size:0.7rem; font-weight:600;">
                                {alert.get('reschedule_count', 0)} reschedules
                            </span>
                        </div>
                    </div>
                    <div style="color:#9EA3A8; font-size:0.8rem; margin-top:6px;">
                        {alert.get('message', '')}
                        {f" • Current Job: {alert.get('current_job_id', '')}" if alert.get('current_job_id') else ''}
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        if len(chain_alerts) > 10:
            st.caption(f"...and {len(chain_alerts) - 10} more alerts")
        
        st.markdown("<br>", unsafe_allow_html=True)

    # Active Chains Table
    st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:12px;">
            <div style="width:4px; height:18px; background:#8DC63F; border-radius:2px;"></div>
            <h4 style="margin:0; color:#F0F2F5;">📋 All Active Chains</h4>
        </div>
    """, unsafe_allow_html=True)
    
    if active_chains:
        # Convert to DataFrame for display
        df_chains = pd.DataFrame(active_chains)
        
        # Select and rename columns
        display_cols = {
            'product_serial': 'Serial Number',
            'carrier': 'Carrier',
            'reschedule_count': 'Reschedules',
            'total_delay_days': 'Days Since First Planned',
            'current_status': 'Current Status',
            'current_job_id': 'Current Job ID'
        }
        
        available_cols = {k: v for k, v in display_cols.items() if k in df_chains.columns}
        df_display = df_chains[list(available_cols.keys())].rename(columns=available_cols)
        
        # Sort by reschedule count
        if 'Reschedules' in df_display.columns:
            df_display = df_display.sort_values('Reschedules', ascending=False)
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(df_display)} active product chains")
    else:
        st.info("No active reschedule chains found. Run the daily import to populate chain data.")
    
    # Rescheduled Jobs in Current View
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:12px;">
            <div style="width:4px; height:18px; background:#60A5FA; border-radius:2px;"></div>
            <h4 style="margin:0; color:#F0F2F5;">📅 Rescheduled Jobs in Current View</h4>
        </div>
    """, unsafe_allow_html=True)
    
    # Filter for rescheduled jobs in current view
    if 'Status' in df_filtered.columns:
        rescheduled_mask = df_filtered['Status'].str.lower().str.contains('resched', na=False)
        df_rescheduled = df_filtered[rescheduled_mask].copy()
        
        if not df_rescheduled.empty:
            resched_cols = {
                'Job_ID': 'Job ID',
                'Product_Serial': 'Serial',
                'Carrier': 'Carrier',
                'Status': 'Status',
                'Planned_Date': 'Planned Date',
                'Product_Name': 'Product',
                'State': 'State'
            }
            available_resched = {k: v for k, v in resched_cols.items() if k in df_rescheduled.columns}
            df_resched_display = df_rescheduled[list(available_resched.keys())].rename(columns=available_resched)
            
            st.dataframe(df_resched_display, use_container_width=True, hide_index=True)
            st.caption(f"{len(df_resched_display)} rescheduled jobs in current filter")
        else:
            st.success("✅ No rescheduled jobs in the current view.")
    else:
        st.info("Status data not available for reschedule filtering.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — FULL JOB LIST
# ═══════════════════════════════════════════════════════════════════════════════
with tab_list:

    st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:16px;">
            <div style="width:4px; height:22px; background:#808285; border-radius:2px;"></div>
            <h3 style="margin:0; color:#F0F2F5;">Full Job List</h3>
        </div>
    """, unsafe_allow_html=True)

    df_list = df_filtered.copy()

    def get_status_emoji(row):
        s = str(row.get('Status', '')).lower()
        if s in ['delivered', 'complete', 'completed']: return "🟢 Complete"
        if s in ['manifested', 'created']:               return "⚪ Scheduled"
        driver = str(row.get('Assigned_Driver', '')).strip()
        scan   = str(row.get('Last_Scan_User', '')).strip()
        if driver and driver not in ('nan', 'None', ''):
            return "⚠️ Routed (No Scan)" if (not scan or scan in ('nan', 'None', '')) else "🟡 Routed"
        if scan and scan not in ('nan', 'None', ''):
            return "🔵 Scanned"
        return "⚪ Planned"

    df_list['Visual_Status'] = df_list.apply(get_status_emoji, axis=1)

    target_cols = {
        'Visual_Status':  'Status',
        'Job_ID':         'Job ID',
        'Carrier':        'Carrier',
        'Product_Name':   'Product',
        'Planned_Date':   'Planned',
        'Actual_Date':    'Actual Arrival',
        'Assigned_Driver':'Driver',
        'Last_Scan_User': 'Scanner',
        'State':          'State',
    }
    final_list_cols = {k: v for k, v in target_cols.items() if k in df_list.columns}
    df_list_view    = df_list[list(final_list_cols.keys())].rename(columns=final_list_cols)

    st.caption(f"Showing {len(df_list_view):,} jobs matching current filters.")
    st.dataframe(df_list_view, use_container_width=True, hide_index=True)
