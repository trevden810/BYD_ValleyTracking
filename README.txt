"BYD/Valley Job Tracking Dashboard - Development Roadmap & Requirements"). It's written in Markdown for easy readability on GitHub, Notion, or Google Docs. It provides clear context, goals, data needs, functional requirements, suggested tech stack, UI/UX guidelines, and phased roadmap so Google Antigravity's developer can hit the ground running.
Feel free to tweak names, add your FM server details (don't share credentials here), or expand sections before sharing.
Markdown# BYD/Valley Job Tracking Dashboard  
**Streamlit Application Requirements & Roadmap**  
Prepared for: Google Antigravity Development Team  
Prepared by: Oglaigh  
Date: February 2026  

Blank github
https://github.com/trevden810/BYD_ValleyTracking.git
## 1. Project Overview

We manage deliveries for BYD/Valley (furniture/appliance white-glove and drop-and-go). Data lives in FileMaker (FM) with a REST API (Data API) exposing job records.

Current pain points:  
- Late deliveries (e.g., Valley often 5–10+ days late)  
- Inconsistent routing & rescheduling  
- Poor visibility into delays, confirmations, and customer notes/hazards  
- Manual tracking of statuses (planned vs. actual, scanned, routed, confirmed, delivered)  

Goal: Build a modern, interactive **web dashboard** in Streamlit to give Ops, CSRs, and managers real-time visibility into BYD/Valley jobs. Replace ad-hoc Python scripts and manual checks.

Key value:  
- Faster delay detection & follow-up  
- Better routing decisions  
- Improved customer confirmation tracking  
- Historical trends to negotiate with carriers  

## 2. Data Source: FileMaker REST API

- **API Version**: Latest supported (vLatest or v1)  
- **Base URL**: `https://[your-fm-server]/fmi/data/vLatest/databases/[your-database]/layouts/[relevant-layout]` (e.g., "Jobs" or "Deliveries")  
- **Authentication**: Bearer token (session-based)  
  - POST to `/sessions` with Basic Auth (username/password or API user) → returns token  
  - Use token in `Authorization: Bearer {token}` for all subsequent calls  
  - Tokens expire after inactivity (~15 min); implement refresh/re-auth logic  
- **Key Tables/Layouts** (query these via `/records`, `/_find`, `/record/{id}`):  
  - Jobs / Deliveries layout with fields like:  
    - BOL_Number (string)  
    - Market (string)  
    - BYD_Flag or Carrier (string, filter "BYD" or "Valley")  
    - Planned_Date (date/datetime)  
    - Actual_Date (date/datetime) → scanned/arrival date  
    - Status (string: e.g., Imported, Scanned, Routed, Confirmed, Rescheduled, Delivered, Complete)  
    - Delay_Days (calculated or derived)  
    - Customer_Notes / Special_Requests (text: drop location, hazards like dogs/uneven ground)  
    - Ops_Manager (string)  
    - Route_Info (text)  
    - Confirmation_Status (string: Pending, Called, Text Sent, Confirmed, No Response)  
    - Reschedule_Count (number)  
    - Last_Updated (timestamp)  

Use `/find` endpoint for filtered queries (e.g., recent jobs, late jobs).  
Example Python snippet for auth & fetch (adapt as needed):  
```python
import requests
from requests.auth import HTTPBasicAuth

def get_token(base_url, fm_user, fm_pass):
    resp = requests.post(f"{base_url}/sessions", auth=HTTPBasicAuth(fm_user, fm_pass))
    return resp.json()["response"]["token"]

def fetch_jobs(base_url, token, query_payload):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(f"{base_url}/_find", json=query_payload, headers=headers)
    return resp.json()["response"]["data"]
Security Note: Never hard-code credentials. Use Streamlit secrets management (st.secrets) or environment variables.
3. Functional Requirements
Core Features

Authentication / Access (optional phase 1): Simple login (username/password → FM token) or API key if FM supports it.
Dashboard Home
KPI cards: On-time %, Avg Delay (days), Late Jobs (>3 days), Pending Confirmations, Reschedules (last 30 days)
Filters: Date range (default last 30–90 days), Market, Status, Carrier (BYD/Valley), Ops Manager

Job List / Table (sortable, filterable, paginated)
Columns: BOL #, Planned Date, Actual Date, Delay Days (color-coded: green ≤0, yellow 1–3, red >3), Status, Market, Notes (truncated + tooltip), Actions (link to detail?)

Visualizations
Bar/line chart: Delays over time
Gantt/timeline: Planned vs Actual for selected jobs
Heatmap: Lateness by Market or Ops Manager
Pie: Status breakdown
Use Plotly or Altair for interactivity

Alerts / Highlights
Auto-highlight late jobs
Optional: Email/Slack notifications for new delays > threshold (via smtplib or external service)

Detail View (click row → expander or new page)
Full notes, hazards, reschedule history, confirmation log


Nice-to-Have (Phase 2+)

Export CSV/PDF of filtered jobs
Trend reports (monthly on-time %)
Manual refresh button + auto-poll (every 10–15 min)
Integration hooks: Button to trigger FM script (e.g., send delay email to Valley)

4. Tech Stack Recommendation

Framework: Streamlit (latest stable)
Data Handling: pandas, requests
Visualization: Plotly (interactive) + Altair (if preferred)
Caching: @st.cache_data (ttl=600–900s) for API calls
Layout: st.columns, st.tabs, st.expander, st.sidebar for filters
Deployment: Streamlit Community Cloud (free tier), or self-host on AWS/EC2/Docker
Secrets: st.secrets.toml for FM credentials
Optional: streamlit-authenticator (simple login), plotly.subplots for complex charts

Best Practices to Follow

Modular structure: app.py (main), utils/api.py, utils/data.py, pages/ for multi-page if needed
Cache expensive calls
Handle API errors gracefully (retry, show user message)
Minimize reruns with session_state for filters
Mobile-responsive by default (Streamlit handles most)

5. Development Roadmap (Phased)
Phase 1 – MVP (1–2 weeks)

FM API connection & auth
Fetch & cache recent BYD/Valley jobs
Basic dashboard: KPI cards + filterable table
Delay calculation & color-coding
Simple bar chart of delays

Phase 2 – Core Visuals & Usability (1–2 weeks)

Add visualizations (Gantt, heatmap, status pie)
Sidebar filters + date picker
Job detail expander
Alerts/highlights for late/pending jobs

Phase 3 – Polish & Extras (1 week)

Auto-refresh toggle
Export functionality
Error handling & loading spinners
Basic auth if multi-user
Deploy to Streamlit Cloud + share link

Phase 4 – Future (post-launch)

Notifications
Historical trends
FM script triggers
Mobile optimizations

6. Success Criteria

Loads <5 seconds (with caching)
Accurate delay calc & status mapping
Filters work intuitively
Team feedback: "This saves me hours/week checking statuses"

7. Next Steps for Developer

Confirm FM API access (test auth & sample query in Postman)
Request sample anonymized data export (JSON/CSV) for prototyping
Set up repo (GitHub private?)
Share early prototype link via Streamlit Cloud