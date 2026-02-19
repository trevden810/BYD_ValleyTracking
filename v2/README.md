# BYD/Valley Tracking V2.0

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
pip install -r v2/requirements_v2.txt
```

### 2. Configure Supabase
- Follow instructions in `v2/SUPABASE_SETUP.md`
- Add your credentials to `.env`
- **NEW:** Run `v2/setup_job_chains.sql` for reschedule tracking

### 3. Configure Email
Update `.env` with:
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`
- `EMAIL_RECIPIENTS` (comma-separated list)

### 4. Test the System (Dry Run)
```bash
python v2/main.py --dry-run
```
This will:
- Process `scanfieldtest.xlsx`
- Calculate KPIs
- Generate HTML report (saved as `temp_report_preview.html`)
- **NOT** send email or store in Supabase

### 5. Run Full Pipeline
```bash
python v2/main.py
```

### 6. Run with Custom File
```bash
python v2/main.py --file path/to/your/export.xlsx
```

## Features

### ✅ Data Processing
- Imports 169-field manual exports
- Parses scan validation JSON
- Calculates KPIs (on-time %, delay averages, etc.)

### ✅ Historical Tracking
- Stores snapshots in Supabase
- Tracks KPI trends over time
- Provides trend indicators (↑↓→)

### ✅ Job Chain Tracking (NEW)
- Tracks rescheduled jobs by product serial number
- Alerts for products with 3+ reschedules
- Dashboard tab for reschedule monitoring
- See `v2/JOB_CHAINS_GUIDE.md` for details

### ✅ Email Reports
- Professional HTML design
- Embedded charts (funnel, timeline)
- Late arrivals watchlist
- Responsive mobile layout

## Scheduling

### Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily at 6:00 AM
4. Action: Start a program
5. Program: `python`
6. Arguments: `C:\Projects\BYD_ValleyTracking\v2\main.py`
7. Start in: `C:\Projects\BYD_ValleyTracking`

## Troubleshooting

### "Supabase URL not set"
- Run `v2/SUPABASE_SETUP.md` SQL in Supabase
- Add credentials to `.env`

### "Email send failed"
- Check SMTP credentials are correct
- For Gmail, enable "Less secure app access" or use App Password

### "Export file not found"
- Update `EXPORT_FILE_PATH` in `.env`
- Or use `--file` argument

### "No active reschedule chains found"
- Run `v2/setup_job_chains.sql` in Supabase
- Run daily import to populate chain data

## Architecture

```
v2/
├── main.py                    # Orchestrator
├── data_processor.py          # Excel import & processing
├── supabase_client.py         # Database integration
├── email_generator.py         # Report generation
├── job_chains.py              # Reschedule tracking (NEW)
├── comparator.py              # Delta detection
├── templates/
│   └── email_report.html      # Email template
├── setup_supabase.sql         # Core database schema
├── setup_job_chains.sql       # Chain tracking schema (NEW)
├── SUPABASE_SETUP.md          # Database setup guide
└── JOB_CHAINS_GUIDE.md        # Reschedule tracking guide (NEW)
```
