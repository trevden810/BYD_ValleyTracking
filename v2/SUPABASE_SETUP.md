# Supabase Database Schema Setup

## Quick Setup

### Option 1: Use the SQL File (Easiest)
1. Go to your Supabase project: https://fykubdyvvjloicfhqmvg.supabase.co
2. Navigate to **SQL Editor** (left sidebar)
3. Open `v2/setup_supabase.sql` in a text editor
4. Copy **ALL** the contents
5. Paste into Supabase SQL Editor
6. Click **"Run"**

### Option 2: Copy SQL Below
Copy only the SQL commands below (⚠️ **DO NOT** copy the \`\`\`sql lines):

**START COPYING FROM HERE:**

```sql
-- Job Snapshots Table
CREATE TABLE IF NOT EXISTS job_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_date TIMESTAMP NOT NULL,
    job_id TEXT NOT NULL,
    planned_date DATE,
    actual_date TIMESTAMP,
    delay_days NUMERIC,
    status TEXT,
    carrier TEXT,
    market TEXT,
    scan_user TEXT,
    scan_timestamp TIMESTAMP,
    product_description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_job_snapshots_date ON job_snapshots(snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_job_snapshots_job_id ON job_snapshots(job_id);

-- KPI History Table
CREATE TABLE IF NOT EXISTS kpi_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_date DATE NOT NULL UNIQUE,
    on_time_pct NUMERIC,
    avg_delay_days NUMERIC,
    total_jobs INTEGER,
    overdue_count INTEGER,
    ready_for_routing INTEGER,
    avg_scans_per_job NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for time series queries
CREATE INDEX IF NOT EXISTS idx_kpi_history_date ON kpi_history(report_date DESC);
```

**STOP COPYING HERE**

## Verification

After running the SQL, verify the tables were created:
1. Go to **Table Editor** in Supabase
2. You should see two new tables:
   - `job_snapshots`
   - `kpi_history`

## Next Steps

Once tables are created, update `.env` with your credentials (✅ Already done):
```env
SUPABASE_URL=https://fykubdyvvjloicfhqmvg.supabase.co
SUPABASE_KEY=your-anon-key
```

