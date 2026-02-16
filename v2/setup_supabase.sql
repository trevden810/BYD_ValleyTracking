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
