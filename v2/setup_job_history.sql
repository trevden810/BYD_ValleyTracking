-- ============================================================
-- Job History Table
-- Archives completed/delivered jobs for long-term analytics
-- ============================================================

CREATE TABLE IF NOT EXISTS job_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id TEXT NOT NULL,
    carrier TEXT,
    state TEXT,
    planned_date DATE,
    actual_date TIMESTAMP,
    delay_days NUMERIC,
    status TEXT,
    product_description TEXT,
    product_serial TEXT,
    piece_count INTEGER DEFAULT 0,
    white_glove BOOLEAN DEFAULT FALSE,
    scan_user TEXT,
    scan_timestamp TIMESTAMP,
    notification_detail TEXT,
    miles_oneway NUMERIC,
    completed_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(job_id)  -- one row per job, upsert on re-runs
);

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_job_history_carrier ON job_history(carrier);
CREATE INDEX IF NOT EXISTS idx_job_history_completed ON job_history(completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_planned ON job_history(planned_date);
CREATE INDEX IF NOT EXISTS idx_job_history_status ON job_history(status);
