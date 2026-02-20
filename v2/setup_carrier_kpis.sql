-- ============================================================
-- Carrier-Level KPI History Table
-- Stores per-carrier KPI breakdowns for trend comparison
-- ============================================================

CREATE TABLE IF NOT EXISTS kpi_carrier_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_date DATE NOT NULL,
    carrier TEXT NOT NULL,
    on_time_pct NUMERIC,
    avg_delay_days NUMERIC,
    total_jobs INTEGER,
    overdue_count INTEGER,
    ready_for_routing INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(report_date, carrier)
);

-- Indexes for querying by date range or carrier
CREATE INDEX IF NOT EXISTS idx_kpi_carrier_date ON kpi_carrier_history(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_kpi_carrier_carrier ON kpi_carrier_history(carrier);
