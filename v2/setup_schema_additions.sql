-- ============================================================
-- Schema Additions — Run in Supabase SQL Editor
-- Adds new fields from the fieldtest2.xlsx export format.
-- Safe to run on an existing database (uses ALTER TABLE IF NOT EXISTS pattern).
-- ============================================================

-- ── job_snapshots ────────────────────────────────────────────
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS market TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS customer_name TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS delivery_address TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS date_received DATE;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS job_created_at TIMESTAMP;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS client_order_number TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS prior_job_id TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS signed_by TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS delivery_scan_count INTEGER DEFAULT 0;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS product_weight_lbs INTEGER DEFAULT 0;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS crew_required INTEGER DEFAULT 1;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS driver_notes TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS job_type TEXT;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS arrival_time TIMESTAMP;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS dwell_minutes NUMERIC;
ALTER TABLE job_snapshots ADD COLUMN IF NOT EXISTS lead_time_days INTEGER;

-- ── job_history ──────────────────────────────────────────────
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS market TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS customer_name TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS delivery_address TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS date_received DATE;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS job_created_at TIMESTAMP;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS client_order_number TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS prior_job_id TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS signed_by TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS delivery_scan_count INTEGER DEFAULT 0;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS product_weight_lbs INTEGER DEFAULT 0;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS crew_required INTEGER DEFAULT 1;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS driver_notes TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS job_type TEXT;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS arrival_time TIMESTAMP;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS dwell_minutes NUMERIC;
ALTER TABLE job_history ADD COLUMN IF NOT EXISTS lead_time_days INTEGER;

-- ── New indexes for the most-queried new columns ─────────────
CREATE INDEX IF NOT EXISTS idx_job_snapshots_market ON job_snapshots(market);
CREATE INDEX IF NOT EXISTS idx_job_snapshots_driver ON job_snapshots(scan_user);
CREATE INDEX IF NOT EXISTS idx_job_history_market ON job_history(market);
CREATE INDEX IF NOT EXISTS idx_job_history_customer ON job_history(customer_name);
CREATE INDEX IF NOT EXISTS idx_job_history_prior_job ON job_history(prior_job_id);
