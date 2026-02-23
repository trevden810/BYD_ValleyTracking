-- ============================================================
-- V2.0 Flag Engine Schema
-- Run in Supabase SQL Editor. Safe on existing databases.
-- ============================================================

-- ── 1. Computed flag columns on job_snapshots ─────────────────
ALTER TABLE job_snapshots
  ADD COLUMN IF NOT EXISTS computed_flag    TEXT         DEFAULT 'none',
  ADD COLUMN IF NOT EXISTS flag_reason      TEXT         DEFAULT '',
  ADD COLUMN IF NOT EXISTS sla_hours_elapsed NUMERIC,
  ADD COLUMN IF NOT EXISTS sla_breach_level TEXT         DEFAULT 'n/a',
  ADD COLUMN IF NOT EXISTS is_pepmove_leg   BOOLEAN      DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS flag_set_at      TIMESTAMP;   -- when this flag was first raised

-- ── 2. flag_history — audit trail for every flag change ───────
--    Lets you prove, after the fact, when a breach occurred.
CREATE TABLE IF NOT EXISTS flag_history (
  id                BIGSERIAL   PRIMARY KEY,
  job_id            TEXT        NOT NULL,
  product_serial    TEXT,
  flag_from         TEXT,                          -- previous flag level
  flag_to           TEXT        NOT NULL,           -- new flag level
  flag_reason       TEXT,
  sla_hours_elapsed NUMERIC,
  is_pepmove_leg    BOOLEAN     DEFAULT FALSE,
  changed_at        TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flag_history_job_id
  ON flag_history (job_id);

CREATE INDEX IF NOT EXISTS idx_flag_history_changed_at
  ON flag_history (changed_at DESC);

-- ── 3. reschedule_watch — persistent tiles for re-scheduled jobs ──
--    A row is created when a job is Re-scheduled.
--    It is resolved (resolved_at filled in) when a new job with
--    the same product_serial reaches Entered or Scheduled status.
CREATE TABLE IF NOT EXISTS reschedule_watch (
  id                BIGSERIAL   PRIMARY KEY,
  product_serial    TEXT        NOT NULL UNIQUE,
  original_job_id   TEXT,
  carrier           TEXT,
  rescheduled_at    DATE,
  days_watching     INTEGER     DEFAULT 0,   -- incremented on each daily import
  resolved_by_job_id TEXT,
  resolved_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reschedule_watch_serial
  ON reschedule_watch (product_serial);

CREATE INDEX IF NOT EXISTS idx_reschedule_watch_unresolved
  ON reschedule_watch (resolved_at)
  WHERE resolved_at IS NULL;            -- efficient filter for active tiles

-- ── 4. Helpful indexes for flag queries on job_snapshots ──────
CREATE INDEX IF NOT EXISTS idx_job_snapshots_computed_flag
  ON job_snapshots (computed_flag);

CREATE INDEX IF NOT EXISTS idx_job_snapshots_is_pepmove
  ON job_snapshots (is_pepmove_leg)
  WHERE is_pepmove_leg = TRUE;
