-- ============================================================
-- Job Stage Transitions Table
-- Tracks status changes for dwell-time analysis
-- ============================================================

CREATE TABLE IF NOT EXISTS job_stage_transitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    transitioned_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(job_id, to_status)  -- one transition per status per job
);

-- Indexes for querying transitions
CREATE INDEX IF NOT EXISTS idx_transitions_job ON job_stage_transitions(job_id);
CREATE INDEX IF NOT EXISTS idx_transitions_status ON job_stage_transitions(to_status);
CREATE INDEX IF NOT EXISTS idx_transitions_time ON job_stage_transitions(transitioned_at DESC);

-- View: average dwell time per stage
-- Calculates how long jobs spend in each stage before moving to the next
CREATE OR REPLACE VIEW v_stage_dwell_times AS
SELECT
    t1.to_status AS stage,
    ROUND(AVG(EXTRACT(EPOCH FROM (t2.transitioned_at - t1.transitioned_at)) / 3600)::NUMERIC, 1)
        AS avg_hours_in_stage,
    ROUND(MIN(EXTRACT(EPOCH FROM (t2.transitioned_at - t1.transitioned_at)) / 3600)::NUMERIC, 1)
        AS min_hours,
    ROUND(MAX(EXTRACT(EPOCH FROM (t2.transitioned_at - t1.transitioned_at)) / 3600)::NUMERIC, 1)
        AS max_hours,
    COUNT(*) AS sample_size
FROM job_stage_transitions t1
JOIN job_stage_transitions t2
    ON t1.job_id = t2.job_id
    AND t2.transitioned_at > t1.transitioned_at
WHERE NOT EXISTS (
    -- Ensure t2 is the NEXT transition after t1 (no intermediate transitions)
    SELECT 1 FROM job_stage_transitions t3
    WHERE t3.job_id = t1.job_id
      AND t3.transitioned_at > t1.transitioned_at
      AND t3.transitioned_at < t2.transitioned_at
)
GROUP BY t1.to_status
ORDER BY avg_hours_in_stage DESC;
