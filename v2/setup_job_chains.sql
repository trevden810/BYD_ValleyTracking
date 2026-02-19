-- ============================================================
-- Job Chain Tracking Schema
-- Tracks rescheduled jobs by linking them via product serial number
-- ============================================================

-- 1. Job Chains: Groups jobs by product serial number
-- Each unique product_serial gets one chain record
CREATE TABLE IF NOT EXISTS job_chains (
    chain_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_serial TEXT NOT NULL UNIQUE,
    carrier TEXT,
    total_jobs INTEGER DEFAULT 1,
    reschedule_count INTEGER DEFAULT 0,
    first_planned_date DATE,
    final_planned_date DATE,
    total_delay_days INTEGER DEFAULT 0,
    current_status TEXT,
    current_job_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 2. Chain Links: Individual jobs within a chain
-- Links each job to its chain with sequence ordering
CREATE TABLE IF NOT EXISTS job_chain_links (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id UUID REFERENCES job_chains(chain_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL,
    sequence_order INTEGER DEFAULT 1,
    status TEXT,
    planned_date DATE,
    actual_date TIMESTAMP,
    delay_days INTEGER,
    reschedule_reason TEXT,
    linked_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain_id, job_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_job_chains_serial ON job_chains(product_serial);
CREATE INDEX IF NOT EXISTS idx_job_chains_carrier ON job_chains(carrier);
CREATE INDEX IF NOT EXISTS idx_job_chains_reschedule_count ON job_chains(reschedule_count DESC);
CREATE INDEX IF NOT EXISTS idx_job_chain_links_chain ON job_chain_links(chain_id);
CREATE INDEX IF NOT EXISTS idx_job_chain_links_job ON job_chain_links(job_id);
CREATE INDEX IF NOT EXISTS idx_job_chain_links_status ON job_chain_links(status);

-- 3. Function to update chain timestamps automatically
CREATE OR REPLACE FUNCTION update_chain_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto-updating timestamps
DROP TRIGGER IF EXISTS trigger_update_chain_timestamp ON job_chains;
CREATE TRIGGER trigger_update_chain_timestamp
    BEFORE UPDATE ON job_chains
    FOR EACH ROW
    EXECUTE FUNCTION update_chain_timestamp();

-- 4. View for active chains with reschedule history
CREATE OR REPLACE VIEW v_active_chains AS
SELECT 
    jc.chain_id,
    jc.product_serial,
    jc.carrier,
    jc.total_jobs,
    jc.reschedule_count,
    jc.first_planned_date,
    jc.final_planned_date,
    jc.total_delay_days,
    jc.current_status,
    jc.current_job_id,
    jc.updated_at,
    CASE 
        WHEN jc.reschedule_count >= 3 THEN 'critical'
        WHEN jc.reschedule_count >= 2 THEN 'warning'
        ELSE 'normal'
    END AS alert_level
FROM job_chains jc
WHERE jc.current_status NOT IN ('Delivered', 'Complete', 'Completed')
ORDER BY jc.reschedule_count DESC, jc.total_delay_days DESC;

-- 5. View for chain details with all linked jobs
CREATE OR REPLACE VIEW v_chain_details AS
SELECT 
    jc.chain_id,
    jc.product_serial,
    jc.carrier,
    jc.reschedule_count,
    jcl.job_id,
    jcl.sequence_order,
    jcl.status,
    jcl.planned_date,
    jcl.actual_date,
    jcl.delay_days,
    jcl.reschedule_reason
FROM job_chains jc
JOIN job_chain_links jcl ON jc.chain_id = jcl.chain_id
ORDER BY jc.product_serial, jcl.sequence_order;
