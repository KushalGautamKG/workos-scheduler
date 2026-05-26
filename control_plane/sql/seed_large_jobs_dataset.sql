-- =============================================================================
-- seed_large_jobs_dataset.sql
-- =============================================================================
--
-- Why this seed exists:
--   Day 23–24 EXPLAIN on a handful of rows often showed Seq Scan because Postgres
--   optimizes for tiny tables. This script inserts ~5000 synthetic jobs so you
--   can inspect scheduler query plans at a more realistic local scale.
--
-- Why larger datasets affect query planning:
--   The planner uses statistics and cost estimates. When most rows are NOT queued
--   (succeeded, failed, etc.), a sequential scan becomes expensive. Indexes like
--   idx_jobs_state_priority_created_at are more likely to help—or at least show
--   up differently in EXPLAIN—when the table is bigger than a demo insert.
--
-- Local benchmarking only:
--   All rows use tenant_id prefix 'seed-tenant-%' and job_id prefix 'seed-job-'.
--   Do NOT run this against production. Clean up with the DELETE at the top.
--
-- How to run (from repository root, Postgres up, migration 001 applied):
--
--   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
--     < control_plane/sql/seed_large_jobs_dataset.sql
--
-- Then rerun EXPLAIN:
--
--   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
--     < control_plane/sql/explain_claim_schedulable_jobs.sql
--
-- State note: KernelQ stores lowercase lifecycle strings ('queued', not 'QUEUED').
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Cleanup: remove any previous seed run (safe to re-run this script)
-- -----------------------------------------------------------------------------

DELETE FROM jobs
WHERE tenant_id LIKE 'seed-tenant-%';

-- -----------------------------------------------------------------------------
-- Insert ~5000 synthetic jobs with deterministic patterns
-- -----------------------------------------------------------------------------
-- generate_series(1, 5000) gives us one row per job number i.
-- Patterns (no randomness):
--   tenant_id  -> seed-tenant-1 .. seed-tenant-10  (i % 10)
--   priority   -> 0 .. 19                          (i % 20)
--   state      -> mostly queued, some dispatched / succeeded / failed (i % 100)
--   created_at -> staggered minutes in the past     (i minutes ago)

INSERT INTO jobs (
    job_id,
    tenant_id,
    priority,
    state,
    payload,
    created_at,
    updated_at
)
SELECT
    'seed-job-' || gs.i AS job_id,
    'seed-tenant-' || ((gs.i % 10) + 1) AS tenant_id,
    (gs.i % 20) AS priority,
    CASE
        WHEN gs.i % 100 < 70 THEN 'queued'       -- 70% waiting in queue
        WHEN gs.i % 100 < 80 THEN 'dispatched'   -- 10% already claimed
        WHEN gs.i % 100 < 95 THEN 'succeeded'    -- 15% finished
        ELSE 'failed'                            --  5% failed
    END AS state,
    '{}'::jsonb AS payload,
    NOW() - (gs.i * INTERVAL '1 minute') AS created_at,
    NOW() - (gs.i * INTERVAL '1 minute') AS updated_at
FROM generate_series(1, 5000) AS gs(i);

-- -----------------------------------------------------------------------------
-- Quick sanity check (optional): row counts by state for seed data only
-- -----------------------------------------------------------------------------

SELECT state, COUNT(*) AS job_count
FROM jobs
WHERE tenant_id LIKE 'seed-tenant-%'
GROUP BY state
ORDER BY state;
