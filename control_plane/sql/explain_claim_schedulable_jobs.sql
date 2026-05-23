-- =============================================================================
-- explain_claim_schedulable_jobs.sql
-- =============================================================================
--
-- What this file is for:
--   Run EXPLAIN plans against KernelQ's scheduler queries on local Postgres.
--   Use this to see whether Postgres uses indexes or scans the whole jobs table.
--
-- How to run (from the repository root):
--
--   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
--     < control_plane/sql/explain_claim_schedulable_jobs.sql
--
-- Or open psql and paste sections one at a time.
--
-- State column note:
--   The lifecycle state is called QUEUED in docs, but Postgres stores lowercase
--   strings from JobState (value 'queued'). Queries below use 'queued'.
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Optional setup: a few sample rows for a realistic plan
-- -----------------------------------------------------------------------------
-- Safe to re-run: removes prior sample rows first, then inserts fresh ones.

DELETE FROM jobs
WHERE job_id LIKE 'explain_sample_%';

INSERT INTO jobs (job_id, tenant_id, priority, state, payload)
VALUES
    ('explain_sample_low_old', 'tenant-demo', 1, 'queued', '{}'::jsonb),
    ('explain_sample_high_new', 'tenant-demo', 10, 'queued', '{}'::jsonb),
    ('explain_sample_high_old', 'tenant-demo', 10, 'queued', '{}'::jsonb),
    ('explain_sample_not_queued', 'tenant-demo', 99, 'succeeded', '{}'::jsonb);

-- Stagger created_at so tie-break ordering is visible in results (optional).
UPDATE jobs SET created_at = NOW() - INTERVAL '3 minutes'
WHERE job_id = 'explain_sample_low_old';
UPDATE jobs SET created_at = NOW() - INTERVAL '2 minutes'
WHERE job_id = 'explain_sample_high_new';
UPDATE jobs SET created_at = NOW() - INTERVAL '1 minute'
WHERE job_id = 'explain_sample_high_old';

-- -----------------------------------------------------------------------------
-- 1) Schedulable job query (read-only shape)
-- -----------------------------------------------------------------------------
-- This matches what list_schedulable_jobs asks for: queued rows, priority then age.

EXPLAIN
SELECT job_id, tenant_id, priority, state, created_at
FROM jobs
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 2) Same query with EXPLAIN ANALYZE
-- -----------------------------------------------------------------------------
-- IMPORTANT: EXPLAIN ANALYZE actually RUNS the query (read-only here, but it
-- still executes and returns real timing/row counts). Use on dev/staging first.

EXPLAIN ANALYZE
SELECT job_id, tenant_id, priority, state, created_at
FROM jobs
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 3) Atomic claim pattern: row lock + skip locked (read-only EXPLAIN)
-- -----------------------------------------------------------------------------
-- claim_schedulable_jobs wraps an UPDATE around this subquery. We EXPLAIN the
-- SELECT alone to study locking and ordering without mutating rows.
--
-- FOR UPDATE       = lock selected rows for this transaction (claim intent).
-- SKIP LOCKED      = if another scheduler already locked a row, skip it.

EXPLAIN
SELECT job_id
FROM jobs
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT 10
FOR UPDATE SKIP LOCKED;

-- -----------------------------------------------------------------------------
-- Cleanup (optional): remove sample rows when you are done inspecting plans
-- -----------------------------------------------------------------------------
-- DELETE FROM jobs WHERE job_id LIKE 'explain_sample_%';
