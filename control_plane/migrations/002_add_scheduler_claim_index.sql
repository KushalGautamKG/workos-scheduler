-- KernelQ migration 002: scheduler claim / schedulable jobs index
--
-- Why this index exists:
--   Every scheduler tick asks Postgres for the next batch of waiting jobs:
--     WHERE state = 'queued'
--     ORDER BY priority DESC, created_at ASC
--     LIMIT N
--   (and the atomic claim path adds FOR UPDATE SKIP LOCKED on the same shape).
--
--   Migration 001 already has idx_jobs_state and idx_jobs_state_priority.
--   This composite index matches the full filter + sort order so the planner
--   can narrow to queued rows and walk them in dispatch order without sorting
--   a large unrelated set on every tick.
--
-- How it supports the scheduler query:
--   - Leading column `state` matches the WHERE clause (queued-only work queue).
--   - `priority DESC` matches "urgent jobs first".
--   - `created_at ASC` matches FIFO tie-break among equal priority.
--   Together they align with list_schedulable_jobs and claim_schedulable_jobs.
--
-- Why ordering direction matters:
--   B-tree indexes can be scanned forward or backward, but defining DESC/ASC
--   to match the query's ORDER BY helps Postgres use the index in the same
--   direction the scheduler expects (high priority first, older jobs first
--   when priority ties). That reduces extra sort steps as the jobs table grows.
--
-- Apply (from repository root):
--   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
--     < control_plane/migrations/002_add_scheduler_claim_index.sql
--
-- Re-run EXPLAIN after applying to compare plans (see control_plane/sql/
-- explain_claim_schedulable_jobs.sql and docs/perf.md).

CREATE INDEX IF NOT EXISTS idx_jobs_state_priority_created_at
    ON jobs (state, priority DESC, created_at ASC);

COMMENT ON INDEX idx_jobs_state_priority_created_at IS
    'Supports scheduler claim/schedulable queries: queued rows by priority DESC, created_at ASC.';
