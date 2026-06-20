-- KernelQ migration 003: optional dispatch timestamp on jobs
--
-- Why this column exists:
--   Queue-wait metrics need a durable "when did the scheduler hand this job off?"
--   boundary. ``updated_at`` changes on every state transition, so it cannot
--   represent dispatch time for completed jobs.
--
-- Backward compatibility:
--   Existing rows get NULL; metrics skip queue wait when ``dispatched_at`` is
--   missing. New dispatches set the column via ``mark_job_dispatched`` /
--   ``claim_schedulable_jobs`` (first dispatch only — not overwritten on retry).
--
-- Apply (from repository root):
--   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
--     < control_plane/migrations/003_add_dispatched_at.sql

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ;

COMMENT ON COLUMN jobs.dispatched_at IS
    'When the scheduler first moved this job from queued to dispatched; NULL until dispatch.';
