# Runbook: Worker Crash Recovery

## Symptoms

- Worker Pod CrashLoopBackOff, OOMKilled, or panic logs.
- Missing job completions while dispatched work exists.
- Alert `WorkerUnavailable` or rising queue depth after restarts.
- Graceful-shutdown timeout metric increments.

## Possible causes

- Panic / nil deref / fatal log.
- OOM from oversized workload.
- Fault injection left enabled (should never happen in production).
- Incomplete work after kill mid-execute (claim-before-completion gap).

## Immediate safety checks

1. Confirm environment is not running with `KERNELQ_FAULTS_ENABLED=true`.
2. Check restart count and last termination reason.
3. Confirm Redis/Kafka health before mass requeue.

## Relevant metrics

- `kernelq:worker_availability`
- `kernelq_recovery_attempts_total` / `success` / `failure`
- `kernelq_graceful_shutdown_timeout_total`
- `kernelq_duplicate_deliveries_total`
- Queue depth / jobs by state

## Relevant structured logs

- `worker shutting down` / `draining in-flight work` / `worker stopped`
- `test fault injected` (test-only; escalate if seen in prod)
- `job execution failed`, `duplicate execution skipped`

## Relevant traces

- Incomplete or errored `worker.execute` / `kafka.process` spans
- Correlate via `trace_id` in logs

## Recovery actions

1. Stabilize workers (fix crash, raise memory, roll back bad image).
2. Allow Kafka redelivery or controlled republish for incomplete jobs.
3. Inspect Redis execution keys for stuck claims (see execution-recovery design).
4. Do not flush all idempotency keys casually.

## Data-correctness checks

- Exactly one business completion per `job_id`+`attempt` after recovery.
- Duplicate deliveries may be >1; duplicate **completions** must be 0 extras.

## Escalation criteria

- Restarts continue >15m, or stuck claims block customer jobs after Redis restore.

## Post-recovery validation

- Workers Ready; consume rate >0; success path logs present; alerts clear.
