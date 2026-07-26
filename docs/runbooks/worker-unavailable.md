# Runbook: Worker Unavailable

## Symptoms

- Alert `WorkerUnavailable` (no Ready worker Pods for 5m).
- `kernelq:worker_availability` ≈ 0.
- Queue/Kafka backlog may grow as a secondary effect.

## Likely causes

- CrashLoopBackOff / failed probes (gRPC health NOT_SERVING).
- Bad image or config.
- Resource limits / OOM.
- Node or scheduling issues.

## Immediate checks

### Metrics

- `kernelq:worker_availability`
- Deployment available vs desired replicas (kube-state-metrics)
- Recent failure / latency spikes

### Logs

- Worker lifecycle JSON: `worker starting`, `worker ready`, `worker shutting down`.
- Crash logs from the worker container (still avoid secrets).

### Traces

- Lack of new `worker.execute` / `kafka.process` spans supports total unavailability.

## Recovery actions

1. Inspect Pod status and recent events (describe / logs) in the local or target cluster.
2. Confirm gRPC health readiness aligns with Day 118 lifecycle expectations.
3. Fix config or roll forward/back the image.
4. Scale only after Pods become Ready — scaling broken Pods does not restore availability.

## Escalation guidance

- Escalate if workers stay unavailable >15m during business-impacting load, or if restarts do not restore Ready.

## Rollback considerations

- Use EKS/local rollback procedures for the last known-good worker image when a deploy caused the outage.

## Verification after recovery

- Available replicas > 0; `kernelq:worker_availability` recovers toward the 99.9% engineering target.
- Consume/execute activity resumes.
- Alert clears after the 5m window.
