# Runbook: Kafka Backlog

## Symptoms

- Alerts `KafkaConsumerStopped` or `QueueBacklogGrowing`.
- Growing `kernelq:queue_depth` or dispatched jobs with no consume rate.
- Consumer group lag rising (when lag metrics are available).

## Likely causes

- Worker Pods down or not Ready.
- Consumer group stuck / misconfigured (`KERNELQ_KAFKA_GROUP_ID`).
- Broker or network issues.
- Severe backpressure (queue full / pause) without resume.

## Immediate checks

### Metrics

- `kernelq:queue_depth`, jobs by state (`queued`, `dispatched`)
- `sum(rate(kernelq_kafka_messages_consumed_total[10m]))`
- `kernelq:worker_availability`
- Publish vs consume rates on the dashboard

### Logs

- Worker lifecycle: starting / ready / shutting down.
- Kafka receive / processing errors (`operation=kafka_receive`, message processing errors without raw payloads).

### Traces

- Absence of new `kafka.process` spans while work is pending supports “consumer stopped”.

## Recovery actions

1. Restore worker availability (see [worker-unavailable.md](worker-unavailable.md)).
2. Confirm consumer process is running and subscribed to `kernelq.jobs.dispatch`.
3. Check backpressure pause/resume signals; restore capacity if paused under load.
4. Avoid deleting topics or resetting offsets in shared environments without coordination.

## Escalation guidance

- Escalate if backlog grows for >30m with workers Ready, or if consume rate stays zero while dispatched > 0.

## Rollback considerations

- If a worker image change stopped consumption, roll back that image.

## Verification after recovery

- Consume rate > 0; queue depth trends down.
- Dispatched count drains toward normal.
- Alerts clear.
