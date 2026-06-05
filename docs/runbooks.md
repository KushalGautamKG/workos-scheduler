# Runbooks

## High Queue Depth

**Symptoms:**
- Queue depth metric shows thousands of pending jobs
- Worker utilization is at 100%
- New jobs are taking longer to start

**Checks:**
- Check worker count and health status
- Verify broker is processing messages
- Review job execution times (are jobs stuck?)
- Check for resource limits (CPU, memory, concurrency)

**Mitigation:**
- Scale up workers if resources allow
- Check for stuck or slow-running jobs and kill if needed
- Temporarily pause new job enqueuing if system is overwhelmed
- Review and adjust concurrency limits

**Follow-up:**
- Analyze root cause (sudden spike? slow jobs? worker crash?)
- Update capacity planning
- Consider implementing backpressure mechanisms

## P95 Latency Spike

**Symptoms:**
- P95 latency jumps from normal to 10x+ baseline
- Some jobs complete quickly, others are very slow
- User complaints about slow job execution

**Checks:**
- Check database query performance
- Review broker message processing rate
- Check for network issues between components
- Look for specific job types causing slowdowns
- Check worker resource utilization

**Mitigation:**
- Identify and kill slow-running jobs if safe
- Check database indexes and query plans
- Verify broker is not backlogged
- Restart workers if they appear stuck
- Temporarily reduce concurrency to reduce contention

**Follow-up:**
- Profile slow jobs to find bottlenecks
- Optimize database queries or add indexes
- Review and tune worker concurrency settings
- Add more granular latency metrics

## Broker Down

**Symptoms:**
- Workers report "connection refused" or "broker unavailable"
- No jobs are being consumed from queue
- Control plane cannot enqueue new jobs
- Queue depth growing but not decreasing

**Checks:**
- Verify broker process is running
- Check broker health endpoint
- Review broker logs for errors
- Check network connectivity to broker

**Mitigation:**
- Restart broker service
- If broker is on separate host, check host health
- Failover to backup broker if available
- Temporarily pause job enqueuing to prevent queue buildup

**Follow-up:**
- Investigate root cause of broker failure
- Review broker configuration and resource limits
- Consider broker high-availability setup
- Test failover procedures

## Database Slow

**Symptoms:**
- Database query latency spikes
- Control plane API responses are slow
- Workers report slow state updates
- Database connection pool exhausted

**Checks:**
- Check database CPU and memory usage
- Review slow query log
- Check for long-running transactions
- Verify database connection pool settings
- Check for table locks or deadlocks

**Mitigation:**
- Kill long-running queries if safe
- Restart database connections
- Scale up database resources if possible
- Temporarily reduce write frequency
- Enable read replicas if available

**Follow-up:**
- Analyze slow queries and optimize
- Review database indexes
- Consider connection pooling improvements
- Plan database scaling strategy

## Worker Crash Loop

**Symptoms:**
- Workers restarting repeatedly
- High error rate in worker logs
- Jobs failing immediately after starting
- Worker process exits with errors

**Checks:**
- Review worker logs for crash reason
- Check worker resource limits (memory, CPU)
- Verify worker configuration is valid
- Check for dependency failures (broker, database)
- Review recent code deployments

**Mitigation:**
- Stop crashing workers to prevent resource drain
- Roll back recent code changes if applicable
- Fix configuration errors
- Restart with increased resource limits if OOM
- Check for dependency service outages

**Follow-up:**
- Fix root cause (code bug, config error, resource issue)
- Add better error handling and graceful degradation
- Improve worker health checks
- Review monitoring and alerting

## Worker Encounters Invalid Kafka Message

**Symptoms:**
- Worker logs parse or validation errors (malformed JSON, wrong `event_type`, blank `job_id`, invalid `state`)
- Shutdown stats show **`message_errors`** increasing while the process stays up

**Immediate impact:**
- The worker **skips the bad record** and **continues polling**—it does **not** exit on a single bad dispatch message
- Healthy messages on **`kernelq.jobs.dispatch`** can still be processed

**Current behavior:**
- Invalid messages increment **`MessageErrors`** and the worker **keeps polling**
- When **`DeadLetterProducer`** is configured, failures also build **`DeadLetterEvent`** objects (`reason`, original key/value, `source_topic`, `worker`) and call **`PublishDeadLetter`**
- **`DeadLettersPublished`** increases on successful DLQ publish; **`DeadLetterPublishErrors`** increases if publish fails—watch both on shutdown stats
- **Tests** use a **fake DLQ producer** (captures events, no broker); **`cmd/consumer`** does not wire a real producer yet
- **Real publishing to `kernelq.jobs.dlq`** is still coming later—bad records are not on the DLQ topic in production today
- **`kafka.Error`** (broker/connection failures) still **stops** the worker

**What operators should inspect:**
- **`reason`** — why the worker rejected the message (in DLQ events when producer is wired)
- **`original_value`** — raw dispatch payload as received
- **`original_key`** — often `job_id` when the producer set a key
- Shutdown stats from **`cmd/consumer`**: `message_errors` (and `messages_seen`, `messages_processed`); DLQ counters **`DeadLettersPublished`** / **`DeadLetterPublishErrors`** exist in **`ConsumerStats`** when a producer is wired
- Compare payload to **`DispatchEvent`** in `worker/internal/worker/dispatch_event.go`

**Common causes:**
- **Schema drift** — Python publish fields or values no longer match Go validation
- **Manual test messages** — ad hoc JSON on `kernelq.jobs.dispatch` (console producer, old smoke tests)
- **Corrupt producer payloads** — truncated JSON, wrong `event_type`, blank ids, invalid `state`

**Checks:**
- Read the message from **`kernelq.jobs.dispatch`** (console consumer or broker tools)
- Compare JSON to the dispatch contract; check shutdown stats (`message_errors`, DLQ publish counters)
- If **`DeadLetterPublishErrors`** (or publish failures in logs) rises, DLQ routing failed—the message may not reach **`kernelq.jobs.dlq`**
- Search topic history for non-production traffic after incidents

**Follow-up:**
- Fix the publisher or remove bad records on the topic if safe in dev
- Align control-plane publish validation with worker rules
- When real **`kernelq.jobs.dlq`** Kafka publishing lands, monitor DLQ depth and replay only after fixing root cause

**Future improvement:**
- Real Kafka **`DeadLetterProducer`** in **`cmd/consumer`** so events land on **`kernelq.jobs.dlq`** automatically
