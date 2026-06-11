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
- When **`DeadLetterProducer`** is configured (wired in **`cmd/consumer`**), failures are routed to **`kernelq.jobs.dlq`** as **`DeadLetterEvent`** JSON (`reason`, `original_key`, `original_value`, `source_topic`, `worker`)
- **`DeadLettersPublished`** increases on successful DLQ publish; **`DeadLetterPublishErrors`** increases if publish fails
- **`kafka.Error`** (broker/connection failures) still **stops** the worker

**What operators should inspect:**
- Consume **`kernelq.jobs.dlq`** to read **`reason`** and **`original_value`** (raw dispatch payload as received)
- **`original_key`** — often `job_id` when the producer set a key
- Shutdown stats from **`cmd/consumer`**: `message_errors`; **`DeadLettersPublished`** / **`DeadLetterPublishErrors`** in **`ConsumerStats`**
- Compare payload to **`DispatchEvent`** in `worker/internal/worker/dispatch_event.go`

**Inspect DLQ on Kafka (local):**

```bash
docker exec -i kernelq-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.dlq \
  --from-beginning \
  --max-messages 5
```

**Common causes:**
- **Schema drift** — Python publish fields or values no longer match Go validation
- **Manual test messages** — ad hoc JSON on `kernelq.jobs.dispatch` (console producer, old smoke tests)
- **Corrupt producer payloads** — truncated JSON, wrong `event_type`, blank ids, invalid `state`

**Checks:**
- Read the failed record from **`kernelq.jobs.dlq`** (preferred) or the original offset on **`kernelq.jobs.dispatch`**
- Compare JSON to the dispatch contract; check shutdown stats (`message_errors`, **`DeadLetterPublishErrors`**)
- If **`DeadLetterPublishErrors`** rises, DLQ routing failed—check **Kafka producer connectivity** (`localhost:9092` from worker host) and confirm **`kernelq.jobs.dlq`** exists (`./infra/kafka/create-topics.sh`)
- Search topic history for non-production traffic after incidents

**Follow-up:**
- Fix the publisher or remove bad records on the topic if safe in dev
- Align control-plane publish validation with worker rules
- Monitor DLQ depth; replay only after fixing root cause

## Worker Reports Retryable Failure

**What it means:**

A **`retryable_failure`** **`ExecutionResult`** means the job attempt failed, but **retrying later may succeed**. These are **temporary execution issues**—not poison Kafka messages and not permanent job failures.

**Examples:**

- **Transient dependency outage** — downstream API or database briefly unavailable
- **Temporary network issue** — connection reset or timeout between worker and dependency
- **Rate limiting** — upstream returned 429 or throttled the worker; backoff may help

**Current behavior:**

- Workers classify this outcome in **`ExecutionResult`** (`status: retryable_failure`, optional **`message`**).
- **Automatic retry workflows are not wired yet**—no publish to **`kernelq.jobs.retry`**, no Postgres **`failed → retry_scheduled`** from worker reports today.

**Future behavior:**

- Scheduler/retry path will use **`retryable_failure`** to trigger **`failed → retry_scheduled → queued`**, honor **`retry_count` / `max_retries`**, and re-publish to **`kernelq.jobs.retry`**.

**What operators should do today:**

- Treat rising retryable failures as **dependency or capacity signals**—check downstream health, network, and rate limits before jobs stall or exhaust retries.
- Distinguish from **invalid dispatch messages** (DLQ on **`kernelq.jobs.dlq`**) and **terminal failures** (no auto-retry).

## Worker Result Event Missing

**Symptoms:**

- Job stays **`dispatched`** or **`running`** in Postgres with **no terminal update** (`succeeded`, `failed`, `dead_lettered`, `canceled`)
- Dispatch message was consumed but lifecycle never closed the loop

**Possible causes:**

- **Worker crashed before publishing result** — execution started or finished in-process but no **`WorkerResultEvent`** reached **`kernelq.jobs.results`**
- **Result topic unavailable** — **`kernelq.jobs.results`** missing or broker unreachable from worker (`./infra/kafka/create-topics.sh`)
- **Result consumer not running** — Python control plane is not yet consuming results (expected today)
- **Invalid result event rejected** — malformed JSON or bad **`status`** / **`event_type`** dropped by future consumer validation

**Checks:**

- Run **`./worker/scripts/smoke_worker_result.sh`** (from repo root) to verify worker-side result publishing end to end
- Run **`PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py`** to poll **one** result message and update Postgres
  - **`poll_result: processed_message=false`** — no message on **`kernelq.jobs.results`** before the timeout (produce a result first, or increase wait)
  - **Message processed but state unchanged** — check **`job_id` exists in Postgres** and **`ResultStateHandler`** mapping (**`succeeded`** → **`succeeded`**; failures → **`failed`** today)
- If the smoke test fails, inspect:
  - **Worker logs** — `/tmp/kernelq-worker-smoke.log` (script output) or your running consumer process
  - **`kernelq.jobs.dispatch`** — dispatch message present and valid JSON
  - **`kernelq.jobs.results`** — result event with matching **`job_id`**
  - **Kafka connectivity** — broker up (`docker compose up -d kafka zookeeper`), topics exist (`./infra/kafka/create-topics.sh`), worker reaches **`localhost:9092`**
- Confirm worker process was up when the job was dispatched
- Inspect **`kernelq.jobs.results`** for a record with matching **`job_id`**
- If result events are missing, check:
  - **`ResultProducer` wiring** — **`cmd/consumer`** passes **`KafkaResultProducer`** into **`DispatchEventHandler`**
  - **`kernelq.jobs.results` topic exists** — `./infra/kafka/create-topics.sh`
  - **Kafka producer connectivity** — worker can reach broker (`localhost:9092`)
  - **Handler publish errors** — **`PublishResult`** failure returns error from **`Handle`** (may increment **`message_errors`**)
- Compare worker logs and shutdown stats with **`kernelq.jobs.dispatch`** / DLQ traffic

**Current status:**

- **Worker handler publishes result events** when **`ResultProducer`** is configured (**`DispatchEventHandler`** → **`PublishResult`** after **`Execute`**)
- **`KafkaResultProducer`** wired in **`cmd/consumer`** with **`WorkerName: kernelq-go-worker`**
- **Python result event parser exists** — **`control_plane/kernelq/result_event.py`** validates **`WorkerResultEvent`** JSON (including allowed **`status`** values)
- **`ResultConsumerRunner` exists** (`control_plane/kernelq/result_consumer.py`) — parses raw result bytes and delegates to a **`ResultHandler`**
- **`ResultStateHandler` exists** (`control_plane/kernelq/result_handler.py`) — maps **`status`** → **`jobs.state`**
- **`KafkaResultConsumer` exists** (`control_plane/kernelq/kafka_result_consumer.py`) — **`poll_once`** on **`kernelq.jobs.results`**; manual script **`consume_result_once.py`**
- **Long-running result consumer loop** is not implemented yet
- **`retryable_failure`** and **`terminal_failure`** both map to **`failed`** (**FAILED**) today — **retry scheduling** (`RETRY_SCHEDULED`, **`DEAD_LETTERED`**) is not implemented yet

**Follow-up (when result pipeline lands):**

- Alert on jobs stuck in **`dispatched`** / **`running`** past SLA
- Monitor result-topic lag and consumer errors alongside dispatch lag

## Result Event Consumed but Job State Unchanged

Use this when a **`WorkerResultEvent`** was parsed/handled but **`jobs.state`** in Postgres did not move.

**Check:**

- **`job_id` exists in Postgres** — **`ResultStateHandler`** raises if **`update_job_state_from_worker_result`** returns **`False`**
- **Repository update returned `True`** — confirm the row was found and updated (not a silent no-op)
- **`status` mapping is supported** — today: **`succeeded`** → **`succeeded`**; **`retryable_failure`** / **`terminal_failure`** → **`failed`**
- **Result consumer is wired to the handler** — **`ResultConsumerRunner`** must use **`ResultStateHandler(repository)`**, not a no-op fake (Kafka subscribe/poll still future work for production)

**Note:** **`retryable_failure`** currently lands on **FAILED** only; do not expect **`retry_scheduled`** or automatic re-dispatch until retry scheduling is implemented.
