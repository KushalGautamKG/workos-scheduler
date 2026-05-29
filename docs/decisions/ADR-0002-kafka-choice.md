# ADR-0002: Kafka as the Work Coordination Backbone

**Status**: Accepted  
**Date**: 2026  
**Deciders**: Engineering Team

## Context

KernelQ separates **scheduling** (Python control plane) from **execution** (Go worker plane). Postgres already stores durable job state—`queued`, `dispatched`, `running`, and so on—but something must **hand runnable work** from schedulers to workers safely under load, failures, and deploys.

Today the control plane can **claim** jobs in Postgres (`claim_schedulable_jobs`, scheduler ticks). **Kafka publish and Go worker consumption are not wired yet.** This ADR records why we chose Kafka as the broker between those two planes.

## Decision Drivers

- **Decouple** scheduler ticks from worker execution speed and availability
- **Durable buffering** when workers are slow or temporarily down
- **Scale workers** independently (many Go consumers, consumer groups)
- **Replay / retry** patterns that fit KernelQ’s lifecycle (`failed`, `retry_scheduled`, dead-letter)
- **High throughput** for the worker plane (see ADR-0001 language split)
- **Clear failure domain** between control plane, broker, workers, and Postgres

## Why KernelQ Needs a Broker

Without a broker, the scheduler would **directly invoke** workers (“run job X now”). That tightly couples:

- **Who picks work** (Python, Postgres-backed ticks) with **who runs work** (Go processes)
- **Scheduling rate** with **execution capacity**
- **Recovery** with **every worker being up and reachable**

A broker is the **handoff layer**: schedulers **produce** dispatch events; workers **consume** when ready. Postgres remains the **system of record** for job identity and lifecycle; Kafka carries **runnable work in flight** at execution scale.

## Considered Options

### Option 1: RabbitMQ

- **Pros:** Mature, familiar queue semantics, good for task queues, often simpler to operate for small teams than Kafka
- **Cons:** Less natural fit for **log-style replay** and long retention; scaling consumption patterns differ from Kafka’s partition + consumer-group model; KernelQ’s future retry/DLQ story maps cleanly to **topics and replay**, not only point-to-point queues

### Option 2: Direct Worker RPC

- **Pros:** Simplest mental model; no broker to run locally; scheduler calls worker HTTP/gRPC directly
- **Cons:** Scheduler must track worker health and capacity; slow or crashed workers block or fail the dispatch path; hard to absorb bursts; retries and backpressure live in scheduler code; does not match ADR-0001’s split (Go workers as high-throughput consumers)

### Option 3: Redis Queues (Lists / Streams)

- **Pros:** Easy local setup; fast; often already in stack for caching
- **Cons:** Primarily in-memory unless carefully configured; durability and replay are weaker defaults than Kafka for a **coordination backbone**; mixing cache and job transport increases blast radius; less ideal for long retention and audit-style replay

### Option 4: Apache Kafka

- **Pros:** Durable log; partitions + consumer groups for scale; replay by offset; strong fit for event-style dispatch and future retry/DLQ topics; widely used for control-plane → worker-plane handoff at volume
- **Cons:** Heavier operationally; local dev setup is more work than Redis or direct RPC; steeper learning curve than RabbitMQ for simple queue-only use cases

## Decision

We will use **Apache Kafka** as KernelQ’s **work coordination backbone** between the Python control plane and Go workers.

### Why Kafka Fits KernelQ Specifically

| Need | How Kafka helps |
|------|-----------------|
| **Durable buffering** | Messages persist on disk (with replication in production); workers can lag without losing work |
| **Replay capability** | Topics are logs—consumers can re-read or new consumer groups can catch up; supports debugging and retry flows |
| **Scalable consumers** | Partitions + **consumer groups** let many Go workers share load without changing scheduler code |
| **Decouple scheduler from workers** | Scheduler **publishes** after Postgres claim; workers **pull** at their own pace |
| **Future DLQ / retry support** | Separate **retry topics** and **DLQ topics** align with `RETRY_SCHEDULED`, `FAILED`, and `DEAD_LETTERED` in the job state machine |

**Interview sound bite:** *Postgres is truth for lifecycle; Kafka is the durable pipe from “claimed” to “executing”—buffer, scale, replay.*

### Relationship to Postgres (Not Either/Or)

- **Postgres:** job rows, state transitions, scheduling queries, atomic claiming
- **Kafka:** transport of “run this job” events after (or as part of) dispatch
- Workers still **report back** to Postgres (and metrics) so lifecycle stays authoritative in the database

## Downsides

- **Operational complexity:** brokers, ZooKeeper/KRaft, monitoring, retention, ACLs—more moving parts than Redis or direct RPC
- **Local setup complexity:** Docker Compose must run Kafka (or Redpanda, etc.) for realistic integration tests; harder than “pytest + Postgres only”
- **Harder initially than RabbitMQ:** for a pure task queue with one consumer type, RabbitMQ can be faster to learn; we accept Kafka’s cost for replay, scale, and log-oriented retry/DLQ patterns

## Future Usage

Planned integration (not all implemented yet):

1. **Scheduler publishes dispatch events** — after `claim_schedulable_jobs` (or publish-then-claim, TBD), control plane writes a message to a runnable-jobs topic (e.g. job id, tenant, priority, payload snapshot or reference)
2. **Go workers consume tasks** — worker plane joins a consumer group, processes messages, transitions jobs `dispatched` → `running` → terminal states in Postgres
3. **Retry topics** — failed executions re-published with backoff metadata or routed to a retry topic consumed by a retry dispatcher aligned with `RETRY_SCHEDULED`
4. **DLQ topics** — jobs that exhaust retries land on a dead-letter topic for inspection, matching `DEAD_LETTERED` operationally

Local milestones will add Kafka to Compose, a minimal publisher in the control plane, and a minimal Go consumer—incrementally, with tests and metrics per step.

## Consequences

### Positive

- Scheduler and workers scale and deploy independently
- Bursts absorbed in the log instead of overloading Postgres polls or RPC fan-out
- Replay and multi-consumer patterns support retries, DLQ, and forensics
- Matches architecture docs and ADR-0001 worker-plane role (broker consumers)

### Negative

- Team must operate and debug Kafka (lag, partitions, consumer groups)
- Local and CI environments need broker containers or test doubles
- End-to-end tests become more complex (Postgres + Kafka + workers)

### Neutral

- `DISPATCHED` in Postgres may mean “claimed, publish pending” until publish is wired; docs and code must stay explicit
- Redis may still be used later for **cache**, not as the primary job transport

## How We Will Validate

1. **Publish latency:** p95 time from claim to Kafka ack (once implemented)
2. **Consumer lag:** max lag per partition under load
3. **Recovery:** worker kill → messages remain → workers catch up without duplicate execution (idempotency + state checks)
4. **Throughput:** tasks/sec with N Go consumers vs. direct-RPC baseline (if measured)
5. **Local dev:** documented Compose path; new contributor can run scheduler + Kafka + one worker

Success criteria: dispatch events flow reliably from control plane to workers, consumer lag stays bounded under target load, and retry/DLQ topics are feasible without replacing the broker.
