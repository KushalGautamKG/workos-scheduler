# ADR-0001: Foundations and Language Split

**Status**: Accepted  
**Date**: 2024  
**Deciders**: Engineering Team

## Context

We are building an internal distributed work coordination and scheduling platform. This system must handle high-throughput job execution, reliable scheduling, and complex coordination logic. It is not a CRUD application or tutorial project—it is a production system evaluated by system-level metrics and reliability patterns.

## Decision Drivers

- Need for high-throughput task execution (thousands of tasks per second)
- Complex scheduling and orchestration logic
- Production-grade reliability requirements
- Team expertise and development velocity
- Operational simplicity and maintainability
- Cost efficiency at scale

## Considered Options

### Option 1: Single Language (Python)
- Pros: Simpler codebase, one language to maintain, Python's rich ecosystem
- Cons: Python's GIL limits true concurrency, slower execution for high-throughput workers

### Option 2: Single Language (Go)
- Pros: Excellent concurrency, fast execution, single binary deployment
- Cons: Less expressive for complex coordination logic, smaller ecosystem for APIs/integrations

### Option 3: Split Architecture (Python Control + Go Workers)
- Pros: Best of both worlds—Python for flexibility, Go for performance
- Cons: Two codebases to maintain, requires gRPC/protocol definition

## Decision

We will use a **split architecture**:
- **Python for the Control Plane**: REST APIs, scheduling logic, job state machines, orchestration
- **Go for the Worker Plane**: Concurrent workers, broker consumers, task execution, metrics

This is NOT a CRUD app or tutorial clone. It is an internal distributed work coordination and scheduling platform evaluated by system metrics (throughput, latency, success rate, duplicate rate, recovery time, etc.).

### Core Requirements

**OS/Systems Concepts (Core, Not Decoration):**
- Scheduling algorithms (FIFO, priority, weighted fair)
- Starvation prevention and fairness guarantees
- Bounded queues with backpressure
- Flow control and admission control / rate limiting
- Resource isolation (worker concurrency limits, per-tenant quotas)
- Failure domains (broker / db / worker / network)
- Retries with idempotency and exactly-once semantics
- Job lifecycle state machine with well-defined transitions

**Production Mindset:**
- Every component must have defined failure modes, observability, metrics, and runbooks
- Reliability patterns: DLQ, exponential backoff + jitter, circuit breakers, bulkheads, timeouts, graceful degradation

**Language Split Rationale:**
- **Python (Control Plane)**: Expressiveness and ecosystem for APIs/coordination. Fast development for complex logic. Lower throughput acceptable for control operations.
- **Go (Worker Plane)**: Performance and concurrency model for high-throughput workers. Efficient resource usage. Predictable latency under load.

**Metrics-First Rule:**
- Every major feature must have at least one measurable metric
- If we cannot instrument or benchmark it, we redesign

## Consequences

### Positive
- Control plane can iterate quickly with Python's flexibility
- Worker plane achieves high throughput with Go's performance
- Clear separation of concerns
- Each plane optimized for its use case

### Negative
- Two codebases to maintain
- Requires protocol definition (gRPC) between planes
- Team needs expertise in both languages
- More complex deployment (two services)

### Neutral
- Initial development may be slower due to setup complexity
- Long-term maintenance balanced by appropriate tool choice

## Worker Plane Started

The **`worker/`** Go directory now exists (`go.mod`, `internal/worker/`). Go is reserved for **future Kafka consumers** and **bounded concurrent execution**—the hot path the control plane hands off via **`kernelq.jobs.dispatch`**.

The **first Go code** validates **`Task`** shape only (`ValidateTask`: non-blank `job_id` / `tenant_id`, non-negative priority). There is **no broker consumer or execution loop** yet.

This reinforces the **Python control plane / Go worker plane** split from this ADR: Python already schedules and publishes; Go scaffolding starts on the execution side without collapsing both into one language.

## How We Will Validate

1. **Throughput**: Measure tasks/second processed by Go workers. Target: >1000 tasks/sec per worker instance.
2. **Latency**: Measure p50/p95/p99 end-to-end latency. Control plane API p95 < 100ms, worker execution p95 < 500ms.
3. **Development Velocity**: Track time to implement new scheduling algorithms in Python vs. if done in Go.
4. **Reliability**: Measure success rate, retry rate, and recovery time after failures.
5. **Cost**: Compare infrastructure costs vs. single-language alternatives at equivalent throughput.
6. **Operational Overhead**: Track deployment complexity, debugging time, and incident resolution time.

Success criteria: System meets throughput/latency targets, development velocity is acceptable, and operational overhead is manageable.
