# Day 129 — Resilience Failure Testing Verification

## Scenario results (local / offline)

| Scenario | Fault injected | Expected | Observed | Recovery time | Final state | Telemetry | Cleanup |
|----------|----------------|----------|----------|---------------|-------------|-----------|---------|
| Fault injector unit tests | error/delay/config reject | Bounded, prod rejected | Pass | N/A | N/A | metric + log + span event | N/A |
| Duplicate delivery | Second Handle same key | One execute | Pass | N/A | duplicate_skipped | duplicate metric | N/A |
| Fault before_claim recovery | ModeError count=1 | Fail then succeed | Pass | Immediate retry | 1 success | fault + recovery metrics | N/A |
| Redis unavailable (simulated store) | Claim error | Fail closed | Pass | Immediate with healthy store | 1 success | recovery_* | N/A |
| Redis live stop/restore | compose stop | Detect + restore | Pass when docker up | Observed in smoke log | Healthy ping | N/A | start redis |
| Kafka live stop/restore | compose stop | Detect + restore | Pass when docker up | `observed_recovery_ms` logged | Topics listable | N/A | start kafka |
| gRPC unavailable | bad addr / timeout tests | Bounded error | Pass | N/A | Dial/deadline fail | recovery_* | N/A |
| Result publish failure | Fake producer | Success result + error | Pass | N/A | Distinct ops | publish logs | N/A |
| Graceful shutdown logs | SIGTERM path | shutting down/draining/stopped | Consumer logs wired | Bound by signal | Exit | optional timeout metric | traps |
| K8s pod delete | Optional cluster | Replacement Ready | Pass or SKIP | Cluster-dependent | Service usable | kube events | delete ns |

## Explicit statements

**These tests validate deterministic local failure scenarios and do not establish production availability guarantees.**

**No production AWS infrastructure was disrupted during this verification.**

Do **not** claim from Day 129 alone:

- Multi-region resilience
- Exact recovery-time guarantees
- Production failover
- Zero data loss under every failure
- Availability-zone tolerance
- Broker durability beyond what was actually tested

## Limitation (honest)

Result-publish failure after a successful Redis claim can leave a claimed-but-unpublished gap until TTL or recovery work (see [execution-recovery.md](../design/execution-recovery.md)). Day 129 documents and tests observability of the failure; it does not redesign the transaction model.
