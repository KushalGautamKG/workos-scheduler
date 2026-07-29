# Resilience Testing (Day 129)

## Before Day 129

```
Retries + Idempotency + Restarts
             │
     Architecturally Present
```

KernelQ already had at-least-once Kafka delivery, Redis/memory execution claims, gRPC deadlines, and graceful shutdown hooks — but controlled failure evidence was incomplete.

## After Day 129

```
Controlled Failure
       │
Expected Behavior
       │
Telemetry Evidence
       │
Recovery
       │
Final-State Verification
```

Deterministic fault injection (disabled by default), resilience metrics, and smoke scenarios prove **local** recovery behavior. This is **not** a production chaos or multi-AZ guarantee.

## Failure model matrix

| Scenario | Failure trigger | Expected logs | Expected metrics | Expected trace status | Expected retry behavior | Expected final state | Cleanup |
|----------|-----------------|---------------|------------------|----------------------|-------------------------|----------------------|---------|
| Worker exits before processing | Process kill before Handle | lifecycle stop; no execute success | optional recovery attempt | no execute span or incomplete | Kafka offset not committed → redelivery | Job remains recoverable | Kill worker; restart; unique group |
| Worker exits during processing | Kill / panic mid-Handle | fault or abrupt stop | recovery_attempts | error or unfinished span | Redelivery / republish per semantics | Complete once after recovery | Delete claim keys if testing Redis gap |
| Duplicate Kafka delivery | Same job_id+attempt twice | `duplicate execution skipped` WARN | `kernelq_duplicate_deliveries_total` | duplicate recorded | No second Execute | One business completion | TTL/key delete |
| Redis temporarily unavailable | Stop Redis / failing store | claim failed + error_type | recovery_* redis | error on claim path | Fail closed — no unsafe execute | Success after Redis restore | `docker compose start redis` |
| Kafka temporarily unavailable | Stop broker | publish/consume errors | recovery_* kafka | publish span error | Bounded failure; retry after restore | Successful publish/consume | Start kafka; report observed recovery ms |
| gRPC execution dependency unavailable | Unreachable addr / short deadline | operation/status/error_type | recovery_* grpc | error / deadline | No infinite retry | Success when target up | N/A |
| Result publish fails | Inject / fake producer error | `result publish failed` | distinct from execute success | publish span error | Replay must not double-complete (claim held — **documented gap**) | Execution result returned **with** error | Honest limitation: claim-before-completion |
| Pod receives SIGTERM | `kill -TERM` | shutting down → draining → stopped | graceful_shutdown_timeout if exceeded | provider shutdown attempted | In-flight completes or recoverable cancel | Process exits within bound | Trap cleanup |
| Pod is deleted | `kubectl delete pod` | restart events | kube available replicas | N/A | Deployment recreates Pod | Ready + Service works | Delete test namespace |
| Invalid job payload | Blank job_id / bad event | validation / classified error | message_errors path | error | No crash loop from one poison message | Worker continues | N/A |

## Fault injection contract

Env (all optional; disabled by default):

| Variable | Default | Notes |
|----------|---------|-------|
| `KERNELQ_FAULTS_ENABLED` | `false` | Must be `true` to activate |
| `KERNELQ_FAULT_POINT` | — | `before_claim`, `after_claim`, `before_execute`, `after_execute`, `before_result_publish`, `after_result_publish` |
| `KERNELQ_FAULT_MODE` | `error` | `error` \| `delay` \| `panic` |
| `KERNELQ_FAULT_COUNT` | `1` | Bounded |
| `KERNELQ_FAULT_DELAY_MS` | `0` | For `delay` mode |
| `KERNELQ_ENVIRONMENT` | `local` | Must be explicit non-prod (`local\|test\|dev\|development`); **production rejected** |

No shell execution, no eval. Package: `worker/internal/faults`.

## Validated local behavior vs production guarantees

| Validated locally | Not claimed |
|-------------------|-------------|
| Deterministic fault points | Multi-region resilience |
| Duplicate delivery → one completion | Exact recovery-time SLOs |
| Redis fail-closed claim | AZ failover |
| Kafka outage observability + restore | Broker durability beyond tested |
| gRPC deadline respect | Production chaos completion |
| Graceful shutdown log sequence | Zero data loss under every failure |

## Future

Cloud chaos tooling, Alertmanager linkage to resilience metrics, and execution-recovery leases remain future work (see [execution-recovery.md](execution-recovery.md)).
