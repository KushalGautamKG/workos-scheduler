# Structured Logging (Day 127)

## Before Day 127

```
Application Events
       │
Mixed Console Output
```

Workers and control-plane scripts emitted mixed `fmt.Printf` / `event=key=value`
lines without a shared schema, without reliable trace correlation in logs, and
without a documented path to a log backend.

## After Day 127

```
Application Events
       │
Structured JSON Logs
       │
Trace Correlation
       │
Kubernetes Metadata
       │
Collector (Fluent Bit DaemonSet)
       │
CloudWatch-Compatible Output
```

## Shared field contract

Required base fields (every event):

| Field | Notes |
|-------|--------|
| `timestamp` | UTC |
| `level` | e.g. `INFO` |
| `message` | human text; do not embed JSON |
| `service` | `kernelq-worker` / `kernelq-control-plane` |
| `environment` | e.g. `local` |
| `version` | e.g. `dev` |

Contextual when available: `trace_id`, `span_id`, `job_id`, `attempt`,
`tenant_id`, `component`, `operation`, `status`, `error_type`.

Rules: one JSON object per line; stable lowercase field names; no entire
payloads; no credentials, tokens, credentialed URLs, or raw authorization
headers.

## Go worker

Package `worker/internal/logging` configures `log/slog` from:

- `KERNELQ_LOG_LEVEL` (default `info`)
- `KERNELQ_LOG_FORMAT` (`json` \| `text`, default `json`)
- `KERNELQ_SERVICE_NAME` (default `kernelq-worker`)
- `KERNELQ_ENVIRONMENT` / `KERNELQ_VERSION`

`WithTraceContext` and `WithJob` enrich loggers from `context.Context` / job
metadata. Production and Kubernetes should use JSON.

## Python control plane

`control_plane/app/core/logging_context.py` attaches identity and optional OTel
trace/span ids via a logging filter/adapter. Env vars align with Go where
possible; service name remains `kernelq-control-plane`. Existing
`format_log_event` key=value helpers remain for scripts.

## Log levels

| Level | Use |
|-------|-----|
| DEBUG | Internal lifecycle detail |
| INFO | Expected business / service events |
| WARN | Recoverable anomalies, duplicates / retries |
| ERROR | Failures needing attention |

Duplicate / idempotency skips are **WARN**, not application crashes.

## Sensitive-data policy

Prefer:

```json
{
  "message": "result publish failed",
  "operation": "result_publish",
  "status": "failed",
  "error_type": "timeout"
}
```

Avoid logging Kafka payloads, Redis URLs with passwords, `Authorization`
headers, or unbounded exception objects as structured fields.

## Collector model

Fluent Bit runs as a **DaemonSet** (one Pod per node) under service account
`kernelq-fluent-bit`. It tails Kubernetes container logs, enriches with
namespace/pod/container/node metadata, parses KernelQ JSON, and outputs to
CloudWatch Logs using env placeholders (`AWS_REGION`, `CLOUDWATCH_LOG_GROUP`,
`CLOUDWATCH_LOG_STREAM_PREFIX`).

Collector IAM belongs only to the Fluent Bit identity (Pod Identity / IRSA).
Static AWS keys must not be mounted. See
[deploy/aws/cloudwatch-logs.md](../../deploy/aws/cloudwatch-logs.md).

EKS composition: `deploy/kubernetes/overlays/eks-observability` =
`../eks` + Fluent Bit.

## Validation honesty

CloudWatch configuration is validated **offline** (Kustomize + smoke scripts).
That does **not** prove real ingestion, delivery guarantees, retention
compliance, or production cost.

## Future

Log dashboards and alerts (error rate, missing traces, collector health) can
build on this contract once a live backend is wired.
