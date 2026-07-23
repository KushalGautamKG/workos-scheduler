#!/usr/bin/env bash
#
# Smoke test: local Kubernetes deploy via kustomize (Day 124).
# Builds images, loads into kind when needed, applies manifests, waits for
# rollout, port-forwards gRPC, runs Execute + health checks, then cleans up.
#
# Run from the repository root:
#   ./worker/scripts/smoke_k8s.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/kubernetes ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

NAMESPACE="${KERNELQ_K8S_NAMESPACE:-kernelq}"
KIND_CLUSTER="${KERNELQ_KIND_CLUSTER:-kernelq-smoke}"
WORKER_IMAGE="${KERNELQ_WORKER_IMAGE:-kernelq-worker:local}"
CONTROL_IMAGE="${KERNELQ_CONTROL_IMAGE:-kernelq-control-plane:local}"
PF_PORT="${KERNELQ_SMOKE_GRPC_PORT:-50084}"
CREATED_KIND_CLUSTER=0
PF_PID=""
DIAG_DIR="${TMPDIR:-/tmp}/kernelq-k8s-smoke-$$"
HEALTH_BIN="${TMPDIR:-/tmp}/kernelq-grpc-health-k8s-smoke"
EXECUTE_BIN="${TMPDIR:-/tmp}/kernelq-grpc-execute-k8s-smoke"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_k8s success=false" >&2
  collect_diagnostics || true
  exit 1
}

collect_diagnostics() {
  mkdir -p "${DIAG_DIR}"
  echo "" >&2
  echo "=== kubectl get pods,svc,configmap ===" >&2
  kubectl -n "${NAMESPACE}" get pods,svc,configmap -o wide 2>&1 | tee "${DIAG_DIR}/get.txt" >&2 || true
  echo "" >&2
  echo "=== kubectl get events ===" >&2
  kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp 2>&1 | tee "${DIAG_DIR}/events.txt" >&2 || true
  local pod
  for pod in $(kubectl -n "${NAMESPACE}" get pods -o name 2>/dev/null || true); do
    echo "" >&2
    echo "=== kubectl describe ${pod} ===" >&2
    kubectl -n "${NAMESPACE}" describe "${pod}" 2>&1 | tee "${DIAG_DIR}/describe-$(basename "${pod}").txt" >&2 || true
    echo "" >&2
    echo "=== kubectl logs ${pod} ===" >&2
    kubectl -n "${NAMESPACE}" logs "${pod}" --all-containers=true 2>&1 | tee "${DIAG_DIR}/logs-$(basename "${pod}").txt" >&2 || true
  done
}

cleanup() {
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
    PF_PID=""
  fi
  if kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "==> Deleting namespace ${NAMESPACE}..."
    kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=120s 2>/dev/null || true
  fi
  if [[ "${CREATED_KIND_CLUSTER}" -eq 1 ]] && command -v kind >/dev/null 2>&1; then
    echo "==> Deleting kind cluster ${KIND_CLUSTER}..."
    kind delete cluster --name "${KIND_CLUSTER}" 2>/dev/null || true
  fi
  rm -rf "${DIAG_DIR}"
}

trap cleanup EXIT INT TERM

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"

echo "==> Validating kustomize render..."
kubectl kustomize deploy/kubernetes >/dev/null || fail "kubectl kustomize failed"

ensure_cluster() {
  if kubectl cluster-info >/dev/null 2>&1; then
    echo "==> Using existing Kubernetes cluster"
    kubectl cluster-info | head -n 1
    return 0
  fi

  if ! command -v kind >/dev/null 2>&1; then
    echo "==> Installing kind (no cluster detected)..."
    if command -v brew >/dev/null 2>&1; then
      brew install kind || fail "brew install kind failed"
    else
      fail "no Kubernetes cluster and kind is not installed (install kind or enable Docker Desktop Kubernetes)"
    fi
  fi

  if kind get clusters 2>/dev/null | grep -Fxq "${KIND_CLUSTER}"; then
    echo "==> Using existing kind cluster ${KIND_CLUSTER}"
    kubectl cluster-info --context "kind-${KIND_CLUSTER}" >/dev/null 2>&1 \
      || fail "kind cluster ${KIND_CLUSTER} exists but kubectl cannot reach it"
    return 0
  fi

  echo "==> Creating kind cluster ${KIND_CLUSTER}..."
  kind create cluster --name "${KIND_CLUSTER}" || fail "kind create cluster failed"
  CREATED_KIND_CLUSTER=1
  kubectl cluster-info --context "kind-${KIND_CLUSTER}" >/dev/null 2>&1 \
    || fail "kind cluster created but not reachable"
}

ensure_cluster

CTX="$(kubectl config current-context 2>/dev/null || true)"
echo "==> kubectl context: ${CTX:-<none>}"

echo "==> Building container images..."
docker build -f deploy/docker/Dockerfile.worker -t "${WORKER_IMAGE}" . \
  || fail "worker image build failed"
docker build -f deploy/docker/Dockerfile.control-plane -t "${CONTROL_IMAGE}" . \
  || fail "control-plane image build failed"

if [[ "${CTX}" == kind-* ]]; then
  KIND_NAME="${CTX#kind-}"
  echo "==> Loading images into kind cluster ${KIND_NAME}..."
  kind load docker-image "${WORKER_IMAGE}" --name "${KIND_NAME}" \
    || fail "kind load worker image failed"
  kind load docker-image "${CONTROL_IMAGE}" --name "${KIND_NAME}" \
    || fail "kind load control-plane image failed"
fi

echo "==> Applying manifests (kubectl apply -k)..."
kubectl apply -k deploy/kubernetes || fail "kubectl apply -k failed"

echo "==> Waiting for rollouts..."
if ! kubectl -n "${NAMESPACE}" rollout status deployment/kernelq-worker --timeout=180s; then
  fail "worker rollout failed"
fi
if ! kubectl -n "${NAMESPACE}" rollout status deployment/kernelq-control-plane --timeout=180s; then
  fail "control-plane rollout failed"
fi

echo "==> Verifying Pods Ready / Services / ConfigMap..."
kubectl -n "${NAMESPACE}" get pods -o wide
kubectl -n "${NAMESPACE}" get svc
kubectl -n "${NAMESPACE}" get configmap
kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp | tail -n 20 || true

READY_COUNT="$(kubectl -n "${NAMESPACE}" get pods --no-headers 2>/dev/null | awk '$2 ~ /^[0-9]+\/[0-9]+$/ && $3=="Running" {split($2,a,"/"); if (a[1]==a[2] && a[1]>0) c++} END{print c+0}')"
[[ "${READY_COUNT}" -ge 2 ]] || fail "expected both Deployments' Pods Ready, got Ready count=${READY_COUNT}"

kubectl -n "${NAMESPACE}" get svc kernelq-worker >/dev/null \
  || fail "missing Service kernelq-worker"
kubectl -n "${NAMESPACE}" get svc kernelq-control-plane >/dev/null \
  || fail "missing Service kernelq-control-plane"
kubectl -n "${NAMESPACE}" get configmap kernelq-config >/dev/null \
  || fail "missing ConfigMap kernelq-config"

echo "==> Building grpc-health and grpc-execute helpers..."
(
  cd worker
  go build -o "${HEALTH_BIN}" ./cmd/grpc-health
  go build -o "${EXECUTE_BIN}" ./cmd/grpc-execute
) || fail "failed to build gRPC helpers"

echo "==> Port-forwarding worker Service to localhost:${PF_PORT}..."
kubectl -n "${NAMESPACE}" port-forward svc/kernelq-worker "${PF_PORT}:50051" >/dev/null 2>&1 &
PF_PID=$!
sleep 1
if ! kill -0 "${PF_PID}" 2>/dev/null; then
  fail "port-forward failed to start"
fi

# Wait until port accepts connections.
PF_OK=0
for _ in $(seq 1 30); do
  if "${HEALTH_BIN}" -addr "127.0.0.1:${PF_PORT}" 2>/dev/null | grep -Fq "status=SERVING"; then
    PF_OK=1
    break
  fi
  sleep 0.5
done
[[ "${PF_OK}" -eq 1 ]] || fail "health did not report SERVING via port-forward"

JOB_ID="day124-k8s-$(date +%s)"
echo "==> Executing job ${JOB_ID} via forwarded gRPC..."
OUT="$("${EXECUTE_BIN}" -addr "127.0.0.1:${PF_PORT}" -job-id "${JOB_ID}" -attempt 0 -payload smoke)"
echo "${OUT}"
echo "${OUT}" | grep -Fq "status=SUCCESS" || fail "expected Execute status=SUCCESS"

# Tracing enabled via ConfigMap — confirm OTel started and execution was logged.
WORKER_POD="$(kubectl -n "${NAMESPACE}" get pod -l app.kubernetes.io/name=kernelq-worker -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${WORKER_POD}" ]] || fail "worker pod not found"

OTEL_OK=0
EXEC_OK=0
SPAN_OK=0
for _ in $(seq 1 40); do
  LOGS="$(kubectl -n "${NAMESPACE}" logs "${WORKER_POD}" --tail=500 2>/dev/null || true)"
  echo "${LOGS}" | grep -Fq "event=otel_tracer_provider_start enabled=true" && OTEL_OK=1
  echo "${LOGS}" | grep -Fq "event=grpc_execute job_id=${JOB_ID}" && EXEC_OK=1
  echo "${LOGS}" | grep -Eq 'worker\.execute|"Name": "worker.execute"' && SPAN_OK=1
  if [[ "${OTEL_OK}" -eq 1 && "${EXEC_OK}" -eq 1 && "${SPAN_OK}" -eq 1 ]]; then
    break
  fi
  sleep 0.5
done
[[ "${OTEL_OK}" -eq 1 ]] || fail "expected OTel enabled (event=otel_tracer_provider_start enabled=true)"
[[ "${EXEC_OK}" -eq 1 ]] || fail "worker did not log execution for ${JOB_ID}"
[[ "${SPAN_OK}" -eq 1 ]] || fail "expected worker.execute span evidence in worker logs (stdout exporter)"

# Health still SERVING after Execute.
"${HEALTH_BIN}" -addr "127.0.0.1:${PF_PORT}" | grep -Fq "status=SERVING" \
  || fail "health not SERVING after Execute"

echo "PASS: kubernetes smoke succeeded"
echo "event=smoke_k8s success=true"
