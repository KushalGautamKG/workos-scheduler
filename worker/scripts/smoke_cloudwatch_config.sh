#!/usr/bin/env bash
#
# Offline CloudWatch / Fluent Bit configuration smoke (Day 127).
# No AWS credentials, network, CloudWatch, EKS, or live cluster required.
#
# Run from repository root:
#   ./worker/scripts/smoke_cloudwatch_config.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/observability/fluent-bit ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-cloudwatch-config-smoke-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"
FB_OUT="${TMP_DIR}/fluent-bit.yaml"
OBS_OUT="${TMP_DIR}/eks-observability.yaml"

AWS_REGION="us-east-1"
CLOUDWATCH_LOG_GROUP="/kernelq/example/application"
CLOUDWATCH_LOG_STREAM_PREFIX="from-fluent-bit-"
FLUENT_BIT_ROLE_ARN="arn:aws:iam::123456789012:role/kernelq-example-fluent-bit"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_cloudwatch_config success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v git >/dev/null 2>&1 || fail "git is required"
mkdir -p "${TMP_DIR}"

git status --short >"${STATUS_BEFORE}"

echo "==> Rendering Fluent Bit collector..."
kubectl kustomize deploy/observability/fluent-bit >"${FB_OUT}" \
  || fail "kustomize fluent-bit failed"

grep -Eq 'kind:[[:space:]]*DaemonSet' "${FB_OUT}" || fail "DaemonSet missing"
grep -Eq 'name:[[:space:]]*kernelq-fluent-bit' "${FB_OUT}" || fail "fluent-bit name missing"
grep -Eq 'kind:[[:space:]]*ServiceAccount' "${FB_OUT}" || fail "ServiceAccount missing"
grep -Fq 'kernelq-fluent-bit' "${FB_OUT}" || fail "dedicated SA name missing"
grep -Fq 'public.ecr.aws/aws-observability/aws-for-fluent-bit:2.32.4' "${FB_OUT}" \
  || fail "explicit image tag missing"
grep -Eq 'requests:' "${FB_OUT}" || fail "resource requests missing"
grep -Eq 'limits:' "${FB_OUT}" || fail "resource limits missing"
grep -Eq 'mountPath:[[:space:]]*/var/log' "${FB_OUT}" || fail "host log mount missing"
grep -Eq 'readOnly:[[:space:]]*true' "${FB_OUT}" || fail "read-only mounts missing"
grep -Fq 'cloudwatch_logs' "${FB_OUT}" || fail "CloudWatch output missing"
grep -Fq 'CLOUDWATCH_LOG_GROUP' "${FB_OUT}" || fail "log group env missing"
grep -Fq 'CLOUDWATCH_LOG_STREAM_PREFIX' "${FB_OUT}" || fail "stream prefix env missing"
grep -Fq 'kubernetes' "${FB_OUT}" || fail "kubernetes metadata enrichment missing"
grep -Fq 'kernelq_json' "${FB_OUT}" || fail "JSON log parsing missing"
# Ensure filter does not remove correlation fields
if grep -E 'Remove[[:space:]]+trace_id|Remove[[:space:]]+span_id' "${FB_OUT}"; then
  fail "trace_id/span_id must not be removed"
fi
grep -Fq 'AWS_ACCESS_KEY_ID' "${FB_OUT}" && fail "AWS credentials must not be embedded" || true
grep -Fq 'aws_secret_access_key' "${FB_OUT}" && fail "AWS secret must not be embedded" || true
grep -Fq 'AKIA' "${FB_OUT}" && fail "access key pattern must not be embedded" || true

echo "==> Rendering EKS observability overlay (with fake IRSA role)..."
WORK="${TMP_DIR}/work"
mkdir -p "${WORK}"
cp -a "${ROOT_DIR}/deploy" "${WORK}/deploy"
# Patch overlay SA annotation to fake role ARN for offline assert (temp tree only).
python3 - "${WORK}" "${FLUENT_BIT_ROLE_ARN}" "${CLOUDWATCH_LOG_GROUP}" <<'PY'
from pathlib import Path
import sys
work, role, group = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
sa = work / "deploy/kubernetes/overlays/eks-observability/fluent-bit-sa-annotation-patch.yaml"
text = sa.read_text(encoding="utf-8")
text = text.replace(
    "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<FLUENT_BIT_ROLE>",
    role,
)
sa.write_text(text, encoding="utf-8")
envp = work / "deploy/kubernetes/overlays/eks-observability/fluent-bit-logging-patch.yaml"
etext = envp.read_text(encoding="utf-8")
etext = etext.replace("/kernelq/eks/application", group)
etext = etext.replace('value: "us-east-1"', 'value: "us-east-1"')
envp.write_text(etext, encoding="utf-8")
PY

kubectl kustomize "${WORK}/deploy/kubernetes/overlays/eks-observability" >"${OBS_OUT}" \
  || fail "kustomize eks-observability failed"

grep -Eq 'kind:[[:space:]]*DaemonSet' "${OBS_OUT}" || fail "overlay missing DaemonSet"
grep -Fq "${FLUENT_BIT_ROLE_ARN}" "${OBS_OUT}" || fail "overlay missing fake Fluent Bit role ARN"
grep -Fq "${CLOUDWATCH_LOG_GROUP}" "${OBS_OUT}" || fail "overlay missing configurable log group"
grep -Fq "${CLOUDWATCH_LOG_STREAM_PREFIX}" "${OBS_OUT}" || fail "overlay missing stream prefix"
grep -Fq "AWS_REGION" "${OBS_OUT}" || fail "overlay missing AWS_REGION"
grep -Fq 'kernelq-fluent-bit' "${OBS_OUT}" || fail "overlay missing collector SA"

# Application Deployments must not receive collector IAM annotations.
python3 - "${OBS_OUT}" <<'PY' || fail "collector IAM leaked to application Deployments"
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8")
docs = [d for d in text.split("\n---\n") if d.strip()]
role_marker = "eks.amazonaws.com/role-arn"
for doc in docs:
    if "kind: Deployment" not in doc:
        continue
    if role_marker in doc:
        raise SystemExit("Deployment unexpectedly contains collector role annotation")
    if "kernelq-fluent-bit" in doc and "kind: Deployment" in doc:
        # Deployments should not reference the collector SA name as their SA.
        for line in doc.splitlines():
            if "serviceAccountName:" in line and "kernelq-fluent-bit" in line:
                raise SystemExit("application Deployment uses fluent-bit service account")
print("application Deployments clean of collector IAM")
PY

# Confirm app Deployments still present (composed from eks).
grep -Eq 'kind:[[:space:]]*Deployment' "${OBS_OUT}" || fail "overlay missing KernelQ Deployments"
grep -Fq 'runAsNonRoot: true' "${OBS_OUT}" || fail "production/EKS policies missing runAsNonRoot"

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked/working tree changed during smoke (unexpected mutation)"
fi

echo "PASS: CloudWatch configuration smoke succeeded"
echo "event=smoke_cloudwatch_config success=true"
