#!/usr/bin/env bash
# Live launch-pin ConfigMap CAS + RBAC certification on a disposable k3d cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${LAUNCH_PIN_CERT_CLUSTER:-dpone-lp-cert}"
NAMESPACE="${LAUNCH_PIN_CERT_NAMESPACE:-airflow}"
EVIDENCE_DIR="${ROOT}/test_artifacts/launch-pin-k3d-cert"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "${EVIDENCE_DIR}"

cleanup() {
  if [[ "${KEEP_CLUSTER:-}" != "1" ]]; then
    k3d cluster delete "${CLUSTER}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v k3d >/dev/null 2>&1; then
  echo "k3d is required but not installed" >&2
  exit 2
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not installed" >&2
  exit 2
fi
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not available" >&2
  exit 2
fi

k3d cluster delete "${CLUSTER}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER}" \
  --agents 1 \
  --servers 1 \
  --wait \
  --kubeconfig-switch-context=false \
  >"${EVIDENCE_DIR}/k3d-create-${TIMESTAMP}.log" 2>&1

export KUBECONFIG
KUBECONFIG="$(k3d kubeconfig write "${CLUSTER}")"
export LAUNCH_PIN_CERT_CLUSTER="${CLUSTER}"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl get nodes -o wide | tee "${EVIDENCE_DIR}/nodes-${TIMESTAMP}.txt"
kubectl version --output=json | tee "${EVIDENCE_DIR}/k8s-version-${TIMESTAMP}.json"

cd "${ROOT}"
uv sync --extra kubernetes --extra full >/dev/null
uv run python tools/launch_pin_k3d_cert.py \
  --namespace "${NAMESPACE}" \
  --cluster "${CLUSTER}" \
  --output "${EVIDENCE_DIR}/receipt-${TIMESTAMP}.json" \
  | tee "${EVIDENCE_DIR}/cert-${TIMESTAMP}.log"

cp "${EVIDENCE_DIR}/receipt-${TIMESTAMP}.json" "${EVIDENCE_DIR}/receipt.json"

# Markdown receipt for human review / docs links.
RECEIPT_MD="${EVIDENCE_DIR}/receipt-${TIMESTAMP}.md"
uv run python - "${EVIDENCE_DIR}/receipt.json" "${RECEIPT_MD}" <<'PY'
import json, sys
from pathlib import Path
receipt = json.loads(Path(sys.argv[1]).read_text())
lines = [
    "# Launch-pin K3d CAS/RBAC certification receipt",
    "",
    f"- **Status:** {receipt['status']}",
    f"- **Certified at:** {receipt['certified_at']}",
    f"- **Commit SHA:** `{receipt['commit_sha']}`",
    f"- **Cluster:** `{receipt['cluster']}`",
    f"- **Namespace:** `{receipt['namespace']}`",
    f"- **Kubernetes:** `{receipt.get('kubernetes_version', 'unknown')}`",
    "",
    "## Scenarios",
    "",
]
for item in receipt["scenarios"]:
    lines.append(
        f"- **{item['name']}:** {item['status']} — {item.get('detail', '')} ({item.get('duration_ms', 0)} ms)"
    )
lines.extend(["", "## Limitations", ""])
for lim in receipt.get("limitations", []):
    lines.append(f"- {lim}")
Path(sys.argv[2]).write_text("\n".join(lines) + "\n")
PY
cp "${RECEIPT_MD}" "${EVIDENCE_DIR}/receipt.md"

echo "Evidence: ${EVIDENCE_DIR}/receipt-${TIMESTAMP}.json"
