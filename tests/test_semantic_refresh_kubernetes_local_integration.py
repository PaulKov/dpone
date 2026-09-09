"""Opt-in local Kubernetes proof for trusted termination-receipt inputs."""

from __future__ import annotations

import hmac
import os
import subprocess
from datetime import datetime, timezone

import pytest

try:
    from kubernetes import client, config
except ModuleNotFoundError:
    pytest.skip("kubernetes optional dependency is not installed", allow_module_level=True)

from dpone.adapters.semantic_refresh_kubernetes import KubernetesCoreV1TerminationObserver
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshTrustedAttemptTerminationReceipt,
)
from dpone.ports.semantic_refresh_termination import (
    MssqlTerminationObservationAuthority,
)
from dpone.services.semantic_refresh_termination_observer import (
    SemanticRefreshTerminationObserverService,
)

_ENABLED = os.getenv("DPONE_RUN_SEMANTIC_REFRESH_K8S_LIVE") == "1"
_NAMESPACE = "dpone-semref-v2-receipt-test"
_POD = "semantic-refresh-termination-probe"

pytestmark = pytest.mark.integration_live


class _LocalSigner:
    """Local-only HMAC proof; it cannot authorize a production takeover."""

    def sign(self, *, subject_sha256: str, authority_id: str) -> str:
        return self._signature(subject_sha256, authority_id)

    def verify(self, *, subject_sha256: str, authority_id: str, signature_sha256: str) -> bool:
        return hmac.compare_digest(signature_sha256, self._signature(subject_sha256, authority_id))

    @staticmethod
    def _signature(subject_sha256: str, authority_id: str) -> str:
        signature = hmac.digest(
            b"semantic-refresh-local-observer",
            f"{authority_id}\n{subject_sha256}".encode(),
            "sha256",
        )
        return "sha256:" + signature.hex()


class _ReceiptStore:
    def __init__(self) -> None:
        self.receipts: list[SemanticRefreshTrustedAttemptTerminationReceipt] = []

    def store(self, receipt: SemanticRefreshTrustedAttemptTerminationReceipt) -> None:
        self.receipts.append(receipt)


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_K8S_LIVE=1")
def test_local_cluster_proves_exact_terminal_pod_uid() -> None:
    _kubectl("delete", "namespace", _NAMESPACE, "--ignore-not-found=true", "--wait=true")
    try:
        _kubectl("create", "namespace", _NAMESPACE)
        _kubectl(
            "run",
            _POD,
            "--namespace",
            _NAMESPACE,
            "--image=busybox:1.36",
            "--restart=Never",
            "--command",
            "--",
            "sh",
            "-c",
            "exit 0",
        )
        _kubectl(
            "wait",
            "--namespace",
            _NAMESPACE,
            f"pod/{_POD}",
            "--for=jsonpath={.status.phase}=Succeeded",
            "--timeout=60s",
        )
        context = _context()
        api = client.CoreV1Api(config.new_client_from_config(context=context))
        pod = api.read_namespaced_pod(name=_POD, namespace=_NAMESPACE)
        operation_id = "sha256:" + "4" * 64
        store = _ReceiptStore()
        authority = MssqlTerminationObservationAuthority(
            workflow_execution_id="local-k8s-proof",
            workflow_execution_binding_sha256="sha256:" + "1" * 64,
            operation_ids=(operation_id,),
            operation_set_sha256=semantic_refresh_sha256({"operation_ids": [operation_id]}),
            attempt_binding_sha256="sha256:" + "2" * 64,
            dag_id="local_semantic_refresh",
            run_id="manual__local",
            task_id="termination_probe",
            map_index=-1,
            try_number=1,
            cluster_id=context,
            namespace=_NAMESPACE,
            pod_name=_POD,
            pod_uid=str(pod.metadata.uid),
            observer_authority="local-kubectl-observer",
            observer_policy_sha256="sha256:" + "5" * 64,
            observer_attestation_sha256="sha256:" + "6" * 64,
            observation_authority_sha256="sha256:" + "7" * 64,
            status="ACTIVE",
        )
        service = SemanticRefreshTerminationObserverService(
            observation=KubernetesCoreV1TerminationObserver(api=api, cluster_id=context),
            signer=_LocalSigner(),
            receipt_store=store,
            clock=lambda: datetime.now(timezone.utc),  # noqa: UP017 - package supports Python 3.10
        )
        receipt = service.observe_and_store(authority)

        assert receipt.terminal_phase == "Succeeded"
        assert receipt.pod_uid == pod.metadata.uid
        assert all(item.container_id for item in receipt.container_terminations)
        assert store.receipts == [receipt]
        assert service.verify(authority=authority, receipt=receipt) is receipt

        _kubectl("delete", "pod", _POD, "--namespace", _NAMESPACE, "--wait=true")
        missing_store = _ReceiptStore()
        missing_service = SemanticRefreshTerminationObserverService(
            observation=KubernetesCoreV1TerminationObserver(api=api, cluster_id=context),
            signer=_LocalSigner(),
            receipt_store=missing_store,
            clock=lambda: datetime.now(timezone.utc),  # noqa: UP017 - package supports Python 3.10
        )
        with pytest.raises(Exception):
            missing_service.observe_and_store(authority)
        assert missing_store.receipts == []
    finally:
        _kubectl("delete", "namespace", _NAMESPACE, "--ignore-not-found=true", "--wait=true")


def _kubectl(*arguments: str) -> str:
    context = os.getenv("DPONE_IT_KUBECONFIG_CONTEXT")
    command = ("kubectl", "--context", context, *arguments) if context else ("kubectl", *arguments)
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr[-2000:])
    return result.stdout


def _context() -> str:
    value = os.getenv("DPONE_IT_KUBECONFIG_CONTEXT") or _kubectl("config", "current-context").strip()
    if not value:
        raise AssertionError("an exact local Kubernetes context is required")
    return value
