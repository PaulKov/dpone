from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dpone.adapters.semantic_refresh_kubernetes import KubernetesCoreV1TerminationObserver
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshTrustedAttemptTerminationReceipt,
)
from dpone.ports.semantic_refresh_termination import MssqlTerminationObservationAuthority
from dpone.services.semantic_refresh_termination_observer import (
    SemanticRefreshTerminationObserverService,
)

_UTC = timezone.utc  # noqa: UP017 - package supports Python 3.10
_OPERATION_ID = "sha256:" + "1" * 64
_ATTEMPT = "sha256:" + "2" * 64
_EXECUTION = "sha256:" + "3" * 64
_POLICY = "sha256:" + "4" * 64
_ATTESTATION = "sha256:" + "5" * 64
_POD_UID = "11111111-2222-4333-8444-555555555555"


class _Api:
    def __init__(self, pod: object) -> None:
        self.pod = pod
        self.calls: list[tuple[str, str]] = []

    def read_namespaced_pod(self, *, name: str, namespace: str) -> object:
        self.calls.append((name, namespace))
        return self.pod


class _Signer:
    def __init__(self) -> None:
        self.subjects: list[tuple[str, str]] = []

    def sign(self, *, subject_sha256: str, authority_id: str) -> str:
        self.subjects.append((subject_sha256, authority_id))
        return self._signature(subject_sha256, authority_id)

    def verify(self, *, subject_sha256: str, authority_id: str, signature_sha256: str) -> bool:
        return signature_sha256 == self._signature(subject_sha256, authority_id)

    @staticmethod
    def _signature(subject_sha256: str, authority_id: str) -> str:
        return semantic_refresh_sha256(
            {
                "authority_id": authority_id,
                "schema": "test.observer-signature.v1",
                "subject_sha256": subject_sha256,
            }
        )


class _Store:
    def __init__(self) -> None:
        self.receipts: list[SemanticRefreshTrustedAttemptTerminationReceipt] = []

    def store(self, receipt: SemanticRefreshTrustedAttemptTerminationReceipt) -> None:
        self.receipts.append(receipt)


def test_observer_reads_exact_terminal_uid_signs_and_stores() -> None:
    api = _Api(_pod())
    signer = _Signer()
    store = _Store()
    service = SemanticRefreshTerminationObserverService(
        observation=KubernetesCoreV1TerminationObserver(api=api, cluster_id="cluster-a"),
        signer=signer,
        receipt_store=store,
        clock=lambda: datetime(2026, 8, 8, 12, 0, tzinfo=_UTC),
    )

    receipt = service.observe_and_store(_authority())

    assert api.calls == [("worker-1", "dpone")]
    assert receipt.pod_uid == _POD_UID
    assert receipt.terminal_phase == "Succeeded"
    assert tuple(item.name for item in receipt.container_terminations) == ("main", "setup")
    assert receipt.observed_at == "2026-08-08T12:00:00.000000Z"
    assert len(signer.subjects) == 1
    assert signer.subjects[0][1] == "vault-transit://semantic-refresh-observer"
    assert store.receipts == [receipt]
    assert service.verify(authority=_authority(), receipt=receipt) is receipt


@pytest.mark.parametrize(
    ("phase", "uid", "terminated", "message"),
    [
        ("Running", _POD_UID, True, "terminal phase"),
        ("Succeeded", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", True, "UID"),
        ("Succeeded", _POD_UID, False, "non-terminated container"),
    ],
)
def test_observer_fails_closed_before_signing_or_storage(
    phase: str,
    uid: str,
    terminated: bool,
    message: str,
) -> None:
    signer = _Signer()
    store = _Store()
    service = SemanticRefreshTerminationObserverService(
        observation=KubernetesCoreV1TerminationObserver(
            api=_Api(_pod(phase=phase, uid=uid, terminated=terminated)),
            cluster_id="cluster-a",
        ),
        signer=signer,
        receipt_store=store,
        clock=lambda: datetime(2026, 8, 8, 12, 0, tzinfo=_UTC),
    )

    with pytest.raises(ValueError, match=message):
        service.observe_and_store(_authority())

    assert signer.subjects == []
    assert store.receipts == []


def test_observer_rejects_non_active_authority_without_kubernetes_call() -> None:
    api = _Api(_pod())
    service = SemanticRefreshTerminationObserverService(
        observation=KubernetesCoreV1TerminationObserver(api=api, cluster_id="cluster-a"),
        signer=_Signer(),
        receipt_store=_Store(),
        clock=lambda: datetime(2026, 8, 8, 12, 0, tzinfo=_UTC),
    )
    authority = _authority(status="DRAINED")

    with pytest.raises(ValueError, match="not ACTIVE"):
        service.observe_and_store(authority)

    assert api.calls == []


def _authority(*, status: str = "ACTIVE") -> MssqlTerminationObservationAuthority:
    return MssqlTerminationObservationAuthority(
        workflow_execution_id="semantic-refresh-run-1",
        workflow_execution_binding_sha256=_EXECUTION,
        operation_ids=(_OPERATION_ID,),
        operation_set_sha256=semantic_refresh_sha256({"operation_ids": [_OPERATION_ID]}),
        attempt_binding_sha256=_ATTEMPT,
        dag_id="semantic_refresh_workflow",
        run_id="scheduled__2026-08-08T00:00:00+00:00",
        task_id="dbt_build_and_tests",
        map_index=-1,
        try_number=1,
        cluster_id="cluster-a",
        namespace="dpone",
        pod_name="worker-1",
        pod_uid=_POD_UID,
        observer_authority="vault-transit://semantic-refresh-observer",
        observer_policy_sha256=_POLICY,
        observer_attestation_sha256=_ATTESTATION,
        observation_authority_sha256="sha256:" + "6" * 64,
        status=status,
    )


def _pod(
    *,
    phase: str = "Succeeded",
    uid: str = _POD_UID,
    terminated: bool = True,
) -> object:
    finish = datetime(2026, 8, 8, 11, 59, 59, tzinfo=_UTC)
    main_state = SimpleNamespace(
        terminated=(SimpleNamespace(exit_code=0, reason="Completed", finished_at=finish) if terminated else None)
    )
    setup_state = SimpleNamespace(terminated=SimpleNamespace(exit_code=0, reason="Completed", finished_at=finish))
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="worker-1",
            namespace="dpone",
            uid=uid,
            resource_version="42",
        ),
        status=SimpleNamespace(
            phase=phase,
            init_container_statuses=(
                SimpleNamespace(name="setup", container_id="containerd://setup", state=setup_state),
            ),
            container_statuses=(SimpleNamespace(name="main", container_id="containerd://main", state=main_state),),
            ephemeral_container_statuses=(),
        ),
    )
