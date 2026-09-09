"""Trusted-observer termination receipt contract tests."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pytest

from dpone.ports.semantic_refresh_termination import (
    AttemptTerminationReceipt,
    ContainerTermination,
    MssqlTerminationObservationAuthority,
)
from dpone.services.semantic_refresh_mssql_termination import (
    SemanticRefreshMssqlTerminationAuthorityService,
    semantic_refresh_mssql_termination_observation_authority_sha256,
)
from tests.test_semantic_refresh_mssql_authority import _bundle, _record


@dataclass(frozen=True)
class _CanonicalAuthority:
    def load(self, workflow_execution_binding_sha256: str):
        bundle = _bundle()
        assert workflow_execution_binding_sha256 == bundle.execution_binding.workflow_execution_binding_sha256
        return _record(bundle)


@dataclass(frozen=True)
class _ObservationAuthority:
    value: MssqlTerminationObservationAuthority

    def load_observation_authority(
        self,
        *,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
    ) -> MssqlTerminationObservationAuthority:
        assert workflow_execution_binding_sha256 == self.value.workflow_execution_binding_sha256
        assert attempt_binding_sha256 == self.value.attempt_binding_sha256
        return self.value


def _receipt() -> AttemptTerminationReceipt:
    return AttemptTerminationReceipt.build(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256="sha256:" + "1" * 64,
        operation_ids=("sha256:" + "4" * 64,),
        attempt_binding_sha256="sha256:" + "2" * 64,
        dag_id="competitive_pricing",
        run_id="scheduled__2026-08-08",
        task_id="dbt_build_and_tests",
        map_index=-1,
        try_number=1,
        cluster_id="dpone-semref-v2",
        namespace="dpone-semref-v2-live",
        pod_name="semantic-refresh-attempt",
        pod_uid="49851278-f049-401b-971e-f89f7b22bff6",
        pod_resource_version="950",
        terminal_phase="Succeeded",
        container_terminations=(
            ContainerTermination(
                name="base",
                container_id="containerd://sha256-runtime-id",
                reason="Completed",
                finished_at="2026-08-08T15:42:16Z",
                exit_code=0,
            ),
        ),
        observed_at="2026-08-08T15:42:17Z",
        observer_authority="kubernetes-observer-v1",
        observer_policy_sha256="sha256:" + "5" * 64,
        observer_attestation_sha256="sha256:" + "6" * 64,
        observer_signature_sha256="sha256:" + "3" * 64,
    )


def test_terminal_receipt_is_canonical_and_attempt_bound() -> None:
    receipt = _receipt()

    assert receipt.terminal_phase == "Succeeded"
    assert receipt.termination_receipt_sha256.startswith("sha256:")
    assert len(receipt.termination_receipt_sha256) == 71


@pytest.mark.parametrize("phase", ["Pending", "Running", "Unknown"])
def test_nonterminal_pod_phase_cannot_authorize_takeover(phase: str) -> None:
    with pytest.raises(ValueError, match="terminal_phase"):
        replace(_receipt(), terminal_phase=phase)


def test_self_modified_receipt_digest_is_rejected() -> None:
    with pytest.raises(ValueError, match="termination_receipt_sha256"):
        replace(_receipt(), pod_resource_version="951")


def test_termination_observer_receives_only_canonical_attempt_authority() -> None:
    authority = _observation_authority()
    service = SemanticRefreshMssqlTerminationAuthorityService(
        canonical_authority=_CanonicalAuthority(),
        observation_authority=_ObservationAuthority(authority),
    )

    loaded = service.load(
        workflow_execution_binding_sha256=authority.workflow_execution_binding_sha256,
        attempt_binding_sha256=authority.attempt_binding_sha256,
    )

    assert loaded == authority


def test_termination_observer_rejects_old_dag_run_authority() -> None:
    authority = _observation_authority()
    stale = replace(authority, run_id="scheduled__old")
    stale = replace(
        stale,
        observation_authority_sha256=semantic_refresh_mssql_termination_observation_authority_sha256(stale),
    )
    service = SemanticRefreshMssqlTerminationAuthorityService(
        canonical_authority=_CanonicalAuthority(),
        observation_authority=_ObservationAuthority(stale),
    )

    with pytest.raises(ValueError, match="differs from canonical attempt"):
        service.load(
            workflow_execution_binding_sha256=authority.workflow_execution_binding_sha256,
            attempt_binding_sha256=authority.attempt_binding_sha256,
        )


def _observation_authority() -> MssqlTerminationObservationAuthority:
    bundle = _bundle()
    attempt = bundle.attempt_bindings[0]
    operation_ids = (attempt.operation_id,)
    value = MssqlTerminationObservationAuthority(
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        operation_ids=operation_ids,
        operation_set_sha256="sha256:" + "0" * 64,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        dag_id="daily_events",
        run_id=attempt.dag_run_id,
        task_id=attempt.task_id,
        map_index=-1,
        try_number=attempt.try_number,
        cluster_id="dpone-semref-v2",
        namespace="dpone-semref-v2-live",
        pod_name="semantic-refresh-attempt",
        pod_uid=attempt.pod_uid,
        observer_authority="kubernetes-observer-v1",
        observer_policy_sha256="sha256:" + "5" * 64,
        observer_attestation_sha256="sha256:" + "6" * 64,
        observation_authority_sha256="sha256:" + "0" * 64,
        status="ACTIVE",
    )
    from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256

    value = replace(
        value,
        operation_set_sha256=semantic_refresh_sha256({"operation_ids": list(operation_ids)}),
    )
    return replace(
        value,
        observation_authority_sha256=semantic_refresh_mssql_termination_observation_authority_sha256(value),
    )
