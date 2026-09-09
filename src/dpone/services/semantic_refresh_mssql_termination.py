"""Protected termination-observation authority for semantic-refresh takeover."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_mssql_authority_codec import authority_from_record
from dpone.ports.semantic_refresh_termination import (
    MssqlTerminationObservationAuthority,
    SemanticRefreshTerminationObservationAuthorityPort,
    mssql_termination_observation_authority_sha256,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_authority import SemanticRefreshMssqlCanonicalAuthorityPort


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlTerminationAuthorityService:
    """Authenticate scheduler/Kubernetes coordinates against canonical attempts."""

    canonical_authority: SemanticRefreshMssqlCanonicalAuthorityPort
    observation_authority: SemanticRefreshTerminationObservationAuthorityPort

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
    ) -> MssqlTerminationObservationAuthority:
        """Return exact observer input; digest-only or caller-created input is rejected."""

        bundle = authority_from_record(self.canonical_authority.load(workflow_execution_binding_sha256))
        attempts = tuple(
            item for item in bundle.attempt_bindings if item.attempt_binding_sha256 == attempt_binding_sha256
        )
        if len(attempts) != 1:
            raise ValueError("attempt is absent or ambiguous in canonical authority")
        attempt = attempts[0]
        observed = self.observation_authority.load_observation_authority(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
        )
        expected_operation_ids = (attempt.operation_id,)
        expected_operation_set = _operation_set_sha256(expected_operation_ids)
        if (
            observed.status != "ACTIVE"
            or observed.workflow_execution_id != bundle.workflow_execution_id
            or observed.workflow_execution_binding_sha256 != workflow_execution_binding_sha256
            or observed.operation_ids != expected_operation_ids
            or observed.operation_set_sha256 != expected_operation_set
            or observed.attempt_binding_sha256 != attempt.attempt_binding_sha256
            or observed.run_id != attempt.dag_run_id
            or observed.task_id != attempt.task_id
            or observed.try_number != attempt.try_number
            or observed.pod_uid != attempt.pod_uid
        ):
            raise ValueError("termination observation authority differs from canonical attempt")
        if observed.observation_authority_sha256 != mssql_termination_observation_authority_sha256(observed):
            raise ValueError("termination observation authority digest differs")
        return observed


def semantic_refresh_mssql_termination_observation_authority_sha256(
    authority: MssqlTerminationObservationAuthority,
) -> str:
    """Digest protected scheduler and observer coordinates without observation output."""

    return mssql_termination_observation_authority_sha256(authority)


def _operation_set_sha256(operation_ids: tuple[str, ...]) -> str:
    import hashlib
    import json

    raw = json.dumps(
        {"operation_ids": list(operation_ids)},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "SemanticRefreshMssqlTerminationAuthorityService",
    "semantic_refresh_mssql_termination_observation_authority_sha256",
]
