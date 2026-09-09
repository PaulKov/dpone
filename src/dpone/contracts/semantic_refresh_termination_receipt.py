"""Canonical trusted-observer attempt termination receipt."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_container_termination import SemanticRefreshContainerTermination
from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    canonical_string_set,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_sorted_unique_strings,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import (
    parse_utc_timestamp,
    require_utc_timestamp,
    require_uuid,
)
from dpone.contracts.semantic_refresh_termination_signature import (
    semantic_refresh_termination_observation_signature_subject,
)

TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA = "dpone.semantic-refresh-trusted-attempt-termination-receipt.v1"
_DIGEST_FIELD = "termination_receipt_sha256"
_VERIFICATION_STATUS = "VERIFIED"
_TERMINAL_PHASES = frozenset({"Succeeded", "Failed"})
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "workflow_execution_id",
        "workflow_execution_binding_sha256",
        "operation_ids",
        "operation_set_sha256",
        "attempt_binding_sha256",
        "dag_id",
        "run_id",
        "task_id",
        "map_index",
        "try_number",
        "cluster_id",
        "namespace",
        "pod_name",
        "pod_uid",
        "pod_resource_version",
        "terminal_phase",
        "container_terminations",
        "observed_at",
        "observer_authority",
        "observer_policy_sha256",
        "observer_attestation_sha256",
        "observer_signature_sha256",
        "verification_status",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshTrustedAttemptTerminationReceipt(SemanticRefreshDocumentCodec):
    """Trusted proof that one exact attempt's pod and every container terminated."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_ids: tuple[str, ...]
    operation_set_sha256: str
    attempt_binding_sha256: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    cluster_id: str
    namespace: str
    pod_name: str
    pod_uid: str
    pod_resource_version: str
    terminal_phase: str
    container_terminations: tuple[SemanticRefreshContainerTermination, ...]
    observed_at: str
    observer_authority: str
    observer_policy_sha256: str
    observer_attestation_sha256: str
    observer_signature_sha256: str
    termination_receipt_sha256: str
    verification_status: str = _VERIFICATION_STATUS
    schema: str = TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        for field in (
            "workflow_execution_id",
            "dag_id",
            "run_id",
            "task_id",
            "cluster_id",
            "namespace",
            "pod_name",
            "pod_resource_version",
            "observer_authority",
        ):
            require_text(getattr(self, field), field)
        for field in (
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
            "observer_policy_sha256",
            "observer_attestation_sha256",
            "observer_signature_sha256",
        ):
            require_digest(getattr(self, field), field)
        operation_ids = require_sorted_unique_strings(list(self.operation_ids), "operation_ids")
        for operation_id in operation_ids:
            require_digest(operation_id, "operation_ids")
        if require_digest(self.operation_set_sha256, "operation_set_sha256") != _operation_set_digest(operation_ids):
            raise SemanticRefreshContractError("operation_set_sha256 differs from operation_ids")
        if isinstance(self.map_index, bool) or not isinstance(self.map_index, int) or self.map_index < -1:
            raise SemanticRefreshContractError("map_index must be -1 or non-negative")
        require_positive_int(self.try_number, "try_number")
        require_uuid(self.pod_uid, "pod_uid")
        if self.terminal_phase not in _TERMINAL_PHASES:
            raise SemanticRefreshContractError("terminal_phase must be Succeeded or Failed")
        _canonical_containers(self.container_terminations)
        require_utc_timestamp(self.observed_at, "observed_at")
        if any(
            parse_utc_timestamp(item.finished_at, "container.finished_at")
            > parse_utc_timestamp(self.observed_at, "observed_at")
            for item in self.container_terminations
        ):
            raise SemanticRefreshContractError("container termination cannot follow observer timestamp")
        if self.verification_status != _VERIFICATION_STATUS:
            raise SemanticRefreshContractError("termination receipt must be VERIFIED")
        validate_digest(self._unsigned(), self.termination_receipt_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        operation_ids: tuple[str, ...],
        attempt_binding_sha256: str,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        try_number: int,
        cluster_id: str,
        namespace: str,
        pod_name: str,
        pod_uid: str,
        pod_resource_version: str,
        terminal_phase: str,
        container_terminations: tuple[SemanticRefreshContainerTermination, ...],
        observed_at: str,
        observer_authority: str,
        observer_policy_sha256: str,
        observer_attestation_sha256: str,
        observer_signature_sha256: str,
    ) -> SemanticRefreshTrustedAttemptTerminationReceipt:
        """Build the receipt from exact attempt, operation-set, pod and observer authority."""

        operations = canonical_string_set(operation_ids, "operation_ids")
        containers = _canonical_containers(container_terminations)
        operation_set_sha256 = _operation_set_digest(operations)
        unsigned = _unsigned_mapping(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_ids=operations,
            operation_set_sha256=operation_set_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            try_number=try_number,
            cluster_id=cluster_id,
            namespace=namespace,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_resource_version=pod_resource_version,
            terminal_phase=terminal_phase,
            container_terminations=containers,
            observed_at=observed_at,
            observer_authority=observer_authority,
            observer_policy_sha256=observer_policy_sha256,
            observer_attestation_sha256=observer_attestation_sha256,
            observer_signature_sha256=observer_signature_sha256,
        )
        return cls(
            workflow_execution_id,
            workflow_execution_binding_sha256,
            operations,
            operation_set_sha256,
            attempt_binding_sha256,
            dag_id,
            run_id,
            task_id,
            map_index,
            try_number,
            cluster_id,
            namespace,
            pod_name,
            pod_uid,
            pod_resource_version,
            terminal_phase,
            containers,
            observed_at,
            observer_authority,
            observer_policy_sha256,
            observer_attestation_sha256,
            observer_signature_sha256,
            semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshTrustedAttemptTerminationReceipt:
        """Parse a closed trusted-observer termination receipt."""

        raw = require_closed_mapping(value, "trusted_attempt_termination_receipt", required=_FIELDS)
        raw_containers = raw.get("container_terminations")
        if not isinstance(raw_containers, Sequence) or isinstance(raw_containers, str | bytes):
            raise SemanticRefreshContractError("container_terminations must be an array")
        map_index = raw.get("map_index")
        if isinstance(map_index, bool) or not isinstance(map_index, int) or map_index < -1:
            raise SemanticRefreshContractError("map_index must be -1 or non-negative")
        return cls(
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            operation_ids=require_sorted_unique_strings(raw.get("operation_ids"), "operation_ids"),
            operation_set_sha256=require_digest(raw.get("operation_set_sha256"), "operation_set_sha256"),
            attempt_binding_sha256=require_digest(raw.get("attempt_binding_sha256"), "attempt_binding_sha256"),
            dag_id=require_text(raw.get("dag_id"), "dag_id"),
            run_id=require_text(raw.get("run_id"), "run_id"),
            task_id=require_text(raw.get("task_id"), "task_id"),
            map_index=map_index,
            try_number=require_positive_int(raw.get("try_number"), "try_number"),
            cluster_id=require_text(raw.get("cluster_id"), "cluster_id"),
            namespace=require_text(raw.get("namespace"), "namespace"),
            pod_name=require_text(raw.get("pod_name"), "pod_name"),
            pod_uid=require_uuid(raw.get("pod_uid"), "pod_uid"),
            pod_resource_version=require_text(raw.get("pod_resource_version"), "pod_resource_version"),
            terminal_phase=require_text(raw.get("terminal_phase"), "terminal_phase"),
            container_terminations=tuple(
                SemanticRefreshContainerTermination.from_mapping(item) for item in raw_containers
            ),
            observed_at=require_utc_timestamp(raw.get("observed_at"), "observed_at"),
            observer_authority=require_text(raw.get("observer_authority"), "observer_authority"),
            observer_policy_sha256=require_digest(raw.get("observer_policy_sha256"), "observer_policy_sha256"),
            observer_attestation_sha256=require_digest(
                raw.get("observer_attestation_sha256"), "observer_attestation_sha256"
            ),
            observer_signature_sha256=require_digest(raw.get("observer_signature_sha256"), "observer_signature_sha256"),
            termination_receipt_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            verification_status=require_text(raw.get("verification_status"), "verification_status"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            workflow_execution_id=self.workflow_execution_id,
            workflow_execution_binding_sha256=self.workflow_execution_binding_sha256,
            operation_ids=self.operation_ids,
            operation_set_sha256=self.operation_set_sha256,
            attempt_binding_sha256=self.attempt_binding_sha256,
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
            try_number=self.try_number,
            cluster_id=self.cluster_id,
            namespace=self.namespace,
            pod_name=self.pod_name,
            pod_uid=self.pod_uid,
            pod_resource_version=self.pod_resource_version,
            terminal_phase=self.terminal_phase,
            container_terminations=self.container_terminations,
            observed_at=self.observed_at,
            observer_authority=self.observer_authority,
            observer_policy_sha256=self.observer_policy_sha256,
            observer_attestation_sha256=self.observer_attestation_sha256,
            observer_signature_sha256=self.observer_signature_sha256,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the canonical verified receipt."""

        return {**self._unsigned(), self.digest_field: self.termination_receipt_sha256}


def _canonical_containers(
    values: tuple[SemanticRefreshContainerTermination, ...],
) -> tuple[SemanticRefreshContainerTermination, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, SemanticRefreshContainerTermination) for item in values)
    ):
        raise SemanticRefreshContractError("container_terminations must be a non-empty tuple")
    ordered = tuple(sorted(values, key=lambda item: item.name))
    if len({item.name for item in ordered}) != len(ordered):
        raise SemanticRefreshContractError("container terminations must have unique names")
    if ordered != values:
        raise SemanticRefreshContractError("container terminations must be sorted by name")
    return ordered


def _operation_set_digest(operation_ids: tuple[str, ...]) -> str:
    return semantic_refresh_sha256({"operation_ids": list(operation_ids)})


def _unsigned_mapping(**values: object) -> dict[str, object]:
    containers = values.pop("container_terminations")
    operation_ids = values.pop("operation_ids")
    assert isinstance(containers, tuple) and isinstance(operation_ids, tuple)
    return {
        **values,
        "container_terminations": [item.to_dict() for item in containers],
        "operation_ids": list(operation_ids),
        "schema": TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA,
        "verification_status": _VERIFICATION_STATUS,
    }


__all__ = [
    "TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA",
    "SemanticRefreshContainerTermination",
    "SemanticRefreshTrustedAttemptTerminationReceipt",
    "semantic_refresh_termination_observation_signature_subject",
]
