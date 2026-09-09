"""Create-only SQL Server activation capabilities for semantic refresh V2."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone.contracts.dbt_semantic_refresh_run_guard import (
    SemanticRefreshRunGuardClosure,
)
from dpone.ports.semantic_refresh_mssql_authority_records import MssqlCanonicalAuthorityRecord
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_worker_pack_fingerprint,
)


@dataclass(frozen=True, order=True, slots=True)
class MssqlBaselineActivation:
    """One authenticated baseline row installed during deployment activation."""

    target_resource_id: str
    model_unique_id: str
    baseline_kind: str
    baseline_receipt_sha256: str
    baseline_receipt_json: str

    def __post_init__(self) -> None:
        _text(self.target_resource_id, "target_resource_id")
        _text(self.model_unique_id, "model_unique_id")
        if self.baseline_kind not in {
            "certified_initial_load",
            "adopted_complete_relation_conformant",
        }:
            raise ValueError("baseline_kind is unsupported")
        _digest(self.baseline_receipt_sha256, "baseline_receipt_sha256")
        _text(self.baseline_receipt_json, "baseline_receipt_json")


@dataclass(frozen=True, order=True, slots=True)
class MssqlTargetOwnerActivation:
    """One deployment-wide target owner installed create-only."""

    target_authority_id: str
    model_unique_id: str
    deployment_id: str
    owner_generation: int

    def __post_init__(self) -> None:
        _text(self.target_authority_id, "target_authority_id")
        _text(self.model_unique_id, "model_unique_id")
        _digest(self.deployment_id, "deployment_id")
        _positive(self.owner_generation, "owner_generation")


@dataclass(frozen=True, order=True, slots=True)
class MssqlTargetHeadActivation:
    """Initial ClickHouse target head proven by a complete baseline receipt."""

    database_name: str
    target_table: str
    target_generation: int
    target_generation_id: str
    target_uuid: str
    baseline_operation_id: str

    def __post_init__(self) -> None:
        _text(self.database_name, "database_name")
        _text(self.target_table, "target_table")
        _positive(self.target_generation, "target_generation")
        _digest(self.target_generation_id, "target_generation_id")
        try:
            UUID(self.target_uuid)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("target_uuid must be a canonical UUID") from exc
        _digest(self.baseline_operation_id, "baseline_operation_id")


@dataclass(frozen=True, slots=True)
class MssqlDeploymentActivationRequest:
    """Closed deployment activation unit committed in one transaction."""

    deployment_id: str
    deployment_subject_sha256: str
    baselines: tuple[MssqlBaselineActivation, ...]
    target_owners: tuple[MssqlTargetOwnerActivation, ...]
    target_heads: tuple[MssqlTargetHeadActivation, ...]
    target_guard_resource_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _digest(self.deployment_id, "deployment_id")
        _digest(self.deployment_subject_sha256, "deployment_subject_sha256")
        model_ids = tuple(item.model_unique_id for item in self.baselines)
        if not model_ids or model_ids != tuple(sorted(set(model_ids))):
            raise ValueError("deployment baselines must be a non-empty canonical model closure")
        if tuple(item.model_unique_id for item in self.target_owners) != model_ids:
            raise ValueError("target owners must cover the exact baseline model closure")
        if len(self.target_heads) != len(model_ids):
            raise ValueError("target heads must cover the exact baseline model closure")
        if self.target_guard_resource_ids != tuple(sorted(set(self.target_guard_resource_ids))):
            raise ValueError("target guards must be unique and canonically ordered")
        if set(self.target_guard_resource_ids) != {item.target_resource_id for item in self.baselines}:
            raise ValueError("target guards must cover the exact baseline resource closure")


@dataclass(frozen=True, slots=True)
class MssqlRunAuthorityRegistration:
    """Create-only run authority and exact initial guard epochs."""

    record: MssqlCanonicalAuthorityRecord
    guard_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if not isinstance(self.record, MssqlCanonicalAuthorityRecord):
            raise TypeError("record must be a canonical authority record")
        resources = tuple(item[0] for item in self.guard_epochs)
        if not resources or resources != tuple(sorted(set(resources))):
            raise ValueError("run guard epochs must be a non-empty canonical closure")
        for resource_id, epoch in self.guard_epochs:
            _text(resource_id, "guard resource_id")
            if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
                raise ValueError("guard predecessor epoch must be non-negative")


@dataclass(frozen=True, order=True, slots=True)
class MssqlActivatedPackRegistration:
    """Run-bound activated-pack identity persisted create-only."""

    pack_fingerprint: str
    activation_authority_receipt_sha256: str
    authority_store_ref: str
    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    plan_bundle_sha256: str
    run_execution_bundle_sha256: str
    run_guard_closure: SemanticRefreshRunGuardClosure
    projection_identity: MssqlStaticProjectionIdentity

    def __post_init__(self) -> None:
        for field_name in (
            "pack_fingerprint",
            "activation_authority_receipt_sha256",
            "workflow_execution_binding_sha256",
            "workflow_plan_sha256",
            "plan_bundle_sha256",
            "run_execution_bundle_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        _bounded_text(self.authority_store_ref, "authority_store_ref", 1024)
        _bounded_text(self.workflow_execution_id, "workflow_execution_id", 512)
        if not isinstance(self.run_guard_closure, SemanticRefreshRunGuardClosure):
            raise TypeError("run_guard_closure must be canonical and typed")
        if not isinstance(self.projection_identity, MssqlStaticProjectionIdentity):
            raise TypeError("projection_identity must be canonical and typed")
        if (
            self.plan_bundle_sha256 != self.projection_identity.plan_bundle_sha256
            or self.workflow_plan_sha256 != self.projection_identity.workflow_plan_sha256
            or self.pack_fingerprint
            != mssql_worker_pack_fingerprint(
                projection_identity=self.projection_identity,
                run_execution_bundle_sha256=self.run_execution_bundle_sha256,
                activation_authority_receipt_sha256=self.activation_authority_receipt_sha256,
                authority_store_ref=self.authority_store_ref,
                run_guard_closure_sha256=self.run_guard_closure.run_guard_closure_sha256,
            )
        ):
            raise ValueError("activated pack fingerprint differs from its complete authority")


class SemanticRefreshMssqlActivationPort(Protocol):
    """Persist authenticated deployment and run authority create-only."""

    def activate_deployment(self, request: MssqlDeploymentActivationRequest) -> None:
        """Install exact deployment prerequisites or acknowledge exact replay."""

    def register_run_authority(self, request: MssqlRunAuthorityRegistration) -> None:
        """Install one ACTIVE run authority and missing initial guards."""


class SemanticRefreshMssqlActivatedPackPort(Protocol):
    """Persist run-bound activated-pack authority independently of deployment state."""

    def register_activated_pack(self, request: MssqlActivatedPackRegistration) -> None:
        """Persist one exact ACTIVE run pack or acknowledge exact replay."""


@dataclass(frozen=True, slots=True)
class MssqlWorkerRunBindingAuthority:
    """Immutable ACTIVE pack and canonical run binding, independent of lifecycle state."""

    record: MssqlCanonicalAuthorityRecord
    workflow_plan_sha256: str
    pack_fingerprint: str
    activation_authority_receipt_sha256: str
    authority_store_ref: str
    plan_bundle_sha256: str
    run_execution_bundle_sha256: str
    run_guard_closure: SemanticRefreshRunGuardClosure
    projection_identity: MssqlStaticProjectionIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.record, MssqlCanonicalAuthorityRecord) or self.record.status != "ACTIVE":
            raise ValueError("worker run binding authority record must be ACTIVE")
        for field_name in (
            "workflow_plan_sha256",
            "pack_fingerprint",
            "activation_authority_receipt_sha256",
            "plan_bundle_sha256",
            "run_execution_bundle_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        _bounded_text(self.authority_store_ref, "authority_store_ref", 1024)
        if not isinstance(self.run_guard_closure, SemanticRefreshRunGuardClosure):
            raise TypeError("run_guard_closure must be canonical and typed")
        if not isinstance(self.projection_identity, MssqlStaticProjectionIdentity):
            raise TypeError("projection_identity must be canonical and typed")
        if (
            self.plan_bundle_sha256 != self.projection_identity.plan_bundle_sha256
            or self.workflow_plan_sha256 != self.projection_identity.workflow_plan_sha256
            or self.pack_fingerprint
            != mssql_worker_pack_fingerprint(
                projection_identity=self.projection_identity,
                run_execution_bundle_sha256=self.run_execution_bundle_sha256,
                activation_authority_receipt_sha256=self.activation_authority_receipt_sha256,
                authority_store_ref=self.authority_store_ref,
                run_guard_closure_sha256=self.run_guard_closure.run_guard_closure_sha256,
            )
        ):
            raise ValueError("worker run pack fingerprint differs from protected projection")


class SemanticRefreshMssqlWorkerRunBindingAuthorityPort(Protocol):
    """Locate the immutable run binding for terminal replay or active workers."""

    def locate_binding(
        self,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> MssqlWorkerRunBindingAuthority:
        """Return exact ACTIVE pack/canonical authority without mutable state claims."""


def mssql_run_guard_closure_from_storage(
    *,
    run_guard_closure_sha256: str,
    workflow_guard_resource_id: str,
    resource_guard_ids_json: str,
) -> SemanticRefreshRunGuardClosure:
    """Reconstruct and authenticate the exact guard closure stored by MSSQL."""

    try:
        resource_guard_ids = json.loads(resource_guard_ids_json)
    except (TypeError, ValueError) as exc:
        raise ValueError("resource_guard_ids_json must be valid JSON") from exc
    return SemanticRefreshRunGuardClosure.from_mapping(
        {
            "schema": "dpone.dbt-semantic-refresh-run-guard-closure.v1",
            "workflow_guard_resource_id": workflow_guard_resource_id,
            "resource_guard_ids": resource_guard_ids,
            "run_guard_closure_sha256": run_guard_closure_sha256,
        }
    )


def _text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")


def _bounded_text(value: str, field_name: str, max_length: int) -> None:
    _text(value, field_name)
    if len(value) > max_length:
        raise ValueError(f"{field_name} exceeds its protected length")


def _digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


def _positive(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be positive")


__all__ = [
    "MssqlActivatedPackRegistration",
    "MssqlBaselineActivation",
    "MssqlCanonicalAuthorityRecord",
    "MssqlDeploymentActivationRequest",
    "MssqlRunAuthorityRegistration",
    "MssqlTargetHeadActivation",
    "MssqlTargetOwnerActivation",
    "MssqlWorkerRunBindingAuthority",
    "SemanticRefreshMssqlActivatedPackPort",
    "SemanticRefreshMssqlActivationPort",
    "SemanticRefreshMssqlWorkerRunBindingAuthorityPort",
    "mssql_run_guard_closure_from_storage",
]
