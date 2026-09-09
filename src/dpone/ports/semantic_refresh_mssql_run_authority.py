"""Protected mutable worker-run and attempt authority projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone.contracts.dbt_semantic_refresh_run_guard import SemanticRefreshRunGuardClosure
from dpone.ports.semantic_refresh_mssql_authority_records import MssqlCanonicalAuthorityRecord
from dpone.ports.semantic_refresh_mssql_primitives import _require_digest, _require_text
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_worker_pack_fingerprint,
)


@dataclass(frozen=True, order=True, slots=True)
class MssqlWorkerAttemptAuthority:
    """One admitted attempt/fence projection exposed to runtime workers."""

    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    owner_id: str
    task_id: str
    try_number: int
    pod_uid: str
    journal_status: str

    def __post_init__(self) -> None:
        for field_name in ("model_unique_id", "owner_id", "task_id", "journal_status"):
            _require_text(getattr(self, field_name), field_name)
        for field_name in (
            "operation_id",
            "operation_plan_sha256",
            "attempt_binding_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _positive(self.fencing_epoch, "fencing_epoch")
        _positive(self.try_number, "try_number")
        try:
            if str(UUID(self.pod_uid)) != self.pod_uid:
                raise ValueError
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("pod_uid must be canonical") from exc


@dataclass(frozen=True, slots=True)
class MssqlWorkerRunAuthority:
    """Protected ACTIVE pack/run authority with optional admitted attempts."""

    record: MssqlCanonicalAuthorityRecord
    workflow_plan_sha256: str
    pack_fingerprint: str
    activation_authority_receipt_sha256: str
    authority_store_ref: str
    plan_bundle_sha256: str
    run_execution_bundle_sha256: str
    run_guard_closure: SemanticRefreshRunGuardClosure
    projection_identity: MssqlStaticProjectionIdentity
    admission_status: str
    attempts: tuple[MssqlWorkerAttemptAuthority, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.record, MssqlCanonicalAuthorityRecord) or self.record.status != "ACTIVE":
            raise ValueError("worker run authority record must be ACTIVE")
        for field_name in (
            "workflow_plan_sha256",
            "pack_fingerprint",
            "activation_authority_receipt_sha256",
            "plan_bundle_sha256",
            "run_execution_bundle_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _require_text(self.authority_store_ref, "authority_store_ref")
        if len(self.authority_store_ref) > 1024:
            raise ValueError("authority_store_ref exceeds its protected length")
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
        if self.admission_status not in {"ADMITTED", "REGISTERED"}:
            raise ValueError("worker run admission_status is invalid")
        if self.attempts != tuple(sorted(self.attempts)) or len({item.operation_id for item in self.attempts}) != len(
            self.attempts
        ):
            raise ValueError("worker run attempts must be a canonical operation closure")
        if (self.admission_status == "REGISTERED") != (not self.attempts):
            raise ValueError("worker attempts may appear only after exact admission")


class SemanticRefreshMssqlWorkerRunAuthorityPort(Protocol):
    """Locate one exact active run authority by plan and logical DagRun."""

    def locate(
        self,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> MssqlWorkerRunAuthority:
        """Return ACTIVE binding and only durably admitted attempt/fence inputs."""


def _positive(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be positive")


__all__ = [
    "MssqlWorkerAttemptAuthority",
    "MssqlWorkerRunAuthority",
    "SemanticRefreshMssqlWorkerRunAuthorityPort",
]
