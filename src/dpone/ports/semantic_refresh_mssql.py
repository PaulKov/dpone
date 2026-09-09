"""Capability ports and immutable evidence for SQL Server semantic refresh."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Protocol

from dpone.ports.semantic_refresh_mssql_evidence import (
    MssqlBuildReceiptEvidence,
    MssqlImageEvidence,
    MssqlOperationEvidence,
    MssqlTransactionDisposition,
    SemanticRefreshMssqlEvidencePort,
    mssql_build_receipt_sha256,
    mssql_operation_evidence_sha256,
    mssql_session_evidence_sha256,
)
from dpone.ports.semantic_refresh_mssql_primitives import (
    MssqlGuardClaim,
    MssqlImageKeyColumn,
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
    MssqlWorkflowResourceBudget,
    _require_digest,
    _require_non_negative,
    _require_text,
    mssql_guard_set_sha256,
    mssql_image_key_columns_json,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlAdmissionTargetHeadAuthority,
)

_SHA256_PREFIX = "sha256:"


@dataclass(frozen=True, slots=True)
class MssqlJournalPreparation:
    """PREPARING journal identity written in the admission transaction."""

    model_unique_id: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    strategy_authority_json: str
    strategy_authority_sha256: str
    baseline_receipt_sha256: str
    baseline_kind: str
    baseline_receipt_json: str
    baseline_status: str
    image_key_columns: tuple[MssqlImageKeyColumn, ...]
    target_resource_id: str
    publication_database: str
    publication_target_table: str
    publication_scope_id: str
    target_predecessor_generation_id: str
    scope_predecessor_operation_id: str | None
    fencing_epoch: int
    replaces_failed_operation_id: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.model_unique_id, "model_unique_id")
        _require_text(self.operation_id, "operation_id")
        _require_digest(self.operation_plan_sha256, "operation_plan_sha256")
        _require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        _require_text(self.strategy_authority_json, "strategy_authority_json")
        _require_digest(self.strategy_authority_sha256, "strategy_authority_sha256")
        if mssql_strategy_authority_sha256(self.strategy_authority_json) != self.strategy_authority_sha256:
            raise ValueError("strategy authority digest differs from its exact NVARCHAR JSON")
        _require_digest(self.baseline_receipt_sha256, "baseline_receipt_sha256")
        if self.baseline_kind not in {
            "certified_initial_load",
            "adopted_complete_relation_conformant",
        }:
            raise ValueError("baseline_kind is unsupported")
        _require_text(self.baseline_receipt_json, "baseline_receipt_json")
        if self.baseline_status != "COMPLETE":
            raise ValueError("baseline_status must equal COMPLETE")
        if (
            not isinstance(self.image_key_columns, tuple)
            or not self.image_key_columns
            or any(not isinstance(item, MssqlImageKeyColumn) for item in self.image_key_columns)
            or len({item.name for item in self.image_key_columns}) != len(self.image_key_columns)
        ):
            raise ValueError("image_key_columns must be unique authenticated key specifications")
        _require_text(self.target_resource_id, "target_resource_id")
        _require_text(self.publication_database, "publication_database")
        _require_text(self.publication_target_table, "publication_target_table")
        _require_text(self.publication_scope_id, "publication_scope_id")
        _require_digest(
            self.target_predecessor_generation_id,
            "target_predecessor_generation_id",
        )
        if self.scope_predecessor_operation_id is not None:
            _require_digest(
                self.scope_predecessor_operation_id,
                "scope_predecessor_operation_id",
            )
        _require_non_negative(self.fencing_epoch, "fencing_epoch")
        if self.fencing_epoch == 0:
            raise ValueError("fencing_epoch must be positive")
        if self.replaces_failed_operation_id is not None:
            _require_digest(self.replaces_failed_operation_id, "replaces_failed_operation_id")


@dataclass(frozen=True, order=True, slots=True)
class MssqlTargetOwnerClaim:
    """Exact deployment-wide owner required for one physical target asset."""

    target_authority_id: str
    model_unique_id: str
    deployment_id: str
    owner_generation: int

    def __post_init__(self) -> None:
        _require_text(self.target_authority_id, "target_authority_id")
        _require_text(self.model_unique_id, "model_unique_id")
        _require_digest(self.deployment_id, "deployment_id")
        _require_non_negative(self.owner_generation, "owner_generation")
        if self.owner_generation == 0:
            raise ValueError("owner_generation must be positive")


def mssql_journal_set_sha256(journals: tuple[MssqlJournalPreparation, ...]) -> str:
    """Hash the canonical mutating-operation identity closure."""

    payload = {
        "journals": [
            {
                "attempt_binding_sha256": item.attempt_binding_sha256,
                "baseline_receipt_sha256": item.baseline_receipt_sha256,
                "baseline_kind": item.baseline_kind,
                "baseline_receipt_json": item.baseline_receipt_json,
                "baseline_status": item.baseline_status,
                "fencing_epoch": item.fencing_epoch,
                "image_key_columns": [
                    {"name": key.name, "order_encoding": key.order_encoding} for key in item.image_key_columns
                ],
                "model_unique_id": item.model_unique_id,
                "operation_id": item.operation_id,
                "operation_plan_sha256": item.operation_plan_sha256,
                "publication_database": item.publication_database,
                "publication_scope_id": item.publication_scope_id,
                "publication_target_table": item.publication_target_table,
                "replaces_failed_operation_id": item.replaces_failed_operation_id,
                "scope_predecessor_operation_id": item.scope_predecessor_operation_id,
                "strategy_authority_json": item.strategy_authority_json,
                "strategy_authority_sha256": item.strategy_authority_sha256,
                "target_predecessor_generation_id": item.target_predecessor_generation_id,
                "target_resource_id": item.target_resource_id,
            }
            for item in journals
        ],
        "schema": "dpone.semantic-refresh-mssql-journal-set.v1",
    }
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class MssqlAdmissionRequest:
    """Complete workflow admission unit; adapters must never split this request.

    ``workflow_id`` is the legacy SQL storage alias for this exact execution,
    not the stable workflow name. The stable workflow is represented by the
    authenticated workflow-plan digest and its canonical authority document.
    """

    workflow_id: str
    workflow_execution_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    canonical_authority_sha256: str
    canonical_authority_json: str
    controller_id: str
    owner_id: str
    reservation_id: str
    resource_budget: MssqlWorkflowResourceBudget
    expected_guard_set_sha256: str
    expected_journal_set_sha256: str
    workflow_guard: MssqlGuardClaim
    resource_guards: tuple[MssqlGuardClaim, ...]
    journals: tuple[MssqlJournalPreparation, ...]
    target_heads: tuple[MssqlAdmissionTargetHeadAuthority, ...]
    target_owners: tuple[MssqlTargetOwnerClaim, ...]
    prerequisite_authorities: tuple[MssqlPrerequisiteAuthorityClaim, ...] = ()
    successor_claim: MssqlWorkflowSuccessorClaim | None = None
    guard_set_sha256: str = field(init=False)
    journal_set_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in (
            "workflow_id",
            "workflow_execution_id",
            "canonical_authority_json",
            "controller_id",
            "owner_id",
            "reservation_id",
        ):
            _require_text(getattr(self, field_name), field_name)
        if self.workflow_execution_id != self.workflow_id:
            raise ValueError("legacy workflow_id storage alias must equal workflow_execution_id")
        _require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        _require_digest(
            self.workflow_execution_binding_sha256,
            "workflow_execution_binding_sha256",
        )
        _require_digest(self.canonical_authority_sha256, "canonical_authority_sha256")
        _require_digest(self.expected_guard_set_sha256, "expected_guard_set_sha256")
        _require_digest(self.expected_journal_set_sha256, "expected_journal_set_sha256")
        if not self.resource_guards:
            raise ValueError("resource_guards must contain the full resource guard set")
        resource_ids = tuple(item.resource_id for item in self.resource_guards)
        if resource_ids != tuple(sorted(resource_ids)) or len(resource_ids) != len(set(resource_ids)):
            raise ValueError("resource_guards must be unique and sorted by resource_id")
        if self.workflow_guard.resource_id in set(resource_ids):
            raise ValueError("workflow_guard must be distinct from resource_guards")
        if not self.journals:
            raise ValueError("journals must contain every selected mutating operation")
        if self.resource_budget.max_workflow_prepared_models < len(self.journals):
            raise ValueError("workflow prepared-model budget is smaller than the journal closure")
        operation_ids = tuple(item.operation_id for item in self.journals)
        if operation_ids != tuple(sorted(operation_ids)) or len(operation_ids) != len(set(operation_ids)):
            raise ValueError("journals must be unique and sorted by operation_id")
        claim_by_resource = {item.resource_id: item for item in self.resource_guards}
        if (
            any(not isinstance(item, MssqlAdmissionTargetHeadAuthority) for item in self.target_heads)
            or self.target_heads != tuple(sorted(self.target_heads))
            or len({item.target_resource_id for item in self.target_heads}) != len(self.target_heads)
        ):
            raise ValueError("target_heads must be a canonical typed resource closure")
        head_by_resource = {item.target_resource_id: item for item in self.target_heads}
        journal_targets: set[str] = set()
        for journal in self.journals:
            guard = claim_by_resource.get(journal.target_resource_id)
            if guard is None:
                raise ValueError("every journal target resource must be present in resource_guards")
            if journal.fencing_epoch != guard.fencing_epoch:
                raise ValueError("journal fencing_epoch must equal its target guard epoch")
            if journal.target_resource_id in journal_targets:
                raise ValueError("each journal must own a distinct target resource guard")
            head = head_by_resource.get(journal.target_resource_id)
            if (
                head is None
                or head.model_unique_id != journal.model_unique_id
                or head.target_generation_id != journal.target_predecessor_generation_id
            ):
                raise ValueError("target_heads must cover the exact journal predecessor closure")
            if head.terminal_receipt_sha256 is None and (
                head.head_authority_receipt_sha256 != journal.baseline_receipt_sha256
            ):
                raise ValueError("baseline target head authority must equal its baseline receipt")
            journal_targets.add(journal.target_resource_id)
        if set(head_by_resource) != journal_targets:
            raise ValueError("target_heads must not contain resources outside the journal closure")
        if self.successor_claim is not None and self.successor_claim.successor_workflow_id != self.workflow_id:
            raise ValueError("successor claim must bind this admission workflow_id")
        replacement_lineage = tuple(item.replaces_failed_operation_id for item in self.journals)
        if self.successor_claim is not None and any(item is None for item in replacement_lineage):
            raise ValueError("replacement admission journals must bind predecessor operations")
        if not self.target_owners:
            raise ValueError("target_owners must contain every selected mutating model")
        owner_model_ids = tuple(item.model_unique_id for item in self.target_owners)
        if owner_model_ids != tuple(sorted(owner_model_ids)) or len(owner_model_ids) != len(set(owner_model_ids)):
            raise ValueError("target_owners must be unique and sorted by model_unique_id")
        if owner_model_ids != tuple(sorted(item.model_unique_id for item in self.journals)):
            raise ValueError("target_owners must cover the exact journal model closure")
        prerequisite_model_ids = tuple(item.model_unique_id for item in self.prerequisite_authorities)
        if any(not isinstance(item, MssqlPrerequisiteAuthorityClaim) for item in self.prerequisite_authorities):
            raise ValueError("prerequisite authorities must be typed protected claims")
        if prerequisite_model_ids != owner_model_ids:
            raise ValueError("prerequisite authorities must cover the exact journal model closure")
        owner_deployments = {item.model_unique_id: item.deployment_id for item in self.target_owners}
        if any(item.deployment_id != owner_deployments[item.model_unique_id] for item in self.prerequisite_authorities):
            raise ValueError("prerequisite authority deployment differs from target ownership")
        target_authority_ids = tuple(item.target_authority_id for item in self.target_owners)
        if len(target_authority_ids) != len(set(target_authority_ids)):
            raise ValueError("each physical target authority must have one model owner")
        guard_set_sha256 = mssql_guard_set_sha256(self.workflow_guard, self.resource_guards)
        journal_set_sha256 = mssql_journal_set_sha256(self.journals)
        if guard_set_sha256 != self.expected_guard_set_sha256:
            raise ValueError("resource_guards do not match expected_guard_set_sha256 authority")
        if journal_set_sha256 != self.expected_journal_set_sha256:
            raise ValueError("journals do not match expected_journal_set_sha256 authority")
        object.__setattr__(self, "guard_set_sha256", guard_set_sha256)
        object.__setattr__(self, "journal_set_sha256", journal_set_sha256)


@dataclass(frozen=True, slots=True)
class MssqlAdmissionReceipt:
    """Durable identities acknowledged after the admission transaction commits."""

    workflow_id: str
    reservation_id: str
    guards: tuple[MssqlGuardClaim, ...]
    preparing_operation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MssqlWorkflowSuccessorClaim:
    """One immutable failed-workflow successor compare-and-set request."""

    predecessor_workflow_id: str
    predecessor_workflow_summary_sha256: str
    successor_workflow_id: str
    replacement_plan_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.predecessor_workflow_id, "predecessor_workflow_id")
        _require_digest(
            self.predecessor_workflow_summary_sha256,
            "predecessor_workflow_summary_sha256",
        )
        _require_text(self.successor_workflow_id, "successor_workflow_id")
        if self.predecessor_workflow_id == self.successor_workflow_id:
            raise ValueError("successor_workflow_id must differ from predecessor_workflow_id")
        _require_digest(self.replacement_plan_sha256, "replacement_plan_sha256")


class SemanticRefreshMssqlAdmissionPort(Protocol):
    """Atomic guard, binding, reservation and PREPARING-journal admission."""

    def admit(self, request: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
        """Commit all effects atomically; reconcile exact replay after acknowledgement loss."""


class SemanticRefreshMssqlPrerequisiteAuthorityPort(Protocol):
    """Revalidate a protected deployment receipt closure at point of use."""

    def require_current(self, claim: MssqlPrerequisiteAuthorityClaim) -> None:
        """Return only when route/runtime authorities remain current and exact."""


__all__ = [
    "MssqlAdmissionReceipt",
    "MssqlAdmissionRequest",
    "MssqlBuildReceiptEvidence",
    "MssqlGuardClaim",
    "MssqlImageEvidence",
    "MssqlImageKeyColumn",
    "MssqlJournalPreparation",
    "MssqlPrerequisiteAuthorityClaim",
    "MssqlRuntimeAssuranceClaim",
    "MssqlTargetOwnerClaim",
    "MssqlWorkflowResourceBudget",
    "MssqlOperationEvidence",
    "MssqlTransactionDisposition",
    "MssqlWorkflowSuccessorClaim",
    "SemanticRefreshMssqlAdmissionPort",
    "SemanticRefreshMssqlEvidencePort",
    "SemanticRefreshMssqlPrerequisiteAuthorityPort",
    "mssql_guard_set_sha256",
    "mssql_build_receipt_sha256",
    "mssql_image_key_columns_json",
    "mssql_journal_set_sha256",
    "mssql_operation_evidence_sha256",
    "mssql_session_evidence_sha256",
    "mssql_strategy_authority_sha256",
]
