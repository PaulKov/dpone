"""Canonical semantic-refresh operation identity and plan."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_daily_scope,
    require_digest,
    require_enum,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_effective_key_identity import (
    semantic_refresh_effective_key_mapping_sha256,
    semantic_refresh_effective_key_template_sha256,
)
from dpone.contracts.semantic_refresh_operation_identity import (
    OPERATION_PLAN_OPTIONAL_FIELDS,
    OPERATION_PLAN_REQUIRED_FIELDS,
    operation_identity_payload,
    optional_digest,
    optional_positive_int,
    optional_text,
    parse_mutation_order,
    validate_effective_key,
    validate_operation_lineage,
)
from dpone.contracts.semantic_refresh_types import EffectiveKeyColumn, WorkflowMode

OPERATION_PLAN_SCHEMA = "dpone.semantic-refresh-operation-plan.v1"
SCOPE_STABLE_EVENT_FACT = "scope_stable_event_fact"
IGNORE_MISSING = "ignore_missing"
UPDATE_INSERT_ORDER = ("UPDATE", "INSERT")
_DIGEST_FIELD = "operation_plan_sha256"


@dataclass(frozen=True, slots=True)
class SemanticRefreshOperationPlan(SemanticRefreshDocumentCodec):
    """One daily immutable-key update/insert operation with pure identity."""

    operation_id: str
    model_unique_id: str
    release_id: str
    deployment_id: str
    environment: str
    workflow_id: str
    scope_family_id: str
    scope_revision: int
    target_predecessor_generation_id: str
    owner_generation: int
    platform_policy_digest: str
    resource_policy_digest: str
    writer_assurance_digest: str
    operation_kind: WorkflowMode
    model_definition_proof_sha256: str
    mutation_closure_sha256: str
    sqlserver_lifecycle_policy_sha256: str
    read_dependency_proof_sha256: str
    scope_start: str
    scope_end: str
    event_time_column: str
    effective_key_columns: tuple[EffectiveKeyColumn, ...]
    effective_key_template_sha256: str
    effective_key_mapping_sha256: str
    operation_plan_sha256: str
    scope_predecessor_operation_id: str | None = None
    replaces_failed_operation_id: str | None = None
    replacement_reason: str | None = None
    replacement_ordinal: int | None = None
    archetype: str = SCOPE_STABLE_EVENT_FACT
    missing_key_policy: str = IGNORE_MISSING
    mutation_order: tuple[str, str] = UPDATE_INSERT_ORDER
    schema: str = OPERATION_PLAN_SCHEMA

    schema_id: ClassVar[str] = OPERATION_PLAN_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        require_text(self.model_unique_id, "model_unique_id")
        for field_name in ("release_id", "deployment_id", "scope_family_id", "target_predecessor_generation_id"):
            require_digest(getattr(self, field_name), field_name)
        for field_name in ("platform_policy_digest", "resource_policy_digest", "writer_assurance_digest"):
            require_digest(getattr(self, field_name), field_name)
        for field_name in ("environment", "workflow_id"):
            require_text(getattr(self, field_name), field_name)
        require_positive_int(self.scope_revision, "scope_revision")
        require_positive_int(self.owner_generation, "owner_generation")
        if not isinstance(self.operation_kind, WorkflowMode):
            raise SemanticRefreshContractError("operation_kind is unsupported")
        for field_name in (
            "model_definition_proof_sha256",
            "mutation_closure_sha256",
            "sqlserver_lifecycle_policy_sha256",
            "read_dependency_proof_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)
        require_daily_scope(self.scope_start, self.scope_end)
        require_text(self.event_time_column, "event_time_column")
        if self.archetype != SCOPE_STABLE_EVENT_FACT:
            raise SemanticRefreshContractError("semantic-refresh archetype is unsupported")
        if self.missing_key_policy != IGNORE_MISSING:
            raise SemanticRefreshContractError("semantic refresh only supports ignore_missing")
        if self.mutation_order != UPDATE_INSERT_ORDER:
            raise SemanticRefreshContractError("semantic refresh mutation order must be UPDATE then INSERT")
        validate_effective_key(self.effective_key_columns, self.event_time_column)
        if self.effective_key_template_sha256 != semantic_refresh_effective_key_template_sha256(
            self.effective_key_columns
        ):
            raise SemanticRefreshContractError("effective_key_template_sha256 differs from ordered key semantics")
        if self.effective_key_mapping_sha256 != semantic_refresh_effective_key_mapping_sha256(
            self.effective_key_columns
        ):
            raise SemanticRefreshContractError("effective_key_mapping_sha256 differs from final ordered key mapping")
        validate_operation_lineage(
            operation_kind=self.operation_kind,
            scope_revision=self.scope_revision,
            scope_predecessor_operation_id=self.scope_predecessor_operation_id,
            replaces_failed_operation_id=self.replaces_failed_operation_id,
            replacement_reason=self.replacement_reason,
            replacement_ordinal=self.replacement_ordinal,
        )
        expected_operation_id = semantic_refresh_sha256(self._identity_payload())
        if require_digest(self.operation_id, "operation_id") != expected_operation_id:
            raise SemanticRefreshContractError("operation_id differs from semantic operation content")
        validate_digest(self._unsigned(), self.operation_plan_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        model_unique_id: str,
        release_id: str,
        deployment_id: str,
        environment: str,
        workflow_id: str,
        scope_family_id: str,
        scope_revision: int,
        target_predecessor_generation_id: str,
        owner_generation: int,
        platform_policy_digest: str,
        resource_policy_digest: str,
        writer_assurance_digest: str,
        operation_kind: WorkflowMode,
        model_definition_proof_sha256: str,
        mutation_closure_sha256: str,
        sqlserver_lifecycle_policy_sha256: str,
        read_dependency_proof_sha256: str,
        scope_start: str,
        scope_end: str,
        event_time_column: str,
        effective_key_columns: tuple[EffectiveKeyColumn, ...],
        scope_predecessor_operation_id: str | None = None,
        replaces_failed_operation_id: str | None = None,
        replacement_reason: str | None = None,
        replacement_ordinal: int | None = None,
    ) -> SemanticRefreshOperationPlan:
        """Build an operation identity first, then its plan digest."""

        effective_key_template_sha256 = semantic_refresh_effective_key_template_sha256(effective_key_columns)
        effective_key_mapping_sha256 = semantic_refresh_effective_key_mapping_sha256(effective_key_columns)
        identity = operation_identity_payload(
            schema=OPERATION_PLAN_SCHEMA,
            archetype=SCOPE_STABLE_EVENT_FACT,
            missing_key_policy=IGNORE_MISSING,
            mutation_order=UPDATE_INSERT_ORDER,
            model_unique_id=model_unique_id,
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            workflow_id=workflow_id,
            scope_family_id=scope_family_id,
            scope_revision=scope_revision,
            target_predecessor_generation_id=target_predecessor_generation_id,
            owner_generation=owner_generation,
            platform_policy_digest=platform_policy_digest,
            resource_policy_digest=resource_policy_digest,
            writer_assurance_digest=writer_assurance_digest,
            operation_kind=operation_kind,
            model_definition_proof_sha256=model_definition_proof_sha256,
            mutation_closure_sha256=mutation_closure_sha256,
            sqlserver_lifecycle_policy_sha256=sqlserver_lifecycle_policy_sha256,
            read_dependency_proof_sha256=read_dependency_proof_sha256,
            scope_start=scope_start,
            scope_end=scope_end,
            event_time_column=event_time_column,
            effective_key_columns=effective_key_columns,
            effective_key_template_sha256=effective_key_template_sha256,
            effective_key_mapping_sha256=effective_key_mapping_sha256,
            scope_predecessor_operation_id=scope_predecessor_operation_id,
            replaces_failed_operation_id=replaces_failed_operation_id,
            replacement_reason=replacement_reason,
            replacement_ordinal=replacement_ordinal,
        )
        operation_id = semantic_refresh_sha256(identity)
        unsigned = {**identity, "operation_id": operation_id}
        return cls(
            operation_id,
            model_unique_id,
            release_id,
            deployment_id,
            environment,
            workflow_id,
            scope_family_id,
            scope_revision,
            target_predecessor_generation_id,
            owner_generation,
            platform_policy_digest,
            resource_policy_digest,
            writer_assurance_digest,
            operation_kind,
            model_definition_proof_sha256,
            mutation_closure_sha256,
            sqlserver_lifecycle_policy_sha256,
            read_dependency_proof_sha256,
            scope_start,
            scope_end,
            event_time_column,
            effective_key_columns,
            effective_key_template_sha256,
            effective_key_mapping_sha256,
            semantic_refresh_sha256(unsigned),
            scope_predecessor_operation_id,
            replaces_failed_operation_id,
            replacement_reason,
            replacement_ordinal,
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshOperationPlan:
        """Parse a closed plan and recompute both operation and plan identities."""

        raw = require_closed_mapping(
            value,
            "operation_plan",
            required=OPERATION_PLAN_REQUIRED_FIELDS,
            optional=OPERATION_PLAN_OPTIONAL_FIELDS,
        )
        raw_keys = raw.get("effective_key_columns")
        if not isinstance(raw_keys, Sequence) or isinstance(raw_keys, str | bytes):
            raise SemanticRefreshContractError("effective_key_columns must be an array")
        raw_order = raw.get("mutation_order")
        if not isinstance(raw_order, list) or any(not isinstance(item, str) for item in raw_order):
            raise SemanticRefreshContractError("mutation_order must be an array of strings")
        return cls(
            operation_id=require_digest(raw.get("operation_id"), "operation_id"),
            model_unique_id=require_text(raw.get("model_unique_id"), "model_unique_id"),
            release_id=require_digest(raw.get("release_id"), "release_id"),
            deployment_id=require_digest(raw.get("deployment_id"), "deployment_id"),
            environment=require_text(raw.get("environment"), "environment"),
            workflow_id=require_text(raw.get("workflow_id"), "workflow_id"),
            scope_family_id=require_digest(raw.get("scope_family_id"), "scope_family_id"),
            scope_revision=require_positive_int(raw.get("scope_revision"), "scope_revision"),
            target_predecessor_generation_id=require_digest(
                raw.get("target_predecessor_generation_id"), "target_predecessor_generation_id"
            ),
            owner_generation=require_positive_int(raw.get("owner_generation"), "owner_generation"),
            platform_policy_digest=require_digest(raw.get("platform_policy_digest"), "platform_policy_digest"),
            resource_policy_digest=require_digest(raw.get("resource_policy_digest"), "resource_policy_digest"),
            writer_assurance_digest=require_digest(raw.get("writer_assurance_digest"), "writer_assurance_digest"),
            operation_kind=require_enum(raw.get("operation_kind"), "operation_kind", WorkflowMode),
            model_definition_proof_sha256=require_digest(
                raw.get("model_definition_proof_sha256"), "model_definition_proof_sha256"
            ),
            mutation_closure_sha256=require_digest(raw.get("mutation_closure_sha256"), "mutation_closure_sha256"),
            sqlserver_lifecycle_policy_sha256=require_digest(
                raw.get("sqlserver_lifecycle_policy_sha256"), "sqlserver_lifecycle_policy_sha256"
            ),
            read_dependency_proof_sha256=require_digest(
                raw.get("read_dependency_proof_sha256"), "read_dependency_proof_sha256"
            ),
            scope_start=require_text(raw.get("scope_start"), "scope_start"),
            scope_end=require_text(raw.get("scope_end"), "scope_end"),
            event_time_column=require_text(raw.get("event_time_column"), "event_time_column"),
            effective_key_columns=tuple(EffectiveKeyColumn.from_mapping(item) for item in raw_keys),
            effective_key_template_sha256=require_digest(
                raw.get("effective_key_template_sha256"), "effective_key_template_sha256"
            ),
            effective_key_mapping_sha256=require_digest(
                raw.get("effective_key_mapping_sha256"), "effective_key_mapping_sha256"
            ),
            operation_plan_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            scope_predecessor_operation_id=optional_digest(raw, "scope_predecessor_operation_id"),
            replaces_failed_operation_id=optional_digest(raw, "replaces_failed_operation_id"),
            replacement_reason=optional_text(raw, "replacement_reason"),
            replacement_ordinal=optional_positive_int(raw, "replacement_ordinal"),
            archetype=require_text(raw.get("archetype"), "archetype"),
            missing_key_policy=require_text(raw.get("missing_key_policy"), "missing_key_policy"),
            mutation_order=parse_mutation_order(raw_order),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _identity_payload(self) -> dict[str, object]:
        return operation_identity_payload(
            schema=self.schema_id,
            archetype=self.archetype,
            missing_key_policy=self.missing_key_policy,
            mutation_order=self.mutation_order,
            model_unique_id=self.model_unique_id,
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            environment=self.environment,
            workflow_id=self.workflow_id,
            scope_family_id=self.scope_family_id,
            scope_revision=self.scope_revision,
            target_predecessor_generation_id=self.target_predecessor_generation_id,
            owner_generation=self.owner_generation,
            platform_policy_digest=self.platform_policy_digest,
            resource_policy_digest=self.resource_policy_digest,
            writer_assurance_digest=self.writer_assurance_digest,
            operation_kind=self.operation_kind,
            model_definition_proof_sha256=self.model_definition_proof_sha256,
            mutation_closure_sha256=self.mutation_closure_sha256,
            sqlserver_lifecycle_policy_sha256=self.sqlserver_lifecycle_policy_sha256,
            read_dependency_proof_sha256=self.read_dependency_proof_sha256,
            scope_start=self.scope_start,
            scope_end=self.scope_end,
            event_time_column=self.event_time_column,
            effective_key_columns=self.effective_key_columns,
            effective_key_template_sha256=self.effective_key_template_sha256,
            effective_key_mapping_sha256=self.effective_key_mapping_sha256,
            scope_predecessor_operation_id=self.scope_predecessor_operation_id,
            replaces_failed_operation_id=self.replaces_failed_operation_id,
            replacement_reason=self.replacement_reason,
            replacement_ordinal=self.replacement_ordinal,
        )

    def _unsigned(self) -> dict[str, object]:
        return {**self._identity_payload(), "operation_id": self.operation_id}

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.operation_plan_sha256}


__all__ = [
    "IGNORE_MISSING",
    "OPERATION_PLAN_SCHEMA",
    "SCOPE_STABLE_EVENT_FACT",
    "SemanticRefreshOperationPlan",
    "UPDATE_INSERT_ORDER",
]
