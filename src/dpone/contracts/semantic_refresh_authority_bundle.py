"""Attempt-free compiler/runtime authority bundle for semantic refresh V2."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_types import WorkflowMode

COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA = "dpone.semantic-refresh-compiler-runtime-authority-bundle.v1"
_DIGEST_FIELD = "authority_bundle_sha256"
_MODEL_FIELDS = frozenset(
    {
        "model_unique_id",
        "baseline_adoption_receipt_sha256",
        "model_definition_proof_sha256",
        "mutation_closure_sha256",
        "sqlserver_lifecycle_policy_sha256",
        "read_dependency_proof_sha256",
        "operation_id",
        "operation_plan_sha256",
    }
)
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "release_id",
        "deployment_id",
        "environment",
        "workflow_mode",
        "workflow_execution_id",
        "route_certification_receipt_sha256",
        "workflow_plan_sha256",
        "workflow_execution_binding_sha256",
        "compiler_policy_sha256",
        "runtime_policy_sha256",
        "models",
    }
)
_OPTIONAL_FIELDS = frozenset({"workflow_replacement_plan_sha256"})


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshModelAuthority:
    """Proof and operation closure for one model before any attempt exists."""

    model_unique_id: str
    baseline_adoption_receipt_sha256: str
    model_definition_proof_sha256: str
    mutation_closure_sha256: str
    sqlserver_lifecycle_policy_sha256: str
    read_dependency_proof_sha256: str
    operation_id: str
    operation_plan_sha256: str

    def __post_init__(self) -> None:
        require_text(self.model_unique_id, "model_authority.model_unique_id")
        for field in _MODEL_FIELDS - {"model_unique_id"}:
            require_digest(getattr(self, field), f"model_authority.{field}")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshModelAuthority:
        """Parse one closed model authority."""

        raw = require_closed_mapping(value, "model_authority", required=_MODEL_FIELDS)
        return cls(
            model_unique_id=require_text(raw.get("model_unique_id"), "model_authority.model_unique_id"),
            baseline_adoption_receipt_sha256=require_digest(
                raw.get("baseline_adoption_receipt_sha256"),
                "model_authority.baseline_adoption_receipt_sha256",
            ),
            model_definition_proof_sha256=require_digest(
                raw.get("model_definition_proof_sha256"),
                "model_authority.model_definition_proof_sha256",
            ),
            mutation_closure_sha256=require_digest(
                raw.get("mutation_closure_sha256"), "model_authority.mutation_closure_sha256"
            ),
            sqlserver_lifecycle_policy_sha256=require_digest(
                raw.get("sqlserver_lifecycle_policy_sha256"),
                "model_authority.sqlserver_lifecycle_policy_sha256",
            ),
            read_dependency_proof_sha256=require_digest(
                raw.get("read_dependency_proof_sha256"),
                "model_authority.read_dependency_proof_sha256",
            ),
            operation_id=require_digest(raw.get("operation_id"), "model_authority.operation_id"),
            operation_plan_sha256=require_digest(
                raw.get("operation_plan_sha256"), "model_authority.operation_plan_sha256"
            ),
        )

    def to_dict(self) -> dict[str, str]:
        """Return the canonical proof/operation closure."""

        return {field: getattr(self, field) for field in sorted(_MODEL_FIELDS)}


@dataclass(frozen=True, slots=True)
class SemanticRefreshCompilerRuntimeAuthorityBundle(SemanticRefreshDocumentCodec):
    """Compiler/runtime authority terminating at execution binding, never attempt facts."""

    release_id: str
    deployment_id: str
    environment: str
    workflow_mode: WorkflowMode
    workflow_execution_id: str
    route_certification_receipt_sha256: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    compiler_policy_sha256: str
    runtime_policy_sha256: str
    models: tuple[SemanticRefreshModelAuthority, ...]
    authority_bundle_sha256: str
    workflow_replacement_plan_sha256: str | None = None
    schema: str = COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA

    schema_id: ClassVar[str] = COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        for field in (
            "release_id",
            "deployment_id",
            "route_certification_receipt_sha256",
            "workflow_plan_sha256",
            "workflow_execution_binding_sha256",
            "compiler_policy_sha256",
            "runtime_policy_sha256",
        ):
            require_digest(getattr(self, field), field)
        require_text(self.environment, "environment")
        require_text(self.workflow_execution_id, "workflow_execution_id")
        if not isinstance(self.workflow_mode, WorkflowMode):
            raise SemanticRefreshContractError("workflow_mode is unsupported")
        if _canonical_models(self.models) != self.models:
            raise SemanticRefreshContractError("model authorities must be canonically ordered")
        if self.workflow_mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT:
            require_digest(self.workflow_replacement_plan_sha256, "workflow_replacement_plan_sha256")
        elif self.workflow_replacement_plan_sha256 is not None:
            raise SemanticRefreshContractError("non-replacement authority cannot reference a replacement plan")
        validate_digest(self._unsigned(), self.authority_bundle_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        release_id: str,
        deployment_id: str,
        environment: str,
        workflow_mode: WorkflowMode,
        workflow_execution_id: str,
        route_certification_receipt_sha256: str,
        workflow_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        compiler_policy_sha256: str,
        runtime_policy_sha256: str,
        models: tuple[SemanticRefreshModelAuthority, ...],
        workflow_replacement_plan_sha256: str | None = None,
    ) -> SemanticRefreshCompilerRuntimeAuthorityBundle:
        """Build an attempt-free authority with mode-conditional replacement binding."""

        ordered = _canonical_models(models)
        unsigned = _unsigned_mapping(
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            workflow_mode=workflow_mode,
            workflow_execution_id=workflow_execution_id,
            route_certification_receipt_sha256=route_certification_receipt_sha256,
            workflow_plan_sha256=workflow_plan_sha256,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            compiler_policy_sha256=compiler_policy_sha256,
            runtime_policy_sha256=runtime_policy_sha256,
            models=ordered,
            workflow_replacement_plan_sha256=workflow_replacement_plan_sha256,
        )
        return cls(
            release_id,
            deployment_id,
            environment,
            workflow_mode,
            workflow_execution_id,
            route_certification_receipt_sha256,
            workflow_plan_sha256,
            workflow_execution_binding_sha256,
            compiler_policy_sha256,
            runtime_policy_sha256,
            ordered,
            semantic_refresh_sha256(unsigned),
            workflow_replacement_plan_sha256,
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshCompilerRuntimeAuthorityBundle:
        """Parse a closed authority bundle and reject attempt/runtime extensions."""

        raw = require_closed_mapping(
            value, "compiler_runtime_authority_bundle", required=_FIELDS, optional=_OPTIONAL_FIELDS
        )
        raw_models = raw.get("models")
        if not isinstance(raw_models, Sequence) or isinstance(raw_models, str | bytes):
            raise SemanticRefreshContractError("models must be an array")
        replacement = raw.get("workflow_replacement_plan_sha256")
        if "workflow_replacement_plan_sha256" in raw and replacement is None:
            raise SemanticRefreshContractError("workflow_replacement_plan_sha256 cannot be null")
        return cls(
            release_id=require_digest(raw.get("release_id"), "release_id"),
            deployment_id=require_digest(raw.get("deployment_id"), "deployment_id"),
            environment=require_text(raw.get("environment"), "environment"),
            workflow_mode=require_enum(raw.get("workflow_mode"), "workflow_mode", WorkflowMode),
            workflow_execution_id=require_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            route_certification_receipt_sha256=require_digest(
                raw.get("route_certification_receipt_sha256"), "route_certification_receipt_sha256"
            ),
            workflow_plan_sha256=require_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            workflow_execution_binding_sha256=require_digest(
                raw.get("workflow_execution_binding_sha256"), "workflow_execution_binding_sha256"
            ),
            compiler_policy_sha256=require_digest(raw.get("compiler_policy_sha256"), "compiler_policy_sha256"),
            runtime_policy_sha256=require_digest(raw.get("runtime_policy_sha256"), "runtime_policy_sha256"),
            models=tuple(SemanticRefreshModelAuthority.from_mapping(item) for item in raw_models),
            authority_bundle_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            workflow_replacement_plan_sha256=(
                None if replacement is None else require_digest(replacement, "workflow_replacement_plan_sha256")
            ),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            environment=self.environment,
            workflow_mode=self.workflow_mode,
            workflow_execution_id=self.workflow_execution_id,
            route_certification_receipt_sha256=self.route_certification_receipt_sha256,
            workflow_plan_sha256=self.workflow_plan_sha256,
            workflow_execution_binding_sha256=self.workflow_execution_binding_sha256,
            compiler_policy_sha256=self.compiler_policy_sha256,
            runtime_policy_sha256=self.runtime_policy_sha256,
            models=self.models,
            workflow_replacement_plan_sha256=self.workflow_replacement_plan_sha256,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the pre-attempt authority mapping."""

        return {**self._unsigned(), self.digest_field: self.authority_bundle_sha256}


def _canonical_models(values: tuple[SemanticRefreshModelAuthority, ...]) -> tuple[SemanticRefreshModelAuthority, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(item, SemanticRefreshModelAuthority) for item in values)
    ):
        raise SemanticRefreshContractError("models must be a non-empty tuple")
    ordered = tuple(sorted(values, key=lambda item: item.model_unique_id))
    if len({item.model_unique_id for item in ordered}) != len(ordered):
        raise SemanticRefreshContractError("model authorities must be unique")
    return ordered


def _unsigned_mapping(
    *,
    release_id: str,
    deployment_id: str,
    environment: str,
    workflow_mode: WorkflowMode,
    workflow_execution_id: str,
    route_certification_receipt_sha256: str,
    workflow_plan_sha256: str,
    workflow_execution_binding_sha256: str,
    compiler_policy_sha256: str,
    runtime_policy_sha256: str,
    models: tuple[SemanticRefreshModelAuthority, ...],
    workflow_replacement_plan_sha256: str | None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "compiler_policy_sha256": compiler_policy_sha256,
        "deployment_id": deployment_id,
        "environment": environment,
        "models": [item.to_dict() for item in models],
        "release_id": release_id,
        "route_certification_receipt_sha256": route_certification_receipt_sha256,
        "runtime_policy_sha256": runtime_policy_sha256,
        "schema": COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA,
        "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        "workflow_execution_id": workflow_execution_id,
        "workflow_mode": workflow_mode.value,
        "workflow_plan_sha256": workflow_plan_sha256,
    }
    if workflow_replacement_plan_sha256 is not None:
        result["workflow_replacement_plan_sha256"] = workflow_replacement_plan_sha256
    return result


__all__ = [
    "COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA",
    "SemanticRefreshCompilerRuntimeAuthorityBundle",
    "SemanticRefreshModelAuthority",
]
