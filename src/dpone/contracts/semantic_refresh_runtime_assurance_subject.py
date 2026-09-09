"""Exact protected subjects for semantic-refresh runtime assurance."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_text,
)

_REQUIRED = frozenset(
    {
        "assurance_kind",
        "subject_type",
        "release_id",
        "deployment_id",
        "environment",
        "database",
        "model_unique_id",
        "mssql_target_authority_id",
        "model_definition_proof_sha256",
        "effective_key_template_sha256",
        "writable_schema_sha256",
        "sqlserver_lifecycle_policy_sha256",
        "route_certification_receipt_sha256",
        "mssql_control_database",
        "mssql_control_schema",
        "mssql_image_schema",
        "scope_image_namespace_policy_sha256",
    }
)
_OPTIONAL = frozenset({"column_name"})


class RuntimeAssuranceKind(str, Enum):  # noqa: UP042
    """Closed assurance axes; every axis requires its own receipt identity."""

    WRITER_EXCLUSIVITY = "writer_exclusivity"
    UTC_SEMANTICS = "utc_semantics"
    DDL_FREEZE = "ddl_freeze"


class RuntimeAssuranceSubjectType(str, Enum):  # noqa: UP042
    """Exact protected target or one column on that target."""

    TARGET = "target"
    COLUMN = "column"


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshRuntimeAssuranceSubject:
    """Deployment-bound model subject protected by exact MSSQL policy coordinates."""

    assurance_kind: RuntimeAssuranceKind
    subject_type: RuntimeAssuranceSubjectType
    release_id: str
    deployment_id: str
    environment: str
    database: str
    model_unique_id: str
    mssql_target_authority_id: str
    model_definition_proof_sha256: str
    effective_key_template_sha256: str
    writable_schema_sha256: str
    sqlserver_lifecycle_policy_sha256: str
    route_certification_receipt_sha256: str
    mssql_control_database: str
    mssql_control_schema: str
    mssql_image_schema: str
    scope_image_namespace_policy_sha256: str
    column_name: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.assurance_kind, RuntimeAssuranceKind):
            raise SemanticRefreshContractError("assurance_kind is unsupported")
        if not isinstance(self.subject_type, RuntimeAssuranceSubjectType):
            raise SemanticRefreshContractError("subject_type is unsupported")
        for field in (
            "environment",
            "database",
            "model_unique_id",
            "mssql_target_authority_id",
            "mssql_control_database",
            "mssql_control_schema",
            "mssql_image_schema",
        ):
            require_text(getattr(self, field), f"subject.{field}")
        for field in (
            "release_id",
            "deployment_id",
            "model_definition_proof_sha256",
            "effective_key_template_sha256",
            "writable_schema_sha256",
            "sqlserver_lifecycle_policy_sha256",
            "route_certification_receipt_sha256",
            "scope_image_namespace_policy_sha256",
        ):
            require_digest(getattr(self, field), f"subject.{field}")
        expects_column = self.assurance_kind is RuntimeAssuranceKind.UTC_SEMANTICS
        if expects_column:
            if self.subject_type is not RuntimeAssuranceSubjectType.COLUMN or self.column_name is None:
                raise SemanticRefreshContractError("UTC semantics assurance requires an exact column subject")
            require_text(self.column_name, "subject.column_name")
        elif self.subject_type is not RuntimeAssuranceSubjectType.TARGET or self.column_name is not None:
            raise SemanticRefreshContractError("writer exclusivity and DDL freeze require an exact target subject")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshRuntimeAssuranceSubject:
        """Parse one closed subject branch."""

        raw = require_closed_mapping(value, "runtime_assurance_subject", required=_REQUIRED, optional=_OPTIONAL)
        if "column_name" in raw and raw["column_name"] is None:
            raise SemanticRefreshContractError("subject.column_name cannot be null")
        return cls(
            assurance_kind=require_enum(raw.get("assurance_kind"), "subject.assurance_kind", RuntimeAssuranceKind),
            subject_type=require_enum(raw.get("subject_type"), "subject.subject_type", RuntimeAssuranceSubjectType),
            release_id=require_digest(raw.get("release_id"), "subject.release_id"),
            deployment_id=require_digest(raw.get("deployment_id"), "subject.deployment_id"),
            environment=require_text(raw.get("environment"), "subject.environment"),
            database=require_text(raw.get("database"), "subject.database"),
            model_unique_id=require_text(raw.get("model_unique_id"), "subject.model_unique_id"),
            mssql_target_authority_id=require_text(
                raw.get("mssql_target_authority_id"), "subject.mssql_target_authority_id"
            ),
            model_definition_proof_sha256=require_digest(
                raw.get("model_definition_proof_sha256"), "subject.model_definition_proof_sha256"
            ),
            effective_key_template_sha256=require_digest(
                raw.get("effective_key_template_sha256"), "subject.effective_key_template_sha256"
            ),
            writable_schema_sha256=require_digest(raw.get("writable_schema_sha256"), "subject.writable_schema_sha256"),
            sqlserver_lifecycle_policy_sha256=require_digest(
                raw.get("sqlserver_lifecycle_policy_sha256"), "subject.sqlserver_lifecycle_policy_sha256"
            ),
            route_certification_receipt_sha256=require_digest(
                raw.get("route_certification_receipt_sha256"), "subject.route_certification_receipt_sha256"
            ),
            mssql_control_database=require_text(raw.get("mssql_control_database"), "subject.mssql_control_database"),
            mssql_control_schema=require_text(raw.get("mssql_control_schema"), "subject.mssql_control_schema"),
            mssql_image_schema=require_text(raw.get("mssql_image_schema"), "subject.mssql_image_schema"),
            scope_image_namespace_policy_sha256=require_digest(
                raw.get("scope_image_namespace_policy_sha256"), "subject.scope_image_namespace_policy_sha256"
            ),
            column_name=(require_text(raw["column_name"], "subject.column_name") if "column_name" in raw else None),
        )

    def to_dict(self) -> dict[str, object]:
        """Return exact deployment and subject coordinates."""

        result: dict[str, object] = {
            "assurance_kind": self.assurance_kind.value,
            "database": self.database,
            "deployment_id": self.deployment_id,
            "effective_key_template_sha256": self.effective_key_template_sha256,
            "environment": self.environment,
            "model_definition_proof_sha256": self.model_definition_proof_sha256,
            "model_unique_id": self.model_unique_id,
            "mssql_control_database": self.mssql_control_database,
            "mssql_control_schema": self.mssql_control_schema,
            "mssql_image_schema": self.mssql_image_schema,
            "mssql_target_authority_id": self.mssql_target_authority_id,
            "release_id": self.release_id,
            "route_certification_receipt_sha256": self.route_certification_receipt_sha256,
            "scope_image_namespace_policy_sha256": self.scope_image_namespace_policy_sha256,
            "sqlserver_lifecycle_policy_sha256": self.sqlserver_lifecycle_policy_sha256,
            "subject_type": self.subject_type.value,
            "writable_schema_sha256": self.writable_schema_sha256,
        }
        if self.column_name is not None:
            result["column_name"] = self.column_name
        return result


__all__ = [
    "RuntimeAssuranceKind",
    "RuntimeAssuranceSubjectType",
    "SemanticRefreshRuntimeAssuranceSubject",
]
