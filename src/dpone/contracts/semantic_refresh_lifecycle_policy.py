"""Pinned SQL Server/dbt lifecycle policy for semantic refresh."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)

SQLSERVER_LIFECYCLE_POLICY_SCHEMA = "dpone.semantic-refresh-sqlserver-lifecycle-policy.v1"
DBT_CORE_VERSION = "1.12.3"
DBT_SQLSERVER_VERSION = "1.11.1"
_DIGEST_FIELD = "sqlserver_lifecycle_policy_sha256"
_FIXED_VALUES: dict[str, object] = {
    "contract_enforced": True,
    "dbt_core_version": DBT_CORE_VERSION,
    "dbt_sqlserver_version": DBT_SQLSERVER_VERSION,
    "existing_table_required": True,
    "full_refresh_allowed": False,
    "hooks_allowed": False,
    "incremental_strategy": "dpone_scope_merge",
    "materialization": "incremental",
    "on_schema_change": "fail",
    "schema_mutation_allowed": False,
}
_VARIABLE_FIELDS = frozenset(
    {
        "python_version",
        "runtime_image_digest",
        "pyodbc_version",
        "odbc_driver",
        "sqlserver_version",
        "compatibility_level",
        "macro_closure_sha256",
        "adapter_policy_digest",
        "project_policy_digest",
        "profile_policy_digest",
        "invocation_policy_digest",
        "package_artifacts_digest",
        "materialization_closure_digest",
        "dispatch_closure_digest",
        "driver_digest",
    }
)
_FIELDS = frozenset({"schema", _DIGEST_FIELD, *_FIXED_VALUES, *_VARIABLE_FIELDS})


@dataclass(frozen=True, slots=True)
class SemanticRefreshSqlServerLifecyclePolicy(SemanticRefreshDocumentCodec):
    """Frozen adapter tuple and the only admitted incremental branch."""

    python_version: str
    runtime_image_digest: str
    pyodbc_version: str
    odbc_driver: str
    sqlserver_version: str
    compatibility_level: int
    macro_closure_sha256: str
    adapter_policy_digest: str
    project_policy_digest: str
    profile_policy_digest: str
    invocation_policy_digest: str
    package_artifacts_digest: str
    materialization_closure_digest: str
    dispatch_closure_digest: str
    driver_digest: str
    sqlserver_lifecycle_policy_sha256: str
    schema: str = SQLSERVER_LIFECYCLE_POLICY_SCHEMA

    schema_id: ClassVar[str] = SQLSERVER_LIFECYCLE_POLICY_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        for field_name in (
            "python_version",
            "pyodbc_version",
            "odbc_driver",
            "sqlserver_version",
        ):
            require_text(getattr(self, field_name), field_name)
        require_digest(self.runtime_image_digest, "runtime_image_digest")
        for field_name in (
            "runtime_image_digest",
            "macro_closure_sha256",
            "adapter_policy_digest",
            "project_policy_digest",
            "profile_policy_digest",
            "invocation_policy_digest",
            "package_artifacts_digest",
            "materialization_closure_digest",
            "dispatch_closure_digest",
            "driver_digest",
        ):
            require_digest(getattr(self, field_name), field_name)
        require_positive_int(self.compatibility_level, "compatibility_level")
        validate_digest(self._unsigned(), self.sqlserver_lifecycle_policy_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        python_version: str,
        runtime_image_digest: str,
        pyodbc_version: str,
        odbc_driver: str,
        sqlserver_version: str,
        compatibility_level: int,
        macro_closure_sha256: str,
        adapter_policy_digest: str,
        project_policy_digest: str,
        profile_policy_digest: str,
        invocation_policy_digest: str,
        package_artifacts_digest: str,
        materialization_closure_digest: str,
        dispatch_closure_digest: str,
        driver_digest: str,
    ) -> SemanticRefreshSqlServerLifecyclePolicy:
        """Build the pinned policy with its canonical digest."""

        values = (
            python_version,
            runtime_image_digest,
            pyodbc_version,
            odbc_driver,
            sqlserver_version,
            compatibility_level,
            macro_closure_sha256,
            adapter_policy_digest,
            project_policy_digest,
            profile_policy_digest,
            invocation_policy_digest,
            package_artifacts_digest,
            materialization_closure_digest,
            dispatch_closure_digest,
            driver_digest,
        )
        return cls(*values, semantic_refresh_sha256(_unsigned_mapping(*values)))

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshSqlServerLifecyclePolicy:
        """Parse a strict lifecycle policy and enforce the certified branch."""

        raw = require_closed_mapping(value, "sqlserver_lifecycle_policy", required=_FIELDS)
        for field_name, expected in _FIXED_VALUES.items():
            if raw.get(field_name) != expected:
                raise SemanticRefreshContractError(f"{field_name} differs from the certified lifecycle")
        return cls(
            python_version=require_text(raw.get("python_version"), "python_version"),
            runtime_image_digest=require_digest(raw.get("runtime_image_digest"), "runtime_image_digest"),
            pyodbc_version=require_text(raw.get("pyodbc_version"), "pyodbc_version"),
            odbc_driver=require_text(raw.get("odbc_driver"), "odbc_driver"),
            sqlserver_version=require_text(raw.get("sqlserver_version"), "sqlserver_version"),
            compatibility_level=require_positive_int(raw.get("compatibility_level"), "compatibility_level"),
            macro_closure_sha256=require_digest(raw.get("macro_closure_sha256"), "macro_closure_sha256"),
            adapter_policy_digest=require_digest(raw.get("adapter_policy_digest"), "adapter_policy_digest"),
            project_policy_digest=require_digest(raw.get("project_policy_digest"), "project_policy_digest"),
            profile_policy_digest=require_digest(raw.get("profile_policy_digest"), "profile_policy_digest"),
            invocation_policy_digest=require_digest(raw.get("invocation_policy_digest"), "invocation_policy_digest"),
            package_artifacts_digest=require_digest(raw.get("package_artifacts_digest"), "package_artifacts_digest"),
            materialization_closure_digest=require_digest(
                raw.get("materialization_closure_digest"), "materialization_closure_digest"
            ),
            dispatch_closure_digest=require_digest(raw.get("dispatch_closure_digest"), "dispatch_closure_digest"),
            driver_digest=require_digest(raw.get("driver_digest"), "driver_digest"),
            sqlserver_lifecycle_policy_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.python_version,
            self.runtime_image_digest,
            self.pyodbc_version,
            self.odbc_driver,
            self.sqlserver_version,
            self.compatibility_level,
            self.macro_closure_sha256,
            self.adapter_policy_digest,
            self.project_policy_digest,
            self.profile_policy_digest,
            self.invocation_policy_digest,
            self.package_artifacts_digest,
            self.materialization_closure_digest,
            self.dispatch_closure_digest,
            self.driver_digest,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.sqlserver_lifecycle_policy_sha256}


def _unsigned_mapping(
    python_version: str,
    runtime_image_digest: str,
    pyodbc_version: str,
    odbc_driver: str,
    sqlserver_version: str,
    compatibility_level: int,
    macro_closure_sha256: str,
    adapter_policy_digest: str,
    project_policy_digest: str,
    profile_policy_digest: str,
    invocation_policy_digest: str,
    package_artifacts_digest: str,
    materialization_closure_digest: str,
    dispatch_closure_digest: str,
    driver_digest: str,
) -> dict[str, object]:
    return {
        **_FIXED_VALUES,
        "compatibility_level": compatibility_level,
        "adapter_policy_digest": adapter_policy_digest,
        "dispatch_closure_digest": dispatch_closure_digest,
        "driver_digest": driver_digest,
        "invocation_policy_digest": invocation_policy_digest,
        "macro_closure_sha256": macro_closure_sha256,
        "materialization_closure_digest": materialization_closure_digest,
        "odbc_driver": odbc_driver,
        "package_artifacts_digest": package_artifacts_digest,
        "profile_policy_digest": profile_policy_digest,
        "project_policy_digest": project_policy_digest,
        "pyodbc_version": pyodbc_version,
        "python_version": python_version,
        "runtime_image_digest": runtime_image_digest,
        "schema": SQLSERVER_LIFECYCLE_POLICY_SCHEMA,
        "sqlserver_version": sqlserver_version,
    }


__all__ = [
    "DBT_CORE_VERSION",
    "DBT_SQLSERVER_VERSION",
    "SQLSERVER_LIFECYCLE_POLICY_SCHEMA",
    "SemanticRefreshSqlServerLifecyclePolicy",
]
