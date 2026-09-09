"""Pinned dbt-sqlserver lifecycle authority for semantic refresh V2."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from dpone.contracts.dbt_contract_validation import canonical_fingerprint, require_digest
from dpone.contracts.dbt_semantic_refresh_common import (
    SemanticRefreshProofIssue,
    nonconformant,
    proof_status,
    unverified,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle_options import (
    EXPECTED_ADAPTER_OPTIONS,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle_report import (
    SQLSERVER_LIFECYCLE_REPORT_SCHEMA,
    SemanticRefreshLifecycleReport,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import (
    SemanticRefreshSqlServerLifecyclePolicy,
)

SQLSERVER_LIFECYCLE_POLICY_SCHEMA = "dpone.semantic-refresh-sqlserver-lifecycle-policy.v1"
DBT_CORE_VERSION = "1.12.3"
DBT_SQLSERVER_VERSION = "1.11.1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshLifecycleLock:
    """Complete certified toolchain and SQL Server deployment coordinate."""

    python_version: str
    image_digest: str
    pyodbc_version: str
    driver_name: str
    driver_version: str
    sqlserver_version: str
    compatibility_level: int
    package_artifacts_sha256: str
    materialization_sha256: str
    macro_closure_sha256: str
    dispatch_sha256: str
    project_flags_sha256: str
    policy_digests: tuple[str, ...]
    certification_coordinate_sha256: str
    dbt_core_version: str = DBT_CORE_VERSION
    dbt_sqlserver_version: str = DBT_SQLSERVER_VERSION

    def __post_init__(self) -> None:
        if self.dbt_core_version != DBT_CORE_VERSION or self.dbt_sqlserver_version != DBT_SQLSERVER_VERSION:
            raise ValueError("semantic refresh requires the pinned dbt toolchain")
        for field in (
            "python_version",
            "pyodbc_version",
            "driver_name",
            "driver_version",
            "sqlserver_version",
        ):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be non-empty")
        if (
            isinstance(self.compatibility_level, bool)
            or not isinstance(self.compatibility_level, int)
            or self.compatibility_level <= 0
        ):
            raise ValueError("compatibility_level must be a positive integer")
        for field in (
            "image_digest",
            "package_artifacts_sha256",
            "materialization_sha256",
            "macro_closure_sha256",
            "dispatch_sha256",
            "project_flags_sha256",
            "certification_coordinate_sha256",
        ):
            require_digest(getattr(self, field), field, "DPONE_DBT_V2_LIFECYCLE_LOCK_INVALID")
        if (
            not isinstance(self.policy_digests, tuple)
            or not self.policy_digests
            or len(self.policy_digests) != len(set(self.policy_digests))
            or tuple(sorted(self.policy_digests)) != self.policy_digests
        ):
            raise ValueError("policy_digests must be a sorted non-empty unique tuple")
        for digest in self.policy_digests:
            require_digest(digest, "policy_digests", "DPONE_DBT_V2_LIFECYCLE_LOCK_INVALID")
        if self.certification_coordinate_sha256 != canonical_fingerprint(self._coordinate_payload()):
            raise ValueError("certification coordinate does not match its pinned content")

    @classmethod
    def create(
        cls,
        *,
        python_version: str,
        image_digest: str,
        pyodbc_version: str,
        driver_name: str,
        driver_version: str,
        sqlserver_version: str,
        compatibility_level: int,
        package_artifacts_sha256: str,
        materialization_sha256: str,
        macro_closure_sha256: str,
        dispatch_sha256: str,
        project_flags_sha256: str,
        policy_digests: tuple[str, ...],
    ) -> SemanticRefreshLifecycleLock:
        """Create a lock only when every certification coordinate is explicit."""

        values: dict[str, Any] = {
            "python_version": python_version,
            "image_digest": image_digest,
            "pyodbc_version": pyodbc_version,
            "driver_name": driver_name,
            "driver_version": driver_version,
            "sqlserver_version": sqlserver_version,
            "compatibility_level": compatibility_level,
            "package_artifacts_sha256": package_artifacts_sha256,
            "materialization_sha256": materialization_sha256,
            "macro_closure_sha256": macro_closure_sha256,
            "dispatch_sha256": dispatch_sha256,
            "project_flags_sha256": project_flags_sha256,
            "policy_digests": tuple(sorted(policy_digests)),
            "dbt_core_version": DBT_CORE_VERSION,
            "dbt_sqlserver_version": DBT_SQLSERVER_VERSION,
        }
        coordinate = canonical_fingerprint(_coordinate_payload(values))
        return cls(**values, certification_coordinate_sha256=coordinate)

    def _coordinate_payload(self) -> dict[str, Any]:
        return _coordinate_payload(
            {
                field: getattr(self, field)
                for field in (
                    "python_version",
                    "image_digest",
                    "pyodbc_version",
                    "driver_name",
                    "driver_version",
                    "sqlserver_version",
                    "compatibility_level",
                    "package_artifacts_sha256",
                    "materialization_sha256",
                    "macro_closure_sha256",
                    "dispatch_sha256",
                    "project_flags_sha256",
                    "policy_digests",
                    "dbt_core_version",
                    "dbt_sqlserver_version",
                )
            }
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshLifecycleObservation:
    """Mutation-sensitive adapter facts projected by the pinned producer."""

    certification_coordinate_sha256: str | None
    dbt_core_version: str
    dbt_sqlserver_version: str
    target_exists: bool | None
    target_relation_type: str | None
    adapter_branch: str | None
    full_refresh: bool | None
    intermediate_relation_absent: bool | None
    backup_relation_absent: bool | None
    adapter_options: Mapping[str, object]
    pre_begin_target_mutations: tuple[str, ...]
    in_transaction_target_mutations: tuple[str, ...]
    post_strategy_target_mutations: tuple[str, ...]
    post_commit_mutations: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.adapter_options, Mapping):
            raise ValueError("adapter_options must be an object")
        object.__setattr__(self, "adapter_options", MappingProxyType(dict(self.adapter_options)))


def evaluate_semantic_refresh_lifecycle(
    lock: SemanticRefreshLifecycleLock,
    observation: SemanticRefreshLifecycleObservation,
    *,
    lifecycle_policy: SemanticRefreshSqlServerLifecyclePolicy,
) -> SemanticRefreshLifecycleReport:
    """Admit only the pinned existing-table incremental lifecycle."""

    issues: list[SemanticRefreshProofIssue] = []
    _compare(issues, observation, "certification_coordinate_sha256", lock.certification_coordinate_sha256)
    _compare(issues, observation, "dbt_core_version", DBT_CORE_VERSION)
    _compare(issues, observation, "dbt_sqlserver_version", DBT_SQLSERVER_VERSION)
    _compare(issues, observation, "target_exists", True)
    _compare(issues, observation, "target_relation_type", "table")
    _compare(issues, observation, "adapter_branch", "existing_table_incremental")
    _compare(issues, observation, "full_refresh", False)
    _compare(issues, observation, "intermediate_relation_absent", True)
    _compare(issues, observation, "backup_relation_absent", True)
    _compare(issues, observation, "pre_begin_target_mutations", ())
    _compare(issues, observation, "in_transaction_target_mutations", ("dpone_scope_merge",))
    _compare(issues, observation, "post_strategy_target_mutations", ("adapter_commit",))
    _compare(issues, observation, "post_commit_mutations", ("drop_attempt_local_temp",))
    _canonical_policy_issues(lock, lifecycle_policy, issues)
    _adapter_option_issues(observation.adapter_options, issues)
    ordered = tuple(sorted(issues, key=lambda issue: (issue.field, issue.code)))
    policy_payload = {
        "schema": SQLSERVER_LIFECYCLE_POLICY_SCHEMA,
        "certification_coordinate_sha256": lock.certification_coordinate_sha256,
        "adapter_options": _jsonable_options(EXPECTED_ADAPTER_OPTIONS),
        "phases": {
            "pre_begin_target_mutations": [],
            "in_transaction_target_mutations": ["dpone_scope_merge"],
            "post_strategy_target_mutations": ["adapter_commit"],
            "post_commit_mutations": ["drop_attempt_local_temp"],
        },
    }
    status = proof_status(ordered)
    lifecycle_authority_sha256 = canonical_fingerprint(policy_payload)
    return SemanticRefreshLifecycleReport.build(
        status=status,
        lifecycle_policy_sha256=lifecycle_policy.sqlserver_lifecycle_policy_sha256,
        lifecycle_authority_sha256=lifecycle_authority_sha256,
        certification_coordinate_sha256=lock.certification_coordinate_sha256,
        issues=ordered,
    )


def _canonical_policy_issues(
    lock: SemanticRefreshLifecycleLock,
    policy: SemanticRefreshSqlServerLifecyclePolicy,
    issues: list[SemanticRefreshProofIssue],
) -> None:
    if not isinstance(policy, SemanticRefreshSqlServerLifecyclePolicy):
        issues.append(
            unverified(
                "DPONE_DBT_V2_LIFECYCLE_UNVERIFIED",
                "lifecycle_policy",
                "the canonical SQL Server lifecycle policy is unavailable",
            )
        )
        return
    expected = {
        "python_version": policy.python_version,
        "image_digest": policy.runtime_image_digest,
        "pyodbc_version": policy.pyodbc_version,
        "driver_name": policy.odbc_driver,
        "sqlserver_version": policy.sqlserver_version,
        "compatibility_level": policy.compatibility_level,
        "package_artifacts_sha256": policy.package_artifacts_digest,
        "materialization_sha256": policy.materialization_closure_digest,
        "macro_closure_sha256": policy.macro_closure_sha256,
        "dispatch_sha256": policy.dispatch_closure_digest,
        "project_flags_sha256": policy.project_policy_digest,
        "policy_digests": tuple(
            sorted(
                (
                    policy.adapter_policy_digest,
                    policy.profile_policy_digest,
                    policy.invocation_policy_digest,
                    policy.driver_digest,
                )
            )
        ),
    }
    for field, value in expected.items():
        if getattr(lock, field) != value:
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_LIFECYCLE_POLICY_MISMATCH",
                    field,
                    "the pinned lifecycle lock differs from the canonical policy",
                )
            )


def _coordinate_payload(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": SQLSERVER_LIFECYCLE_POLICY_SCHEMA,
        **{key: list(value) if key == "policy_digests" else value for key, value in values.items()},
    }


def _compare(
    issues: list[SemanticRefreshProofIssue],
    observation: SemanticRefreshLifecycleObservation,
    field: str,
    expected: object,
) -> None:
    actual = getattr(observation, field)
    if actual is None:
        issues.append(
            unverified(
                "DPONE_DBT_V2_LIFECYCLE_UNVERIFIED",
                field,
                "the mutation-sensitive lifecycle observation is unavailable",
            )
        )
    elif actual != expected:
        issues.append(
            nonconformant(
                "DPONE_DBT_V2_LIFECYCLE_NONCONFORMANT",
                field,
                "the adapter lifecycle differs from the pinned authority",
            )
        )


def _adapter_option_issues(
    options: Mapping[str, object],
    issues: list[SemanticRefreshProofIssue],
) -> None:
    for field in sorted(set(options) | set(EXPECTED_ADAPTER_OPTIONS)):
        if (
            field not in EXPECTED_ADAPTER_OPTIONS
            or field not in options
            or options[field] != EXPECTED_ADAPTER_OPTIONS.get(field)
        ):
            issues.append(
                nonconformant(
                    "DPONE_DBT_V2_ADAPTER_OPTION_UNSUPPORTED",
                    f"adapter_options.{field}",
                    "the mutation-sensitive adapter option is absent, unknown or noncanonical",
                )
            )


def _jsonable_options(options: Mapping[str, object]) -> dict[str, object]:
    return {key: list(value) if isinstance(value, tuple) else value for key, value in options.items()}


__all__ = [
    "DBT_CORE_VERSION",
    "DBT_SQLSERVER_VERSION",
    "SQLSERVER_LIFECYCLE_POLICY_SCHEMA",
    "SQLSERVER_LIFECYCLE_REPORT_SCHEMA",
    "SemanticRefreshLifecycleLock",
    "SemanticRefreshLifecycleObservation",
    "SemanticRefreshLifecycleReport",
    "evaluate_semantic_refresh_lifecycle",
]
