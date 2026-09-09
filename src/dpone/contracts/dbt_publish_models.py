"""Immutable contracts shared by the dbt publish build plane."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy

PUBLISH_INTENT_SCHEMA = "dpone.dbt-publish-intent.v2"
COMPILE_REPORT_SCHEMA = "dpone.dbt-publish-compile.v2"


@dataclass(frozen=True, slots=True)
class DbtPublishIssue:
    code: str
    message: str
    path: str
    severity: str = "error"
    remediation: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "severity": self.severity,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        return payload


@dataclass(frozen=True, slots=True)
class DbtColumnArtifact:
    """One ordered dbt contract column without adapter-specific runtime objects."""

    name: str
    data_type: str
    nullable: bool
    constraints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DbtModelArtifact:
    unique_id: str
    name: str
    original_file_path: str
    database: str | None
    schema: str
    alias: str
    materialized: str
    contract_enforced: bool
    columns: tuple[str, ...]
    column_contracts: tuple[DbtColumnArtifact, ...]
    group: str | None
    tags: tuple[str, ...]
    meta: Mapping[str, Any]
    unique_key: tuple[str, ...]
    depends_on: tuple[str, ...]
    test_ids: tuple[str, ...] = ()
    fqn: tuple[str, ...] = ()
    incremental_strategy: str | None = None
    raw_code: str = ""
    compiled_code_by_target: Mapping[str, str] = field(default_factory=dict)
    macro_sources: Mapping[str, str] = field(default_factory=dict)
    semantic_refresh_macro_sources: Mapping[str, str] = field(default_factory=dict)
    semantic_refresh_macro_closure_complete: bool = True

    @property
    def fqn_selector(self) -> str:
        return ".".join(self.fqn)

    @property
    def relation(self) -> dict[str, str]:
        payload = {"schema": self.schema, "name": self.alias}
        if self.database:
            payload["database"] = self.database
        return payload


@dataclass(frozen=True, slots=True)
class DbtManifestArtifact:
    path: str
    sha256: str
    schema_version: int
    dbt_version: str | None
    invocation_id: str | None
    project_name: str | None
    models: tuple[DbtModelArtifact, ...]


@dataclass(frozen=True, slots=True)
class DbtPublishIntent:
    enabled: bool
    profile: str
    workflow: str
    target_schema: str | None = None
    target_table: str | None = None
    strategy_mode: str = "auto"
    unique_key: tuple[str, ...] = ()
    partition_key: str | None = None
    window_days: int | None = None
    physical_profile: str | None = None
    engine: str | None = None
    order_by: tuple[str, ...] = ()
    partition_by: str | None = None
    execution_profile: str | None = None
    max_parallelism: int | None = None
    quality_preset: str = "standard"
    lineage_enabled: bool = True
    schema: str = PUBLISH_INTENT_SCHEMA

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "schema": self.schema,
            "enabled": self.enabled,
            "profile": self.profile,
            "workflow": self.workflow,
            "target": {"schema": self.target_schema, "table": self.target_table},
            "strategy": {
                "mode": self.strategy_mode,
                "unique_key": list(self.unique_key),
                "partition_key": self.partition_key,
                "window_days": self.window_days,
            },
            "physical_design": {
                "profile": self.physical_profile,
                "engine": self.engine,
                "order_by": list(self.order_by),
                "partition_by": self.partition_by,
            },
            "execution": {"profile": self.execution_profile, "max_parallelism": self.max_parallelism},
            "quality": {"preset": self.quality_preset},
            "lineage": {"enabled": self.lineage_enabled},
        }
        return payload


@dataclass(frozen=True, slots=True)
class DbtRouteCertificationProfile:
    """Platform-owned coordinates for one certifiable route implementation."""

    transport: str
    schema_evolution: str
    airflow_runtime_mode: str


@dataclass(frozen=True, slots=True)
class DbtPublishProfile:
    name: str
    source_type: str
    source_connection_ref: str
    sink_type: str
    sink_connection_ref: str
    target_schema: str
    staging_schema: str | None
    runtime_image: str
    toolchain_id: str = DBT_SQLSERVER_1_12_CERTIFIED.contract_id
    certification: DbtRouteCertificationProfile | None = None
    state: Mapping[str, Any] = field(default_factory=dict)
    source_options: Mapping[str, Any] = field(default_factory=dict)
    sink_options: Mapping[str, Any] = field(default_factory=dict)
    runtime: Mapping[str, Any] = field(default_factory=dict)
    physical_design: Mapping[str, Any] = field(default_factory=dict)
    execution: Mapping[str, Any] = field(default_factory=dict)
    quality: Mapping[str, Any] = field(default_factory=dict)
    lineage: Mapping[str, Any] = field(default_factory=dict)
    semantic_refresh: SemanticRefreshProfilePolicy | None = None


@dataclass(frozen=True, slots=True)
class DbtPublishStrategyPolicy:
    """Environment-neutral destructive and atomic strategy guardrails."""

    allowed_strategies: tuple[str, ...]
    full_refresh_authorized: bool = False
    full_refresh_max_source_bytes: int | None = None
    partition_replace_requires_atomic_capability: bool = True

    def allows(self, strategy: str) -> bool:
        """Return whether policy permits capability evaluation for a strategy."""

        if strategy not in self.allowed_strategies:
            return False
        return strategy != "full_refresh" or self.full_refresh_authorized


@dataclass(frozen=True, slots=True)
class DbtWorkflowProfile:
    name: str
    schedule: str | None
    start_date: str
    timezone: str
    owner: str
    tags: tuple[str, ...]
    catchup: bool = False
    max_active_runs: int = 1


@dataclass(frozen=True, slots=True)
class CompiledDbtModel:
    model: DbtModelArtifact
    intent: DbtPublishIntent
    profile: DbtPublishProfile
    strategy: Mapping[str, Any]
    physical_design: Mapping[str, Any]
    workload_id: str
    manifest: Mapping[str, Any]
    route_capability: Mapping[str, Any] = field(default_factory=dict)
    warnings: tuple[DbtPublishIssue, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "model": self.model.unique_id,
            "model_path": self.model.original_file_path,
            "source_relation": self.model.relation,
            "intent": self.intent.to_jsonable(),
            "resolved_strategy": dict(self.strategy),
            "resolved_physical_design": dict(self.physical_design),
            "route_capability": dict(self.route_capability),
            "workload_id": self.workload_id,
            "warnings": [item.to_jsonable() for item in self.warnings],
        }
        if self.profile.semantic_refresh is not None:
            payload["semantic_refresh"] = {
                "workflow_mode": "normal",
                "model_archetype": "scope_stable_event_fact",
                "scope": "UTC day [start,end)",
                "static_dependency_closure": "UNVERIFIED",
                "runtime_dependency_closure": "RUNTIME_REQUIRED",
                "ephemeral_nodes": "none",
                "adapter_lifecycle_policy": "UNVERIFIED",
                "runtime_adapter_lifecycle": "RUNTIME_REQUIRED",
                "dbt_core": "1.12.3",
                "dbt_sqlserver": "1.11.1",
                "writer_assurance": "RUNTIME_REQUIRED",
                "source_side_pruning": "NOT_PROVEN",
                "event_time_policy": "immutable_effective_key_member",
                "effective_key_policy": "non_null_injective_exact",
                "utc_assurance": "RUNTIME_REQUIRED",
                "publication": "sequential_model_atomic",
                "recovery": "evidence_driven",
                "replay_mutation": "upsert_only",
                "row_removal": "unsupported",
                "profile_sha256": self.profile.semantic_refresh.profile_sha256,
            }
        return payload


@dataclass(frozen=True, slots=True)
class CompiledDbtWorkflow:
    workflow: str
    profile: DbtWorkflowProfile
    models: tuple[CompiledDbtModel, ...]
    dag_id: str


@dataclass(frozen=True, slots=True)
class DbtCompileReport:
    manifest_path: str
    manifest_schema_version: int | None
    manifest_sha256: str | None = None
    dbt_version: str | None = None
    dbt_adapter: str | None = None
    dbt_adapter_version: str | None = None
    release_id: str | None = None
    models: tuple[CompiledDbtModel, ...] = ()
    workflows: tuple[CompiledDbtWorkflow, ...] = ()
    warnings: tuple[DbtPublishIssue, ...] = ()
    blockers: tuple[DbtPublishIssue, ...] = ()
    artifacts: Mapping[str, str] = field(default_factory=dict)
    schema: str = COMPILE_REPORT_SCHEMA

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "schema": self.schema,
            "manifest_path": self.manifest_path,
            "manifest_schema_version": self.manifest_schema_version,
            "manifest_sha256": self.manifest_sha256,
            "dbt_version": self.dbt_version,
            "dbt_adapter": self.dbt_adapter,
            "dbt_adapter_version": self.dbt_adapter_version,
            "release_id": self.release_id,
            "passed": self.passed,
            "models": [item.to_jsonable() for item in self.models],
            "workflows": [
                {
                    "workflow": item.workflow,
                    "dag_id": item.dag_id,
                    "models": [model.workload_id for model in item.models],
                }
                for item in self.workflows
            ],
            "warnings": [item.to_jsonable() for item in self.warnings],
            "blockers": [item.to_jsonable() for item in self.blockers],
            "artifacts": dict(sorted(self.artifacts.items())),
        }
        identity_payload = {
            key: (_issue_identity(value) if key in {"warnings", "blockers"} else value)
            for key, value in payload.items()
            if key not in {"manifest_path", "artifacts"}
        }
        payload["compile_fingerprint"] = fingerprint(identity_payload)
        return payload


def fingerprint(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _issue_identity(value: object) -> object:
    if not isinstance(value, list):
        return value
    return [
        {key: item[key] for key in ("code", "message", "severity", "remediation") if key in item}
        if isinstance(item, dict)
        else item
        for item in value
    ]


__all__ = [
    "CompiledDbtModel",
    "CompiledDbtWorkflow",
    "DbtCompileReport",
    "DbtManifestArtifact",
    "DbtModelArtifact",
    "DbtPublishIntent",
    "DbtPublishIssue",
    "DbtPublishProfile",
    "DbtWorkflowProfile",
    "fingerprint",
]
