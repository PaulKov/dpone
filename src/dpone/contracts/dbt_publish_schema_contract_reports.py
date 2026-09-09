"""Published CLI, CI and promotion report schemas for dbt self-service."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.dbt_publish_schema_contract_promotion import (
    DIGEST,
    RELATIVE,
    TOKEN,
    nullable,
    object_schema,
    promotion_schema_contracts,
)
from dpone.contracts.dbt_publish_schema_contract_workspace import workspace_report_schema_contracts


def report_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return report schemas kept separate from executable runtime contracts."""

    return {
        **promotion_schema_contracts(),
        **workspace_report_schema_contracts(compile_report=_compile_report()),
        "dpone.dbt-publish-compile.v2": _compile_report(),
        "dpone.dbt-publish-explain.v1": _explain_report(),
        "dpone.dbt-publish-explain.v2": _explain_v2_report(),
    }


def _compile_report() -> dict[str, Any]:
    issue = {
        "type": "object",
        "required": ["code", "message", "path", "severity"],
        "additionalProperties": False,
        "properties": {
            "code": TOKEN,
            "message": TOKEN,
            "path": {"type": "string", "maxLength": 4096},
            "severity": {"enum": ["error", "warning"]},
            "remediation": TOKEN,
        },
    }
    model = object_schema(
        (
            "model",
            "model_path",
            "source_relation",
            "intent",
            "resolved_strategy",
            "resolved_physical_design",
            "route_capability",
            "workload_id",
            "warnings",
        ),
        {
            "model": TOKEN,
            "model_path": RELATIVE,
            "source_relation": {"type": "object"},
            "intent": {"type": "object"},
            "resolved_strategy": {"type": "object"},
            "resolved_physical_design": {"type": "object"},
            "route_capability": {"type": "object"},
            "workload_id": TOKEN,
            "warnings": {"type": "array", "items": deepcopy(issue)},
        },
    )
    workflow = object_schema(
        ("workflow", "dag_id", "models"),
        {
            "workflow": TOKEN,
            "dag_id": TOKEN,
            "models": _unique_tokens(),
        },
    )
    return object_schema(
        (
            "schema",
            "manifest_path",
            "manifest_schema_version",
            "manifest_sha256",
            "dbt_version",
            "dbt_adapter",
            "dbt_adapter_version",
            "release_id",
            "passed",
            "models",
            "workflows",
            "warnings",
            "blockers",
            "artifacts",
            "compile_fingerprint",
        ),
        {
            "schema": {"const": "dpone.dbt-publish-compile.v2"},
            "manifest_path": {"type": "string"},
            "manifest_schema_version": nullable({"type": "integer", "enum": [10, 11, 12]}),
            "manifest_sha256": nullable(DIGEST),
            "dbt_version": nullable(TOKEN),
            "dbt_adapter": nullable(TOKEN),
            "dbt_adapter_version": nullable(TOKEN),
            "release_id": nullable(DIGEST),
            "passed": {"type": "boolean"},
            "models": {"type": "array", "items": model},
            "workflows": {"type": "array", "items": workflow},
            "warnings": {"type": "array", "items": deepcopy(issue)},
            "blockers": {"type": "array", "items": deepcopy(issue)},
            "artifacts": {
                "type": "object",
                "additionalProperties": RELATIVE,
            },
            "compile_fingerprint": DIGEST,
        },
    )


def _explain_report() -> dict[str, Any]:
    relation = object_schema(
        ("schema", "name"),
        {
            "database": TOKEN,
            "schema": TOKEN,
            "name": TOKEN,
        },
    )
    return object_schema(
        (
            "schema",
            "model",
            "model_path",
            "source_relation",
            "intent",
            "resolved_strategy",
            "resolved_physical_design",
            "route_capability",
            "workload_id",
            "warnings",
        ),
        {
            "schema": {"const": "dpone.dbt-publish-explain.v1"},
            "model": TOKEN,
            "model_path": RELATIVE,
            "source_relation": relation,
            "intent": {"type": "object"},
            "resolved_strategy": {"type": "object"},
            "resolved_physical_design": {"type": "object"},
            "route_capability": {"type": "object"},
            "workload_id": TOKEN,
            "warnings": {"type": "array", "items": {"type": "object"}},
        },
    )


def _explain_v2_report() -> dict[str, Any]:
    schema = deepcopy(_explain_report())
    schema["required"].append("semantic_refresh")
    schema["properties"]["schema"] = {"const": "dpone.dbt-publish-explain.v2"}
    schema["properties"]["semantic_refresh"] = object_schema(
        (
            "workflow_mode",
            "model_archetype",
            "scope",
            "static_dependency_closure",
            "runtime_dependency_closure",
            "ephemeral_nodes",
            "adapter_lifecycle_policy",
            "runtime_adapter_lifecycle",
            "dbt_core",
            "dbt_sqlserver",
            "writer_assurance",
            "source_side_pruning",
            "event_time_policy",
            "effective_key_policy",
            "utc_assurance",
            "publication",
            "recovery",
            "replay_mutation",
            "row_removal",
            "profile_sha256",
        ),
        {
            "workflow_mode": {"enum": ["normal", "failed_precommit_replacement", "complete_scope_replay"]},
            "model_archetype": {"const": "scope_stable_event_fact"},
            "scope": {"const": "UTC day [start,end)"},
            "static_dependency_closure": {"enum": ["PROVEN", "NONCONFORMANT", "UNVERIFIED"]},
            "runtime_dependency_closure": {"enum": ["RUNTIME_REQUIRED", "PROVEN", "UNVERIFIED"]},
            "ephemeral_nodes": {"const": "none"},
            "adapter_lifecycle_policy": {"enum": ["FROZEN", "UNVERIFIED"]},
            "runtime_adapter_lifecycle": {"enum": ["RUNTIME_REQUIRED", "PROVEN", "UNVERIFIED"]},
            "dbt_core": {"const": "1.12.3"},
            "dbt_sqlserver": {"const": "1.11.1"},
            "writer_assurance": {"enum": ["PROVEN", "RUNTIME_REQUIRED", "UNVERIFIED"]},
            "source_side_pruning": {"enum": ["PROVEN", "NOT_PROVEN"]},
            "event_time_policy": {"const": "immutable_effective_key_member"},
            "effective_key_policy": {"const": "non_null_injective_exact"},
            "utc_assurance": {"enum": ["PROVEN", "RUNTIME_REQUIRED", "UNVERIFIED"]},
            "publication": {"const": "sequential_model_atomic"},
            "recovery": {"const": "evidence_driven"},
            "replay_mutation": {"const": "upsert_only"},
            "row_removal": {"const": "unsupported"},
            "profile_sha256": DIGEST,
        },
    )
    return schema


def _unique_tokens() -> dict[str, Any]:
    return {
        "type": "array",
        "uniqueItems": True,
        "items": TOKEN,
    }


__all__ = ["report_schema_contracts"]
