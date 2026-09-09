"""Invocation, selection, bundle and execution-pack schema fragments."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_invocation import (
    DBT_DYNAMIC_VARS,
    DBT_INDIRECT_SELECTION,
    DBT_INVOCATION_CONTEXT_SCHEMA,
    DBT_INVOCATION_ENVIRONMENT_POLICY,
    DBT_STATIC_ENVIRONMENT,
)
from dpone.contracts.dbt_publish_schema_contract_common import (
    DIGEST,
    RELATIVE,
    TOKEN,
    nonempty_tokens,
    object_schema,
)
from dpone.contracts.dbt_publishing import (
    SUPPORTED_DBT_ADAPTER_VERSION,
    SUPPORTED_DBT_CORE_VERSION,
    SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION,
    SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION,
)
from dpone.contracts.dbt_sqlserver_policy import (
    DBT_LOGIN_TIMEOUT_SECONDS,
    DBT_PROCESS_TIMEOUT_MAX_SECONDS,
    DBT_PROCESS_TIMEOUT_MIN_SECONDS,
    DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA,
    DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS,
    DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA,
)


def runtime_input_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return immutable dbt runtime-input contract bodies."""

    return {
        "dpone.dbt-invocation-context.v1": _invocation_context(),
        "dpone.dbt-selection-lock.v1": _selection_lock(),
        "dpone.dbt-project-bundle.v1": _project_bundle(),
        "dpone.dbt-execution-pack.v1": _execution_pack(),
        "dpone.dbt-execution-pack.v2": _execution_pack_v2(),
    }


def _selection_lock() -> dict[str, Any]:
    schema = object_schema(
        (
            "schema",
            "manifest_sha256",
            "toolchain_sha256",
            "invocation_context_sha256",
            "graph_policy_id",
            "graph_policy_sha256",
            "graph_contract_sha256",
            "selectors",
            "selected_graph_unique_ids",
            "expected_run_result_unique_ids",
            "publish_model_unique_ids",
            "selection_sha256",
        ),
        {
            "schema": {"const": "dpone.dbt-selection-lock.v1"},
            "manifest_sha256": DIGEST,
            "toolchain_sha256": DIGEST,
            "invocation_context_sha256": DIGEST,
            "graph_policy_id": TOKEN,
            "graph_policy_sha256": DIGEST,
            "graph_contract_sha256": DIGEST,
            "selectors": nonempty_tokens(),
            "selected_graph_unique_ids": nonempty_tokens(),
            "expected_run_result_unique_ids": nonempty_tokens(),
            "publish_model_unique_ids": nonempty_tokens(),
            "selection_sha256": DIGEST,
        },
    )
    schema["$comment"] = (
        "publish_model_unique_ids must be a subset of "
        "expected_run_result_unique_ids, which must be a subset of "
        "selected_graph_unique_ids; selection_sha256 must match the "
        "canonical semantic payload."
    )
    schema["x-dpone-semantic-invariants"] = [
        "expected_run_result_unique_ids_subset_of_selected_graph_unique_ids",
        "publish_model_unique_ids_subset_of_expected_run_result_unique_ids",
        "selection_sha256_matches_canonical_payload",
    ]
    return schema


def _project_bundle() -> dict[str, Any]:
    file_schema = object_schema(
        ("path", "sha256", "bytes"),
        {
            "path": RELATIVE,
            "sha256": DIGEST,
            "bytes": {"type": "integer", "minimum": 0},
        },
    )
    return object_schema(
        ("schema", "archive", "extracted_bytes", "files"),
        {
            "schema": {"const": "dpone.dbt-project-bundle.v1"},
            "archive": object_schema(
                ("format", "sha256", "bytes"),
                {
                    "format": {"const": "tar+gzip"},
                    "sha256": DIGEST,
                    "bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 268435456,
                    },
                },
            ),
            "extracted_bytes": {
                "type": "integer",
                "minimum": 0,
                "maximum": 1073741824,
            },
            "files": {
                "type": "array",
                "minItems": 1,
                "maxItems": 20000,
                "items": file_schema,
            },
        },
    )


def _invocation_context() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "environment_policy",
            "indirect_selection",
            "static_environment",
            "dynamic_vars",
            "invocation_context_sha256",
        ),
        {
            "schema": {"const": DBT_INVOCATION_CONTEXT_SCHEMA},
            "environment_policy": {"const": DBT_INVOCATION_ENVIRONMENT_POLICY},
            "indirect_selection": {"const": DBT_INDIRECT_SELECTION},
            "static_environment": object_schema(
                tuple(name for name, _value in DBT_STATIC_ENVIRONMENT),
                {name: {"const": value} for name, value in DBT_STATIC_ENVIRONMENT},
            ),
            "dynamic_vars": {
                "type": "array",
                "prefixItems": [{"const": name} for name in DBT_DYNAMIC_VARS],
                "items": False,
                "minItems": len(DBT_DYNAMIC_VARS),
                "maxItems": len(DBT_DYNAMIC_VARS),
            },
            "invocation_context_sha256": DIGEST,
        },
    )


def _execution_pack() -> dict[str, Any]:
    profile = object_schema(
        (
            "profile_name",
            "target_name",
            "connection_ref",
            "adapter_type",
            "database",
            "schema",
            "threads",
        ),
        {
            "profile_name": TOKEN,
            "target_name": TOKEN,
            "connection_ref": TOKEN,
            "adapter_type": TOKEN,
            "database": TOKEN,
            "schema": TOKEN,
            "threads": {"type": "integer", "minimum": 1},
        },
    )
    return object_schema(
        (
            "schema",
            "workflow_id",
            "project_bundle_sha256",
            "project_subdir",
            "target_path",
            "profile",
            "selection_lock",
            "invocation_context",
            "adapter_runtime",
            "adapter_policy",
            "dbt_core_version",
            "dbt_adapter_version",
            "manifest_schema_version",
            "run_results_schema_version",
            "dbt_warning_policy",
            "timeout_seconds",
            "pack_sha256",
        ),
        {
            "schema": {"const": "dpone.dbt-execution-pack.v1"},
            "workflow_id": {
                "type": "string",
                "pattern": "^[a-z][a-z0-9_]{0,63}$",
            },
            "project_bundle_sha256": DIGEST,
            "project_subdir": RELATIVE,
            "target_path": RELATIVE,
            "profile": profile,
            "selection_lock": _selection_lock(),
            "invocation_context": _invocation_context(),
            "adapter_runtime": sqlserver_runtime_policy_schema(),
            "adapter_policy": sqlserver_adapter_policy_schema(),
            "dbt_core_version": {"const": SUPPORTED_DBT_CORE_VERSION},
            "dbt_adapter_version": {
                "const": SUPPORTED_DBT_ADAPTER_VERSION,
            },
            "manifest_schema_version": {
                "const": SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION,
            },
            "run_results_schema_version": {
                "const": SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION,
            },
            "dbt_warning_policy": {"enum": ["fail", "allow"]},
            "timeout_seconds": {
                "type": "integer",
                "minimum": DBT_PROCESS_TIMEOUT_MIN_SECONDS,
                "maximum": DBT_PROCESS_TIMEOUT_MAX_SECONDS,
            },
            "pack_sha256": DIGEST,
        },
    )


def _execution_pack_v2() -> dict[str, Any]:
    """Add a required base target without widening the shipped v1 schema."""

    schema = _execution_pack()
    schema["properties"]["schema"] = {"const": "dpone.dbt-execution-pack.v2"}
    schema["required"].append("invocation_target")
    schema["properties"]["invocation_target"] = object_schema(
        ("database", "schema"), {"database": TOKEN, "schema": TOKEN}
    )
    return schema


def sqlserver_runtime_policy_schema() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "backend",
            "retries",
            "login_timeout_seconds",
            "query_timeout_seconds",
            "adapter_runtime_sha256",
        ),
        {
            "schema": {"const": DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA},
            "backend": {"const": "pyodbc"},
            "retries": {"const": 1},
            "login_timeout_seconds": {"const": DBT_LOGIN_TIMEOUT_SECONDS},
            "query_timeout_seconds": {
                "type": "integer",
                "minimum": DBT_PROCESS_TIMEOUT_MIN_SECONDS - 300,
                "maximum": DBT_PROCESS_TIMEOUT_MAX_SECONDS - 300,
            },
            "adapter_runtime_sha256": DIGEST,
        },
    )


def sqlserver_adapter_policy_schema() -> dict[str, Any]:
    flags = dict(DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS)
    return object_schema(
        ("schema", "required_project_flags", "adapter_policy_sha256"),
        {
            "schema": {"const": DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA},
            "required_project_flags": object_schema(
                tuple(sorted(flags)),
                {name: {"const": flags[name]} for name in sorted(flags)},
            ),
            "adapter_policy_sha256": DIGEST,
        },
    )


__all__ = [
    "runtime_input_schema_contracts",
    "sqlserver_adapter_policy_schema",
    "sqlserver_runtime_policy_schema",
]
