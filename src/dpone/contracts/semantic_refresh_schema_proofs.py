"""Generated schema definitions for semantic-refresh proofs and policy."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_lifecycle_policy import (
    DBT_CORE_VERSION,
    DBT_SQLSERVER_VERSION,
    SQLSERVER_LIFECYCLE_POLICY_SCHEMA,
)
from dpone.contracts.semantic_refresh_model_proof import MODEL_DEFINITION_PROOF_SCHEMA
from dpone.contracts.semantic_refresh_mutation_closure import MUTATION_CLOSURE_SCHEMA
from dpone.contracts.semantic_refresh_read_dependency import (
    READ_DEPENDENCY_PROOF_SCHEMA,
    ReadDependencyKind,
)
from dpone.contracts.semantic_refresh_schema_common import (
    CLOSURE_STATUS_SCHEMA,
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    closed_schema,
    schema_discriminator,
    string_set_schema,
)


def proof_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh deterministic schemas for four proof/policy documents."""

    return {
        MODEL_DEFINITION_PROOF_SCHEMA: _model_definition_proof_schema(),
        MUTATION_CLOSURE_SCHEMA: _mutation_closure_schema(),
        SQLSERVER_LIFECYCLE_POLICY_SCHEMA: _lifecycle_policy_schema(),
        READ_DEPENDENCY_PROOF_SCHEMA: _read_dependency_schema(),
    }


def _model_definition_proof_schema() -> dict[str, Any]:
    digest_fields = (
        "manifest_sha256",
        "raw_code_sha256",
        "compiled_sql_sha256",
        "macro_closure_sha256",
        "toolchain_sha256",
        "resolved_relation_dependency_digest",
        "resolved_module_dependency_digest",
        "catalog_observation_digest",
        "parser_runtime_policy_digest",
        "target_independence_policy_digest",
        "model_definition_proof_sha256",
    )
    properties = {field: DIGEST_SCHEMA for field in digest_fields}
    properties.update(
        {
            "model_unique_id": TEXT_SCHEMA,
            "schema": schema_discriminator(MODEL_DEFINITION_PROOF_SCHEMA),
            "status": CLOSURE_STATUS_SCHEMA,
        }
    )
    return closed_schema(MODEL_DEFINITION_PROOF_SCHEMA, properties, sorted(properties))


def _mutation_closure_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "indirect_selection": {"const": "empty"},
        "mutation_closure_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(MUTATION_CLOSURE_SCHEMA),
        "selected_mutating_node_ids": string_set_schema(),
        "selected_node_ids": string_set_schema(),
        "selected_read_only_node_ids": string_set_schema(allow_empty=True),
        "selectors": {
            **string_set_schema(),
            "items": {"minLength": 1, "not": {"pattern": "\\+"}, "type": "string"},
        },
        "status": CLOSURE_STATUS_SCHEMA,
        "unclassified_mutating_node_ids": string_set_schema(allow_empty=True),
    }
    return closed_schema(
        MUTATION_CLOSURE_SCHEMA,
        properties,
        sorted(properties),
        comment="Selected nodes equal the disjoint mutating/read-only closure; PROVEN has no unclassified mutation.",
    )


def _lifecycle_policy_schema() -> dict[str, Any]:
    digest_fields = (
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
        "sqlserver_lifecycle_policy_sha256",
    )
    properties: dict[str, object] = {field: DIGEST_SCHEMA for field in digest_fields}
    properties.update(
        {
            "compatibility_level": POSITIVE_INTEGER_SCHEMA,
            "contract_enforced": {"const": True},
            "dbt_core_version": {"const": DBT_CORE_VERSION},
            "dbt_sqlserver_version": {"const": DBT_SQLSERVER_VERSION},
            "existing_table_required": {"const": True},
            "full_refresh_allowed": {"const": False},
            "hooks_allowed": {"const": False},
            "incremental_strategy": {"const": "dpone_scope_merge"},
            "materialization": {"const": "incremental"},
            "odbc_driver": TEXT_SCHEMA,
            "on_schema_change": {"const": "fail"},
            "pyodbc_version": TEXT_SCHEMA,
            "python_version": TEXT_SCHEMA,
            "schema": schema_discriminator(SQLSERVER_LIFECYCLE_POLICY_SCHEMA),
            "schema_mutation_allowed": {"const": False},
            "sqlserver_version": TEXT_SCHEMA,
        }
    )
    return closed_schema(SQLSERVER_LIFECYCLE_POLICY_SCHEMA, properties, sorted(properties))


def _read_dependency_schema() -> dict[str, Any]:
    edge = {
        "additionalProperties": False,
        "properties": {
            "dependency_kind": {"enum": [item.value for item in ReadDependencyKind], "type": "string"},
            "from_object_id": POSITIVE_INTEGER_SCHEMA,
            "to_object_id": POSITIVE_INTEGER_SCHEMA,
        },
        "required": ["dependency_kind", "from_object_id", "to_object_id"],
        "type": "object",
    }
    properties: dict[str, object] = {
        "base_relation_object_ids": {
            "items": POSITIVE_INTEGER_SCHEMA,
            "type": "array",
            "uniqueItems": True,
        },
        "catalog_snapshot_sha256": DIGEST_SCHEMA,
        "compiled_sql_sha256": DIGEST_SCHEMA,
        "database_name": TEXT_SCHEMA,
        "dependency_edges": {"items": edge, "type": "array", "uniqueItems": True},
        "max_definition_bytes": POSITIVE_INTEGER_SCHEMA,
        "max_depth": POSITIVE_INTEGER_SCHEMA,
        "max_edges": POSITIVE_INTEGER_SCHEMA,
        "max_nodes": POSITIVE_INTEGER_SCHEMA,
        "model_unique_id": TEXT_SCHEMA,
        "normalized_definitions_sha256": DIGEST_SCHEMA,
        "policy_sha256": DIGEST_SCHEMA,
        "read_dependency_proof_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(READ_DEPENDENCY_PROOF_SCHEMA),
        "status": CLOSURE_STATUS_SCHEMA,
    }
    return closed_schema(READ_DEPENDENCY_PROOF_SCHEMA, properties, sorted(properties))


__all__ = ["proof_contract_schemas"]
