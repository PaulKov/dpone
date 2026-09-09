from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    documented_contract,
    integer_schema,
    issue_ref,
    object_schema,
    string_schema,
)


def core_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        affected_contract(),
        affected_workloads_contract(),
        plan_contract(),
        verify_contract(),
        bundle_contract(),
        attestation_contract(),
        workloads_contract(),
        error_contract(),
    )


def affected_contract() -> GitOpsSchemaContract:
    return contract(
        name="affected",
        kind="gitops.affected",
        title="dpone GitOps affected contract",
        required=("kind", "changed_files", "impacted_manifests"),
        properties={
            "kind": const_schema("gitops.affected"),
            "changed_files": array_schema(string_schema()),
            "impacted_manifests": array_schema(
                object_schema(
                    required=("manifest", "workload_root", "reasons", "suggested_commands"),
                    properties={
                        "manifest": string_schema(),
                        "workload_root": string_schema(),
                        "reasons": array_schema(object_schema()),
                        "suggested_commands": array_schema(string_schema()),
                        "emitted_plan": string_schema(),
                    },
                )
            ),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def affected_workloads_contract() -> GitOpsSchemaContract:
    return contract(
        name="affected-workloads",
        kind="gitops.affected_workloads",
        title="dpone GitOps affected workloads contract",
        required=("kind", "schema_version", "producer", "changed_files", "affected_workloads"),
        properties={
            "kind": const_schema("gitops.affected_workloads"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "changed_files": array_schema(string_schema()),
            "affected_workloads": array_schema(
                object_schema(
                    required=("workload_id", "manifest", "reasons"),
                    properties={
                        "workload_id": string_schema(),
                        "manifest": string_schema(),
                        "reasons": array_schema(object_schema()),
                    },
                )
            ),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def plan_contract() -> GitOpsSchemaContract:
    return contract(
        name="plan",
        kind="gitops.plan",
        title="dpone GitOps plan contract",
        required=("kind", "manifest", "workload_root", "runner", "command", "sparse_paths"),
        properties={
            "kind": const_schema("gitops.plan"),
            "manifest": string_schema(),
            "workload_root": string_schema(),
            "runner": object_schema(
                required=("kind",),
                properties={"kind": string_schema(), "requires_sparse_checkout": boolean_schema()},
            ),
            "command": object_schema(required=("text",), properties={"text": string_schema()}),
            "sparse_paths": array_schema(sparse_path_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
            "provenance": object_schema(),
            "lock": object_schema(),
        },
    )


def verify_contract() -> GitOpsSchemaContract:
    return contract(
        name="verify",
        kind="gitops.verify",
        title="dpone GitOps verify contract",
        required=("kind", "plan", "worktree", "manifest", "checked_paths"),
        properties={
            "kind": const_schema("gitops.verify"),
            "plan": string_schema(),
            "worktree": string_schema(),
            "manifest": string_schema(),
            "checked_paths": array_schema(
                object_schema(
                    required=("path", "required", "exists", "is_dir"),
                    properties={
                        "path": string_schema(),
                        "required": boolean_schema(),
                        "exists": boolean_schema(),
                        "is_dir": boolean_schema(),
                    },
                )
            ),
            "lock_checks": array_schema(object_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def bundle_contract() -> GitOpsSchemaContract:
    return contract(
        name="bundle",
        kind="gitops.bundle",
        title="dpone GitOps bundle contract",
        required=("kind", "output_dir", "affected_path", "summary_path", "entries", "policy"),
        properties={
            "kind": const_schema("gitops.bundle"),
            "output_dir": string_schema(),
            "affected_path": string_schema(),
            "summary_path": string_schema(),
            "entries": array_schema(bundle_entry_schema()),
            "policy": object_schema(required=("profile",), properties={"profile": string_schema()}),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
            "attestation": attestation_schema(),
        },
    )


def attestation_contract() -> GitOpsSchemaContract:
    return contract(
        name="attestation",
        kind="gitops.bundle.attestation",
        title="dpone GitOps bundle attestation contract",
        required=("schema_version", "producer", "hash_algorithm", "bundle_digest", "provenance", "artifacts"),
        properties=attestation_schema()["properties"],
    )


def workloads_contract() -> GitOpsSchemaContract:
    return contract(
        name="workloads",
        kind="gitops.workloads",
        title="dpone GitOps workloads contract",
        required=("kind", "schema_version", "producer", "workload_set", "env", "workloads"),
        properties={
            "kind": const_schema("gitops.workloads"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "workload_set": string_schema(),
            "env": string_schema(),
            "workloads": array_schema(
                object_schema(
                    required=("workload_id", "manifest", "catalog_path", "effective_config", "provenance"),
                    properties={
                        "workload_id": string_schema(),
                        "manifest": string_schema(),
                        "domain": string_schema(),
                        "catalog_path": string_schema(),
                        "effective_config": object_schema(),
                        "provenance": object_schema(),
                    },
                )
            ),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def error_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="error",
        kind="dpone.error.v1",
        title="dpone GitOps structured error",
        required=("schema", "code", "stage", "severity", "message"),
        properties=error_properties(),
    )


def error_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.error.v1"},
        "code": {
            "type": "string",
            "pattern": "^DPONE_[A-Z0-9_]+$",
        },
        "stage": {"type": "string", "minLength": 1},
        "severity": {"enum": ["info", "warning", "error"]},
        "entity": error_entity_schema(),
        "path": {"type": "string"},
        "message": {"type": "string", "minLength": 1},
        "fixes": {"type": "array", "items": error_fix_schema()},
        "docs_url": {"type": "string"},
        "trace_id": {"type": "string"},
    }


def error_entity_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "kind": {"type": "string"},
            "id": {"type": "string"},
        },
    }


def error_fix_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "safety"],
        "additionalProperties": True,
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "safety": {"enum": ["safe", "manual", "destructive"]},
            "command": {"type": "string"},
        },
    }


def sparse_path_schema() -> dict[str, Any]:
    return object_schema(
        required=("path", "kind", "source", "required", "exists", "is_dir", "reason"),
        properties={
            "path": string_schema(),
            "kind": string_schema(),
            "source": string_schema(),
            "required": boolean_schema(),
            "exists": boolean_schema(),
            "is_dir": boolean_schema(),
            "reason": string_schema(),
        },
    )


def bundle_entry_schema() -> dict[str, Any]:
    return object_schema(
        required=("manifest", "plan_path", "verify_path", "passed"),
        properties={
            "manifest": string_schema(),
            "plan_path": string_schema(),
            "verify_path": string_schema(),
            "passed": boolean_schema(),
        },
    )


def attestation_schema() -> dict[str, Any]:
    return object_schema(
        required=("schema_version", "producer", "hash_algorithm", "bundle_digest", "provenance", "artifacts"),
        properties={
            "schema_version": string_schema(),
            "producer": string_schema(),
            "hash_algorithm": const_schema("sha256"),
            "bundle_digest": string_schema(),
            "provenance": object_schema(),
            "artifacts": array_schema(
                object_schema(
                    required=("path", "sha256", "bytes"),
                    properties={
                        "path": string_schema(),
                        "sha256": string_schema(),
                        "bytes": integer_schema(),
                    },
                )
            ),
        },
    )


__all__ = ["core_schema_contracts"]
