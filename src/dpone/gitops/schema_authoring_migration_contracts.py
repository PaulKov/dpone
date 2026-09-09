"""Public schema for explicit authoring-mode migration plans and receipts."""

from __future__ import annotations

from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    documented_contract,
    integer_schema,
    object_schema,
    string_schema,
)

_SHA256 = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
_MODE = {"enum": ["classic", "flow", "folder"]}


def authoring_migration_schema_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="authoring-migration",
        kind="dpone.authoring-migration.v1",
        title="dpone GitOps authoring-mode migration plan and receipt",
        required=(
            "kind",
            "schema",
            "passed",
            "mode",
            "status",
            "plan_id",
            "source",
            "target",
            "changes",
            "retained_files",
            "warnings",
            "errors",
        ),
        properties={
            "kind": {"const": "dpone.authoring-migration.v1"},
            "schema": {"const": "dpone.authoring-migration.v1"},
            "passed": boolean_schema(),
            "mode": {"enum": ["plan", "apply"]},
            "status": {"enum": ["ready", "no_op", "blocked", "applied"]},
            "plan_id": _SHA256,
            "source": _identity_schema(),
            "target": _identity_schema(),
            "changes": array_schema(_change_schema()),
            "retained_files": array_schema(string_schema()),
            "warnings": array_schema(string_schema()),
            "errors": array_schema(_error_schema()),
        },
        additional_properties=False,
    )


def _identity_schema() -> dict[str, object]:
    return object_schema(
        required=("mode", "semantic_fingerprint"),
        properties={
            "mode": _MODE,
            "path": string_schema(),
            "sha256": _SHA256,
            "semantic_fingerprint": _SHA256,
        },
    )


def _change_schema() -> dict[str, object]:
    return object_schema(
        required=("action", "path", "before_sha256", "after_sha256", "unified_diff"),
        properties={
            "action": {"enum": ["create", "modify", "no_op"]},
            "path": string_schema(),
            "before_sha256": {"oneOf": [_SHA256, {"type": "null"}]},
            "after_sha256": _SHA256,
            "unified_diff": string_schema(),
        },
    )


def _error_schema() -> dict[str, object]:
    return object_schema(
        required=("schema", "code", "stage", "severity", "message", "fixes"),
        properties={
            "schema": {"const": "dpone.error.v1"},
            "code": string_schema(),
            "stage": {"const": "authoring_migration"},
            "severity": string_schema(),
            "message": string_schema(),
            "path": string_schema(),
            "entity": object_schema(),
            "fixes": array_schema(object_schema()),
            "exit_code": integer_schema(),
        },
    )


__all__ = ["authoring_migration_schema_contract"]
