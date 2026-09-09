"""Finalization and verification report schemas for dbt dev evidence."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

_SAFE_TEXT = {
    "type": "string",
    "minLength": 1,
    "maxLength": 2048,
    "pattern": "^[^\\u0000-\\u001f\\u007f]+$",
}
_LEGACY_OUTPUT_ROOT = {
    "type": "string",
    "minLength": 1,
    "maxLength": 4096,
}
_SUBJECT_PATH = {
    "type": "string",
    "minLength": 1,
    "maxLength": 1024,
    "pattern": r"^(?!/)(?!.*(?:^|/)\.\.(?:/|$))(?!.*\\).+$",
}


@dataclass(frozen=True, slots=True)
class _SchemaPrimitives:
    digest: dict[str, Any]
    token: dict[str, Any]
    nonempty_tokens: Callable[[], dict[str, Any]]
    object_schema: Callable[
        [tuple[str, ...], dict[str, Any]],
        dict[str, Any],
    ]


def evidence_report_schema_contracts(
    *,
    digest: dict[str, Any],
    token: dict[str, Any],
    nonempty_tokens: Callable[[], dict[str, Any]],
    object_schema: Callable[
        [tuple[str, ...], dict[str, Any]],
        dict[str, Any],
    ],
    max_bundle_files: int,
) -> dict[str, dict[str, Any]]:
    primitives = _SchemaPrimitives(
        digest=digest,
        token=token,
        nonempty_tokens=nonempty_tokens,
        object_schema=object_schema,
    )
    return {
        "dpone.dbt-dev-evidence-bundle.v1": _bundle(
            False,
            primitives,
            max_bundle_files=max_bundle_files,
        ),
        "dpone.dbt-dev-evidence-bundle.v2": _bundle(
            True,
            primitives,
            max_bundle_files=max_bundle_files,
        ),
        "dpone.dbt-dev-evidence-provenance.v1": _provenance(
            False,
            primitives,
        ),
        "dpone.dbt-dev-evidence-provenance.v2": _provenance(
            True,
            primitives,
        ),
        "dpone.dbt-dev-evidence-verification.v1": _verification(
            False,
            primitives,
        ),
        "dpone.dbt-dev-evidence-verification.v2": _verification(
            True,
            primitives,
        ),
    }


def _bundle(
    with_set: bool,
    primitives: _SchemaPrimitives,
    *,
    max_bundle_files: int,
) -> dict[str, Any]:
    required = [
        "schema",
        "passed",
        "release_id",
        "deployment_id",
        "output_root",
        "subject_path",
        "subject_sha256",
        "file_count",
        "total_bytes",
        "verified_workloads",
        "no_op",
    ]
    properties: dict[str, Any] = {
        "schema": {"const": ("dpone.dbt-dev-evidence-bundle.v2" if with_set else "dpone.dbt-dev-evidence-bundle.v1")},
        "passed": {"const": True},
        "release_id": primitives.digest,
        "deployment_id": primitives.digest,
        "output_root": deepcopy(_SAFE_TEXT if with_set else _LEGACY_OUTPUT_ROOT),
        "subject_path": deepcopy(_SUBJECT_PATH),
        "subject_sha256": primitives.digest,
        "file_count": {
            "type": "integer",
            "minimum": 4,
            "maximum": max_bundle_files,
        },
        "total_bytes": {
            "type": "integer",
            "minimum": 1,
            "maximum": 268435456,
        },
        "verified_workloads": primitives.nonempty_tokens(),
        "no_op": {"type": "boolean"},
    }
    if with_set:
        required.extend(("evidence_set_id", "campaign_request_sha256"))
        properties["evidence_set_id"] = primitives.digest
        properties["campaign_request_sha256"] = primitives.digest
    return primitives.object_schema(tuple(required), properties)


def _provenance(
    with_set: bool,
    primitives: _SchemaPrimitives,
) -> dict[str, Any]:
    if not with_set:
        return primitives.object_schema(
            (
                "schema",
                "release_id",
                "deployment_id",
                "producer_repository",
                "producer_workflow",
                "source_commit",
            ),
            {
                "schema": {"const": "dpone.dbt-dev-evidence-provenance.v1"},
                "release_id": primitives.digest,
                "deployment_id": primitives.digest,
                "producer_repository": deepcopy(_SAFE_TEXT),
                "producer_workflow": deepcopy(_SAFE_TEXT),
                "source_commit": {
                    "type": "string",
                    "pattern": "^[0-9a-f]{40}$",
                },
            },
        )
    properties = {
        "schema": {"const": "dpone.dbt-dev-evidence-provenance.v2"},
        "release_id": primitives.digest,
        "deployment_id": primitives.digest,
        "evidence_set_id": primitives.digest,
        "campaign_request_sha256": primitives.digest,
        "campaign_controller_repository": deepcopy(_SAFE_TEXT),
        "campaign_controller_workflow": deepcopy(_SAFE_TEXT),
        "campaign_source_commit": {
            "type": "string",
            "pattern": "^[0-9a-f]{40}$",
        },
        "orchestration_run_id": deepcopy(_SAFE_TEXT),
        "orchestration_run_attempt": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
        },
        "finalizer_repository": deepcopy(_SAFE_TEXT),
        "finalizer_workflow": deepcopy(_SAFE_TEXT),
        "finalizer_source_commit": {
            "type": "string",
            "pattern": "^[0-9a-f]{40}$",
        },
    }
    return primitives.object_schema(tuple(properties), properties)


def _verification(
    with_set: bool,
    primitives: _SchemaPrimitives,
) -> dict[str, Any]:
    required = [
        "schema",
        "status",
        "passed",
        "code",
        "release_id",
        "deployment_id",
        "required_workloads",
        "verified_workloads",
        "verified_dbt_workflows",
        "reason_codes",
    ]
    properties: dict[str, Any] = {
        "schema": {
            "const": (
                "dpone.dbt-dev-evidence-verification.v2" if with_set else "dpone.dbt-dev-evidence-verification.v1"
            )
        },
        "status": {"enum": ["passed", "unverified"]},
        "passed": {"type": "boolean"},
        "code": primitives.token,
        "release_id": primitives.digest,
        "deployment_id": primitives.digest,
        "required_workloads": _unique_tokens(primitives),
        "verified_workloads": _unique_tokens(primitives),
        "verified_dbt_workflows": _unique_tokens(primitives),
        "reason_codes": _unique_tokens(primitives),
    }
    if with_set:
        required.append("evidence_set_id")
        properties["evidence_set_id"] = primitives.digest
    schema = primitives.object_schema(tuple(required), properties)
    schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "passed"},
                "passed": {"const": True},
                "code": {"const": "DPONE_DBT_DEV_EVIDENCE_VERIFIED"},
                "reason_codes": {"maxItems": 0},
            },
            "required": [
                "status",
                "passed",
                "code",
                "reason_codes",
            ],
        },
        {
            "properties": {
                "status": {"const": "unverified"},
                "passed": {"const": False},
                "code": {"const": "DPONE_DBT_DEV_EVIDENCE_UNVERIFIED"},
                "reason_codes": {"minItems": 1},
            },
            "required": [
                "status",
                "passed",
                "code",
                "reason_codes",
            ],
        },
    ]
    return schema


def _unique_tokens(
    primitives: _SchemaPrimitives,
) -> dict[str, Any]:
    return {
        "type": "array",
        "uniqueItems": True,
        "items": primitives.token,
    }


__all__ = ["evidence_report_schema_contracts"]
