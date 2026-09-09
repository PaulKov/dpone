"""Dependency-free closed contract for merge-closure receipt payloads."""

from __future__ import annotations

import re
from typing import Any

RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "binding_id",
        "repository",
        "protected_base_ref",
        "pr_number",
        "merged_at",
        "integration_method",
        "reviewed_head_sha",
        "reviewed_head_tree",
        "base_parent_sha",
        "integration_commit_sha",
        "integration_tree",
        "changed_paths",
        "pr_body_sha256",
        "source_receipt",
        "producer",
        "errors",
        "warnings",
    }
)
SOURCE_RECEIPT_FIELDS = frozenset(
    {
        "check_run_id",
        "workflow_run_id",
        "workflow_run_attempt",
        "artifact_id",
        "artifact_digest",
        "artifact_size_bytes",
        "archive_sha256",
        "archive_size_bytes",
        "created_at",
        "completed_at",
        "receipt_sha256",
        "audit_manifest_sha256",
        "body_sha256",
        "status",
    }
)
PRODUCER_FIELDS = frozenset({"workflow", "run_id", "run_attempt"})

_SHA = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_PASS_REQUIRED_STRINGS = (
    "protected_base_ref",
    "merged_at",
)
_PASS_REQUIRED_SHAS = (
    "reviewed_head_sha",
    "reviewed_head_tree",
    "base_parent_sha",
    "integration_commit_sha",
    "integration_tree",
)


def validate_receipt_payload(payload: dict[str, Any]) -> None:
    """Validate the exact v1 schema without repository files or third parties."""

    _require_exact_mapping(payload, RECEIPT_FIELDS, field="receipt")
    if isinstance(payload.get("schema_version"), bool) or payload.get("schema_version") != 1:
        _invalid("receipt.schema_version must equal 1")
    status = payload.get("status")
    if status not in ("PASS", "FAIL"):
        _invalid("receipt.status must be PASS or FAIL")

    _nullable_digest(payload.get("binding_id"), field="receipt.binding_id")
    _non_empty_string(payload.get("repository"), field="receipt.repository")
    _nullable_string(payload.get("protected_base_ref"), field="receipt.protected_base_ref")
    _nullable_positive_int(payload.get("pr_number"), field="receipt.pr_number")
    _nullable_string(payload.get("merged_at"), field="receipt.merged_at")
    if payload.get("integration_method") not in ("merge", "squash", None):
        _invalid("receipt.integration_method is invalid")
    for field in _PASS_REQUIRED_SHAS:
        _nullable_sha(payload.get(field), field=f"receipt.{field}")
    _string_list(payload.get("changed_paths"), field="receipt.changed_paths", unique=True)
    _nullable_digest(payload.get("pr_body_sha256"), field="receipt.pr_body_sha256")

    source = payload.get("source_receipt")
    if source is not None:
        _validate_source_receipt(source)
    _validate_producer(payload.get("producer"))
    errors = _string_list(payload.get("errors"), field="receipt.errors")
    _string_list(payload.get("warnings"), field="receipt.warnings")

    if status == "PASS":
        _digest(payload.get("binding_id"), field="receipt.binding_id")
        for field in _PASS_REQUIRED_STRINGS:
            _non_empty_string(payload.get(field), field=f"receipt.{field}")
        _positive_int(payload.get("pr_number"), field="receipt.pr_number")
        if payload.get("integration_method") not in ("merge", "squash"):
            _invalid("PASS receipt.integration_method is required")
        for field in _PASS_REQUIRED_SHAS:
            _sha(payload.get(field), field=f"receipt.{field}")
        _digest(payload.get("pr_body_sha256"), field="receipt.pr_body_sha256")
        if source is None:
            _invalid("PASS receipt.source_receipt is required")
        if errors:
            _invalid("PASS receipt.errors must be empty")
    elif not errors:
        _invalid("FAIL receipt.errors must not be empty")


def _validate_source_receipt(value: Any) -> None:
    source = _require_exact_mapping(value, SOURCE_RECEIPT_FIELDS, field="receipt.source_receipt")
    for field in (
        "check_run_id",
        "workflow_run_id",
        "workflow_run_attempt",
        "artifact_id",
        "artifact_size_bytes",
        "archive_size_bytes",
    ):
        _positive_int(source.get(field), field=f"receipt.source_receipt.{field}")
    for field in (
        "artifact_digest",
        "archive_sha256",
        "receipt_sha256",
        "audit_manifest_sha256",
        "body_sha256",
    ):
        _digest(source.get(field), field=f"receipt.source_receipt.{field}")
    for field in ("created_at", "completed_at"):
        _non_empty_string(source.get(field), field=f"receipt.source_receipt.{field}")
    if source.get("status") not in ("PASS", "N/A"):
        _invalid("receipt.source_receipt.status must be PASS or N/A")


def _validate_producer(value: Any) -> None:
    producer = _require_exact_mapping(value, PRODUCER_FIELDS, field="receipt.producer")
    for field in PRODUCER_FIELDS:
        _non_empty_string(producer.get(field), field=f"receipt.producer.{field}")


def _require_exact_mapping(value: Any, expected: frozenset[str], *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != expected:
        _invalid(f"{field} fields must match the closed contract")
    return value


def _string_list(value: Any, *, field: str, unique: bool = False) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        _invalid(f"{field} must be an array of non-empty strings")
    if unique and len(value) != len(set(value)):
        _invalid(f"{field} values must be unique")
    return value


def _non_empty_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        _invalid(f"{field} must be a non-empty string")
    return value


def _nullable_string(value: Any, *, field: str) -> None:
    if value is not None:
        _non_empty_string(value, field=field)


def _positive_int(value: Any, *, field: str) -> int:
    is_integer = isinstance(value, int) or (isinstance(value, float) and value.is_integer())
    if isinstance(value, bool) or not is_integer or value < 1:
        _invalid(f"{field} must be a positive integer")
    return int(value)


def _nullable_positive_int(value: Any, *, field: str) -> None:
    if value is not None:
        _positive_int(value, field=field)


def _sha(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        _invalid(f"{field} must be a lowercase full commit SHA")
    return value


def _nullable_sha(value: Any, *, field: str) -> None:
    if value is not None:
        _sha(value, field=field)


def _digest(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        _invalid(f"{field} must be a lowercase SHA-256 digest")
    return value


def _nullable_digest(value: Any, *, field: str) -> None:
    if value is not None:
        _digest(value, field=field)


def _invalid(message: str) -> None:
    raise ValueError(f"merge receipt does not satisfy its closed schema: {message}")


__all__ = [
    "PRODUCER_FIELDS",
    "RECEIPT_FIELDS",
    "SOURCE_RECEIPT_FIELDS",
    "validate_receipt_payload",
]
