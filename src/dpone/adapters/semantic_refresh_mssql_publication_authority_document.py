"""Canonical JSON integrity proof for locked MSSQL publication authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

_AUTHORITY_FIELDS = {
    "attempt_bindings",
    "authority_sha256",
    "controller_id",
    "execution_binding",
    "model_resources",
    "operation_plans",
    "owner_id",
    "replacement_plan",
    "reservation_id",
    "resource_budget",
    "resource_guards",
    "schema",
    "workflow_execution_id",
    "workflow_guard",
    "workflow_plan",
}


def authenticate_canonical_authority_document(
    *,
    workflow_execution_id: object,
    workflow_execution_binding_sha256: object,
    authority_sha256: object,
    authority_json: object,
) -> None:
    """Recompute the canonical document digest while its SQL row is locked."""

    if not isinstance(workflow_execution_id, str) or not workflow_execution_id:
        raise ValueError("canonical publication workflow execution identity is invalid")
    if not isinstance(authority_sha256, str) or not authority_sha256.startswith("sha256:"):
        raise ValueError("canonical publication authority digest is invalid")
    if not isinstance(authority_json, str) or not authority_json:
        raise ValueError("canonical publication authority document is absent")
    try:
        raw = json.loads(authority_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical publication authority JSON is invalid") from exc
    if not isinstance(raw, Mapping) or set(raw) != _AUTHORITY_FIELDS:
        raise ValueError("canonical publication authority fields are not closed")
    if raw["authority_sha256"] != authority_sha256:
        raise ValueError("canonical publication authority record digest differs")
    if raw["workflow_execution_id"] != workflow_execution_id:
        raise ValueError("canonical publication workflow execution identity differs")
    execution_binding = raw["execution_binding"]
    if (
        not isinstance(execution_binding, Mapping)
        or execution_binding.get("workflow_execution_binding_sha256") != workflow_execution_binding_sha256
    ):
        raise ValueError("canonical publication execution binding differs")
    unsigned = {key: value for key, value in raw.items() if key != "authority_sha256"}
    canonical_unsigned = json.dumps(
        unsigned,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    actual_sha256 = "sha256:" + hashlib.sha256(canonical_unsigned).hexdigest()
    if actual_sha256 != authority_sha256:
        raise ValueError("canonical publication authority document digest differs")
    canonical_document = json.dumps(
        raw,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    if canonical_document != authority_json:
        raise ValueError("canonical publication authority document bytes are not canonical")


__all__ = ["authenticate_canonical_authority_document"]
