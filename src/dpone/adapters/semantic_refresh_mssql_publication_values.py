"""Closed scalar and authority-projection validation for MSSQL publication."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def require_identifier(value: str, field_name: str) -> str:
    """Return one validated simple SQL identifier."""

    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a simple SQL identifier")
    return value


def validate_authority_projection(request: Mapping[str, object]) -> None:
    """Validate protected predecessor and target coordinates."""

    require_digest(request["publication_authority_sha256"], "publication_authority_sha256")
    for field_name in (
        "target_resource_id",
        "target_authority_id",
        "clickhouse_cluster_authority_id",
        "database",
        "target_table",
        "scope_id",
    ):
        require_text(request[field_name], field_name)
    if request["target_authority_id"] != (
        f"clickhouse://{request['clickhouse_cluster_authority_id']}/{request['database']}/{request['target_table']}"
    ):
        raise ValueError("target_authority_id differs from protected publication target")
    require_positive_int(request["scope_revision"], "scope_revision")
    require_text(request["workflow_execution_id"], "workflow_execution_id")
    require_digest(request["target_predecessor_generation_id"], "target_predecessor_generation_id")
    require_non_negative_int(request["predecessor_target_generation"], "predecessor_target_generation")
    require_uuid(request["predecessor_target_uuid"], "predecessor_target_uuid")
    require_digest(request["predecessor_target_operation_id"], "predecessor_target_operation_id")
    scope_predecessor = request["scope_predecessor_operation_id"]
    predecessor_scope_revision = request["predecessor_scope_revision"]
    checkpoint = (
        request["predecessor_checkpoint_sha256"],
        request["predecessor_checkpoint_operation_id"],
        request["predecessor_checkpoint_version"],
    )
    if scope_predecessor is None:
        if predecessor_scope_revision is not None or any(value is not None for value in checkpoint):
            raise ValueError("first-scope protected predecessors must be absent")
    else:
        require_digest(scope_predecessor, "scope_predecessor_operation_id")
        require_positive_int(predecessor_scope_revision, "predecessor_scope_revision")
        require_digest(checkpoint[0], "predecessor_checkpoint_sha256")
        require_digest(checkpoint[1], "predecessor_checkpoint_operation_id")
        require_positive_int(checkpoint[2], "predecessor_checkpoint_version")
    if "expected_target_generation" in request:
        expected = (
            request["predecessor_target_generation"],
            predecessor_scope_revision or 0,
            request["predecessor_checkpoint_sha256"],
        )
        observed = (
            request["expected_target_generation"],
            request["expected_scope_revision"],
            request["expected_checkpoint_sha256"],
        )
        if observed != expected:
            raise ValueError("publication expected heads differ from protected predecessors")


def require_closed(
    request: Mapping[str, object],
    *,
    legacy: set[str],
    protected: set[str] | frozenset[str],
    required: bool,
) -> None:
    """Require one exact legacy or protected request field closure."""

    if not isinstance(request, Mapping):
        raise ValueError("semantic refresh state request fields are not closed")
    accepted = {frozenset(protected)} if required else {frozenset(legacy), frozenset(protected)}
    if frozenset(request) not in accepted:
        raise ValueError("semantic refresh state request fields are not closed")


def require_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def require_digest(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


def require_uuid(value: object, field_name: str) -> None:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        parsed = None
    if parsed is None or str(parsed) != value:
        raise ValueError(f"{field_name} must be a canonical UUID")


def require_positive_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")


def require_non_negative_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
