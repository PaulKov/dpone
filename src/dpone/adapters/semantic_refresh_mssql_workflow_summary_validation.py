"""Validation primitives for durable MSSQL workflow summaries."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_publication_summary_authority import (
    SemanticRefreshWorkflowSummaryError,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_workflow_summary import SemanticRefreshDurableWorkflowSummary


class OutputCursor(Protocol):
    """Minimal DML OUTPUT surface needed by summary compare-and-set checks."""

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


def validate_workflow_summary_payload(summary: Mapping[str, object]) -> dict[str, object]:
    """Parse the sole canonical complete workflow-summary authority."""

    try:
        return SemanticRefreshDurableWorkflowSummary.from_mapping(summary).to_dict()
    except (SemanticRefreshContractError, TypeError, ValueError) as exc:
        raise SemanticRefreshWorkflowSummaryError("durable workflow summary is invalid") from exc


def require_rows(
    cursor: OutputCursor,
    expected: Sequence[tuple[object, ...]],
    label: str,
) -> None:
    """Require the exact compare-and-set OUTPUT identity closure."""

    observed = tuple(sorted((tuple(row) for row in cursor.fetchall()), key=_row_sort_key))
    wanted = tuple(sorted(expected, key=_row_sort_key))
    if observed != wanted:
        raise SemanticRefreshWorkflowSummaryError(f"{label} compare-and-set failed")


def _row_sort_key(row: tuple[object, ...]) -> tuple[str, ...]:
    return tuple(str(value) for value in row)


def uuid_text(value: object) -> str | None:
    """Normalize a nullable UUID returned by the DB-API driver."""

    return None if value is None else str(uuid.UUID(str(value)))


def canonical_json(value: Mapping[str, object]) -> str:
    """Encode canonical durable summary JSON."""

    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def expected_journals(publications: list[object]) -> tuple[tuple[object, ...], ...]:
    """Project exact terminal journal coordinates from the summary."""

    return tuple(
        (
            item["operation_id"],
            item["operation_plan_sha256"],
            item["attempt_binding_sha256"],
            "COMPLETE",
            item["artifact_manifest_sha256"],
            item["clickhouse_terminal_receipt_sha256"],
            item["terminal_receipt_sha256"],
            item["target_generation"],
            item["scope_revision"],
        )
        for item in publications
        if isinstance(item, Mapping)
    )


def journal_resources(durable: tuple[tuple[Any, ...], ...]) -> tuple[str, ...]:
    """Validate and return the exact journal guard resource closure."""

    resources = tuple(str(row[9]) for row in durable)
    if any(not resource.strip() for resource in resources) or len(set(resources)) != len(resources):
        raise SemanticRefreshWorkflowSummaryError("workflow journal guard closure differs")
    return resources


def is_digest(value: object) -> bool:
    """Return whether a value is a canonical lowercase SHA-256 digest."""

    return (
        isinstance(value, str)
        and len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def is_uuid(value: object) -> bool:
    """Return whether a value is canonical UUID text."""

    try:
        text = str(value)
        return str(uuid.UUID(text)) == text.lower()
    except (AttributeError, TypeError, ValueError):
        return False
