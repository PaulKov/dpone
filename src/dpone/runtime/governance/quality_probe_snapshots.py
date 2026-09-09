"""Canonical quality probe snapshots derived from extract/load artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.governance.quality import QualityProbeSnapshot
from dpone.runtime.native_transfer_row_authority import sum_slice_export_rows

_MISSING = object()


def source_snapshot(extract_result: Any) -> QualityProbeSnapshot:
    """Build the source probe from completed export authority only.

    Planned ``estimated_rows`` never certify row-count gates. Prefer explicit
    ``rows_exported`` / ``row_count``, then slice evidence or materialised
    ``_rows`` length. Known contract wrappers expose only their inner completed
    authority; their estimates and row values never certify quality gates.
    Malformed counts become unavailable so row gates fail closed.
    """

    artifact = getattr(extract_result, "artifact", None)
    row_count = _completed_source_row_count(artifact)
    return QualityProbeSnapshot(
        row_count=canonical_row_count(row_count),
        typed_hash=canonical_typed_hash(getattr(extract_result, "typed_hash", None)),
    )


def target_snapshot(load_result: Any) -> QualityProbeSnapshot:
    row_count = getattr(load_result, "staging_rows", None)
    if row_count is None:
        row_count = getattr(load_result, "total_rows", None)
    return QualityProbeSnapshot(
        row_count=canonical_row_count(row_count),
        typed_hash=canonical_typed_hash(getattr(load_result, "typed_hash", None)),
    )


def canonical_row_count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def canonical_typed_hash(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value


def _completed_source_row_count(
    artifact: object | None,
    *,
    allow_materialized_rows: bool = True,
    visited: frozenset[int] = frozenset(),
) -> object | None:
    if artifact is None or id(artifact) in visited:
        return None
    observed = visited | {id(artifact)}
    for attribute in ("rows_exported", "row_count"):
        value = _safe_attribute(artifact, attribute)
        if value is _MISSING:
            continue
        if value is not None:
            return value
    slice_evidence = _safe_attribute(artifact, "slice_evidence")
    if (
        slice_evidence is not _MISSING
        and isinstance(slice_evidence, Sequence)
        and not isinstance(slice_evidence, (str, bytes, bytearray))
    ):
        value = sum_slice_export_rows(slice_evidence)
        if value is not None:
            return value
    inner_authority = _safe_attribute(artifact, "completed_source_authority_artifact")
    if inner_authority is not _MISSING and inner_authority is not None:
        return _completed_source_row_count(
            inner_authority,
            allow_materialized_rows=False,
            visited=observed,
        )
    rows = _safe_attribute(artifact, "_rows") if allow_materialized_rows else _MISSING
    if rows is not _MISSING:
        try:
            return len(rows)
        except (TypeError, ValueError, OverflowError):
            return None
    return None


def _safe_attribute(artifact: object, name: str) -> object:
    try:
        return getattr(artifact, name)
    except (AttributeError, RuntimeError, TypeError, ValueError, OverflowError, RecursionError):
        return _MISSING
