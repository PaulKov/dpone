"""Bounded validation for native-transfer logical quality-scope evidence."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

_SCOPE_KIND = "dpone.native_transfer.quality_scope.v1"
_SCOPE_FIELDS = frozenset(
    {
        "kind",
        "digest",
        "planned_count",
        "active_count",
        "skipped_committed_count",
        "reactivated_count",
        "source_rows",
        "target_rows",
    }
)
_COUNT_FIELDS = (
    "planned_count",
    "active_count",
    "skipped_committed_count",
    "reactivated_count",
)
_ROW_FIELDS = ("source_rows", "target_rows")


class NativeTransferQualityScopeEvidenceError(ValueError):
    """Reject malformed or unbounded native quality-scope evidence."""

    code = "native_transfer_quality_scope_invalid"

    def __init__(self) -> None:
        super().__init__(self.code)


def normalize_native_quality_scope_summary(
    value: object,
) -> dict[str, object] | None:
    """Return a bounded canonical copy, or fail closed for an invalid summary."""

    if value is None:
        return None
    if not isinstance(value, Mapping) or frozenset(value) != _SCOPE_FIELDS:
        raise NativeTransferQualityScopeEvidenceError
    if value.get("kind") != _SCOPE_KIND or not is_canonical_sha256_digest(value.get("digest")):
        raise NativeTransferQualityScopeEvidenceError

    counts = {field: _non_negative_int(value.get(field)) for field in _COUNT_FIELDS}
    if any(item is None for item in counts.values()):
        raise NativeTransferQualityScopeEvidenceError
    planned = counts["planned_count"]
    active = counts["active_count"]
    skipped = counts["skipped_committed_count"]
    reactivated = counts["reactivated_count"]
    assert planned is not None and active is not None and skipped is not None and reactivated is not None
    if reactivated > skipped or active + skipped - reactivated != planned:
        raise NativeTransferQualityScopeEvidenceError

    rows = {field: _optional_non_negative_int(value.get(field)) for field in _ROW_FIELDS}
    if any(item is _INVALID for item in rows.values()):
        raise NativeTransferQualityScopeEvidenceError

    return {
        "kind": _SCOPE_KIND,
        "digest": value["digest"],
        **counts,
        **rows,
    }


def _non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


_INVALID = object()


def _optional_non_negative_int(value: object) -> int | None | object:
    if value is None:
        return None
    parsed = _non_negative_int(value)
    return parsed if parsed is not None else _INVALID


__all__ = [
    "NativeTransferQualityScopeEvidenceError",
    "normalize_native_quality_scope_summary",
]
