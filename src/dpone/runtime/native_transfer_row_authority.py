"""Independent source/target row authorities for native-transfer quality."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint


from collections.abc import Mapping, Sequence
from typing import Any

ROW_COUNT_AUTHORITY = "source_export_and_target_loader"
SOURCE_ROW_COUNT_INVALID = "native_transfer_source_row_count_invalid"
TARGET_ROW_COUNT_INVALID = "native_transfer_target_row_count_invalid"


def canonical_non_negative_int(value: object) -> int | None:
    """Accept only exact non-negative integers; never coerce."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def require_target_row_count(value: object) -> int:
    """Fail closed when a sink loader does not return an exact row count."""

    rows = canonical_non_negative_int(value)
    if rows is None:
        raise RuntimeError(TARGET_ROW_COUNT_INVALID)
    return rows


def require_source_row_count(value: object) -> int:
    """Fail closed when an export advertises an invalid completed row count."""

    rows = canonical_non_negative_int(value)
    if rows is None:
        raise RuntimeError(SOURCE_ROW_COUNT_INVALID)
    return rows


def actual_export_rows(artifact: object) -> int | None:
    """Read an explicit completed export count; never use planned estimates."""

    for attribute in ("rows_exported", "row_count"):
        if not hasattr(artifact, attribute):
            continue
        value = getattr(artifact, attribute)
        if value is None:
            continue
        rows = canonical_non_negative_int(value)
        if rows is None:
            raise RuntimeError(SOURCE_ROW_COUNT_INVALID)
        return rows
    return None


def authority_diagnostics(
    *,
    source_rows: int | None,
    target_rows: int | None,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach additive provenance only when both authorities validate."""

    diagnostics = dict(base or {})
    if source_rows is None or target_rows is None:
        return diagnostics
    diagnostics["rows_loaded"] = target_rows
    diagnostics["row_count_authority"] = ROW_COUNT_AUTHORITY
    return diagnostics


def trusted_checkpoint_rows(checkpoint: PartitionCheckpoint) -> tuple[int | None, int | None]:
    """Return independent source/target counts only for marked checkpoints."""

    if checkpoint.diagnostics.get("row_count_authority") != ROW_COUNT_AUTHORITY:
        return None, None
    source_rows = canonical_non_negative_int(checkpoint.rows_exported)
    target_rows = canonical_non_negative_int(checkpoint.diagnostics.get("rows_loaded"))
    if source_rows is None or target_rows is None:
        return None, None
    return source_rows, target_rows


def slice_export_rows_by_key(
    slice_evidence: Sequence[object] | None,
) -> dict[tuple[int, int], int] | None:
    """Map each slice to its completed export row count when evidence is complete."""

    if not slice_evidence:
        return None
    observed: dict[tuple[int, int], int] = {}
    for item in slice_evidence:
        if not isinstance(item, Mapping):
            return None
        partition_index = canonical_non_negative_int(item.get("partition_index"))
        slice_index = canonical_non_negative_int(item.get("slice_index"))
        rows_exported = canonical_non_negative_int(item.get("rows_exported"))
        if partition_index is None or slice_index is None or rows_exported is None:
            return None
        key = (partition_index, slice_index)
        if key in observed:
            return None
        observed[key] = rows_exported
    return observed


def sum_slice_export_rows(slice_evidence: Sequence[object] | None) -> int | None:
    """Return the total completed export rows when every slice is authoritative."""

    observed = slice_export_rows_by_key(slice_evidence)
    if observed is None:
        return None
    if not observed:
        return 0
    return sum(observed.values())


def sum_completed_chunk_rows(chunks: Sequence[object]) -> int:
    """Sum exact chunk authorities or fail closed without coercion."""

    total = 0
    for chunk in chunks:
        rows = canonical_non_negative_int(getattr(chunk, "row_count", None))
        if rows is None:
            raise RuntimeError(SOURCE_ROW_COUNT_INVALID)
        total += rows
    return total


def stamp_single_partition_loader_rows(active_artifacts: tuple[object, ...], load_result: object) -> None:
    """Copy an exact single-partition loader count onto the attempt-owned artifact.

    Multi-partition loads must publish ``rows_loaded`` themselves. This helper
    only fills the unambiguous one-artifact case used by direct sink loads.
    """

    if len(active_artifacts) != 1:
        return
    artifact = active_artifacts[0]
    if canonical_non_negative_int(getattr(artifact, "rows_loaded", None)) is not None:
        return
    rows_loaded = canonical_non_negative_int(getattr(load_result, "inserted_rows", None))
    if rows_loaded is None:
        rows_loaded = canonical_non_negative_int(getattr(load_result, "total_rows", None))
    if rows_loaded is None:
        return
    setattr(artifact, "rows_loaded", rows_loaded)


__all__ = [
    "ROW_COUNT_AUTHORITY",
    "SOURCE_ROW_COUNT_INVALID",
    "TARGET_ROW_COUNT_INVALID",
    "actual_export_rows",
    "authority_diagnostics",
    "canonical_non_negative_int",
    "require_source_row_count",
    "require_target_row_count",
    "slice_export_rows_by_key",
    "stamp_single_partition_loader_rows",
    "sum_completed_chunk_rows",
    "sum_slice_export_rows",
    "trusted_checkpoint_rows",
]
