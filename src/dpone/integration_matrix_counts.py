"""Count, ratio, and selection helpers for integration matrix mock runs."""

from __future__ import annotations

import os

from dpone.integration_matrix_constants import DEFAULT_MOCK_ROW_COUNT, MAX_MOCK_ROW_COUNT


def _merge_unique(*chunks: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    for chunk in chunks:
        for item in chunk:
            if item and item not in values:
                values.append(item)
    return tuple(values)


def _split_filter(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    normalized = value.strip()
    if not normalized or normalized == "*":
        return ()
    return tuple(part.strip() for part in normalized.split(",") if part.strip())


def _matches_filter(value: str, allowed: tuple[str, ...]) -> bool:
    return not allowed or value in allowed


def _resolve_mock_row_count(row_count: int | None = None) -> int:
    if row_count is None:
        raw = (os.environ.get("DPONE_MATRIX_MOCK_ROW_COUNT") or os.environ.get("DPONE_MATRIX_ROW_COUNT") or "").strip()
        row_count = int(raw) if raw else DEFAULT_MOCK_ROW_COUNT
    if row_count < 1:
        raise ValueError("DPONE_MATRIX_MOCK_ROW_COUNT must be positive")
    if row_count > MAX_MOCK_ROW_COUNT:
        raise ValueError(f"DPONE_MATRIX_MOCK_ROW_COUNT must be <= {MAX_MOCK_ROW_COUNT}")
    return row_count


def _resolve_mock_ratio(name: str, default: float, *, override: float | None = None) -> float:
    ratio = override
    if ratio is None:
        raw = os.environ.get(name, "").strip()
        ratio = float(raw) if raw else default
    if ratio < 0 or ratio > 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return ratio


def _validate_mock_ratios(*, change_ratio: float, delete_ratio: float) -> None:
    if delete_ratio > change_ratio:
        raise ValueError("DPONE_MATRIX_DELETE_RATIO must be <= DPONE_MATRIX_CHANGE_RATIO")


def _changed_count(row_count: int, *, change_ratio: float) -> int:
    return max(1, int(row_count * change_ratio))


def _delete_count(row_count: int, *, delete_ratio: float) -> int:
    return max(1, int(row_count * delete_ratio))


def _insert_count(row_count: int, *, change_ratio: float, delete_ratio: float) -> int:
    changed_count = _changed_count(row_count, change_ratio=change_ratio)
    deleted_count = _delete_count(row_count, delete_ratio=delete_ratio)
    return min(deleted_count, max(0, changed_count - deleted_count))


def _update_count(row_count: int, *, change_ratio: float, delete_ratio: float) -> int:
    changed_count = _changed_count(row_count, change_ratio=change_ratio)
    deleted_count = _delete_count(row_count, delete_ratio=delete_ratio)
    inserted_count = _insert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    return max(0, changed_count - deleted_count - inserted_count)


def _upsert_count(row_count: int, *, change_ratio: float, delete_ratio: float) -> int:
    return _update_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio) + _insert_count(
        row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
    )


def _source_snapshot_count_after_delta(row_count: int, *, change_ratio: float, delete_ratio: float) -> int:
    return (
        row_count
        - _delete_count(row_count, delete_ratio=delete_ratio)
        + _insert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    )


def _mock_source_row_count(strategy: str, *, row_count: int, change_ratio: float, delete_ratio: float) -> int:
    if strategy in {"full_refresh", "snapshot_diff", "scd2"}:
        return _source_snapshot_count_after_delta(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    if strategy in {"incremental_append", "incremental_merge", "xmin", "cdc"}:
        return _changed_count(row_count, change_ratio=change_ratio)
    if strategy in {"replace", "partition_replace", "backfill"}:
        return _upsert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    raise ValueError(f"Unsupported integration matrix strategy: {strategy}")


def _sample_ids(ids: range, *, sample_size: int = 3) -> tuple[int, ...]:
    values = list(ids)
    if len(values) <= sample_size * 2:
        return tuple(values)
    return tuple((*values[:sample_size], *values[-sample_size:]))


def _delete_id_range(row_count: int, *, delete_ratio: float) -> range:
    return range(1, _delete_count(row_count, delete_ratio=delete_ratio) + 1)


def _update_id_range(row_count: int, *, change_ratio: float, delete_ratio: float) -> range:
    deleted_count = _delete_count(row_count, delete_ratio=delete_ratio)
    updated_count = _update_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    return range(deleted_count + 1, deleted_count + updated_count + 1)


def _insert_id_range(row_count: int, *, change_ratio: float, delete_ratio: float) -> range:
    inserted_count = _insert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    return range(row_count + 1, row_count + inserted_count + 1)


def _stable_id_range(row_count: int, *, change_ratio: float, delete_ratio: float) -> range:
    deleted_count = _delete_count(row_count, delete_ratio=delete_ratio)
    updated_count = _update_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    return range(deleted_count + updated_count + 1, row_count + 1)


__all__ = [
    "_merge_unique",
    "_split_filter",
    "_matches_filter",
    "_resolve_mock_row_count",
    "_resolve_mock_ratio",
    "_validate_mock_ratios",
    "_changed_count",
    "_delete_count",
    "_insert_count",
    "_update_count",
    "_upsert_count",
    "_source_snapshot_count_after_delta",
    "_mock_source_row_count",
    "_sample_ids",
    "_delete_id_range",
    "_update_id_range",
    "_insert_id_range",
    "_stable_id_range",
]
