"""Sample row builders for deterministic integration matrix artifacts."""

from __future__ import annotations

from dpone.integration_matrix_behavior import _mock_kafka_events
from dpone.integration_matrix_constants import MatrixRow
from dpone.integration_matrix_counts import (
    _delete_id_range,
    _insert_id_range,
    _sample_ids,
    _stable_id_range,
    _update_id_range,
)
from dpone.integration_matrix_wide import _mock_row


def _source_snapshot_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows: list[MatrixRow] = []
    rows.extend(
        _mock_row(row_id, source=source, variant="source_updated")
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant="source_stable")
        for row_id in _sample_ids(_stable_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant="source_inserted")
        for row_id in _sample_ids(_insert_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    return tuple(rows)


def _target_before_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows: list[MatrixRow] = []
    rows.extend(
        _mock_row(row_id, source=source, variant="target_deleted_later")
        for row_id in _sample_ids(_delete_id_range(row_count, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant="target_stale")
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant="target_stable")
        for row_id in _sample_ids(_stable_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    return tuple(rows)


def _delta_sample_rows_for_strategy(
    strategy: str,
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    if strategy in {"full_refresh", "snapshot_diff", "scd2"}:
        return _source_snapshot_sample_rows(
            source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    if strategy == "incremental_append":
        return _append_delta_sample_rows(
            source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    if strategy in {"incremental_merge", "xmin"}:
        return _delete_aware_delta_sample_rows(
            strategy=strategy, source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    if strategy == "cdc":
        return _cdc_delta_sample_rows(
            source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    if strategy in {"replace", "partition_replace", "backfill"}:
        return _replace_delta_sample_rows(
            source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    raise ValueError(f"Unsupported integration matrix strategy: {strategy}")


def _append_delta_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows = _delete_aware_delta_sample_rows(
        strategy="incremental_append",
        source=source,
        row_count=row_count,
        change_ratio=change_ratio,
        delete_ratio=delete_ratio,
    )
    return tuple({(row["id"], row.get("__dpone__op")): row for row in rows}.values())


def _delete_aware_delta_sample_rows(
    *,
    strategy: str,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows: list[MatrixRow] = []
    rows.extend(
        _mock_row(row_id, source=source, variant=f"{strategy}_delete", op="delete")
        for row_id in _sample_ids(_delete_id_range(row_count, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant=f"{strategy}_update", op="update")
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant=f"{strategy}_insert", op="insert")
        for row_id in _sample_ids(_insert_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    return tuple(rows)


def _cdc_delta_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    return _delete_aware_delta_sample_rows(
        strategy="cdc", source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
    )


def _replace_delta_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows: list[MatrixRow] = []
    rows.extend(
        _mock_row(row_id, source=source, variant="replace_update", op="update")
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        _mock_row(row_id, source=source, variant="replace_insert", op="insert")
        for row_id in _sample_ids(_insert_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    return tuple(rows)


def _mock_target_row_samples(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    return _target_before_sample_rows(
        source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
    )


def _mock_source_row_samples_for_strategy(
    strategy: str,
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    return _delta_sample_rows_for_strategy(
        strategy, source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
    )


def _scd2_actual_sample_rows(
    *,
    source: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    rows: list[MatrixRow] = []
    rows.extend(
        {
            **_mock_row(row_id, source=source, variant="scd2_expired_deleted", op="delete"),
            "__dpone__is_current": False,
        }
        for row_id in _sample_ids(_delete_id_range(row_count, delete_ratio=delete_ratio))
    )
    rows.extend(
        {
            **_mock_row(row_id, source=source, variant="scd2_expired_changed", op="update"),
            "__dpone__is_current": False,
        }
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        {
            **_mock_row(row_id, source=source, variant="scd2_current_changed", op="insert"),
            "__dpone__is_current": True,
        }
        for row_id in _sample_ids(_update_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        {
            **_mock_row(row_id, source=source, variant="scd2_current_stable"),
            "__dpone__is_current": True,
        }
        for row_id in _sample_ids(_stable_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    rows.extend(
        {
            **_mock_row(row_id, source=source, variant="scd2_current_inserted", op="insert"),
            "__dpone__is_current": True,
        }
        for row_id in _sample_ids(_insert_id_range(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio))
    )
    return tuple(rows)


def _mock_actual_row_samples(
    strategy: str,
    *,
    source: str,
    sink: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> tuple[MatrixRow, ...]:
    if sink == "kafka":
        if strategy == "snapshot_diff":
            return _mock_kafka_events(
                strategy,
                _delete_aware_delta_sample_rows(
                    strategy=strategy,
                    source=source,
                    row_count=row_count,
                    change_ratio=change_ratio,
                    delete_ratio=delete_ratio,
                ),
            )
        return _mock_kafka_events(
            strategy,
            _delta_sample_rows_for_strategy(
                strategy, source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
            ),
        )
    if strategy == "scd2":
        return _scd2_actual_sample_rows(
            source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
        )
    if strategy == "incremental_append":
        return (
            *_target_before_sample_rows(
                source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
            ),
            *_delta_sample_rows_for_strategy(
                strategy, source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
            ),
        )
    return _source_snapshot_sample_rows(
        source=source, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio
    )


__all__ = [
    "_source_snapshot_sample_rows",
    "_target_before_sample_rows",
    "_delta_sample_rows_for_strategy",
    "_append_delta_sample_rows",
    "_delete_aware_delta_sample_rows",
    "_cdc_delta_sample_rows",
    "_replace_delta_sample_rows",
    "_mock_target_row_samples",
    "_mock_source_row_samples_for_strategy",
    "_scd2_actual_sample_rows",
    "_mock_actual_row_samples",
]
