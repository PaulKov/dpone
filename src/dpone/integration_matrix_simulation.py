"""Deterministic strategy simulation for integration matrix cases."""

from __future__ import annotations

from typing import Any

from dpone.integration_matrix_behavior import (
    _mock_actual_row_count,
    _mock_quality_checks,
    _mock_strategy_checksum,
    _mock_strategy_notes,
)
from dpone.integration_matrix_constants import INCREMENTAL_CHANGE_RATIO, PHYSICAL_DELETE_RATIO
from dpone.integration_matrix_counts import (
    _changed_count,
    _delete_count,
    _mock_source_row_count,
    _resolve_mock_ratio,
    _resolve_mock_row_count,
    _validate_mock_ratios,
)
from dpone.integration_matrix_samples import (
    _mock_actual_row_samples,
    _mock_source_row_samples_for_strategy,
    _mock_target_row_samples,
)


def simulate_mock_strategy_behavior_for_case(
    case: Any,
    *,
    result_type: type[Any],
    row_count: int | None = None,
    change_ratio: float | None = None,
    delete_ratio: float | None = None,
) -> Any:
    """Execute a deterministic in-memory strategy model for certification preflight."""

    configured_row_count = _resolve_mock_row_count(row_count)
    configured_change_ratio = _resolve_mock_ratio(
        "DPONE_MATRIX_CHANGE_RATIO",
        INCREMENTAL_CHANGE_RATIO,
        override=change_ratio,
    )
    configured_delete_ratio = _resolve_mock_ratio(
        "DPONE_MATRIX_DELETE_RATIO",
        PHYSICAL_DELETE_RATIO,
        override=delete_ratio,
    )
    _validate_mock_ratios(change_ratio=configured_change_ratio, delete_ratio=configured_delete_ratio)
    actual_row_count = _mock_actual_row_count(
        case.strategy,
        sink=case.sink,
        row_count=configured_row_count,
        change_ratio=configured_change_ratio,
        delete_ratio=configured_delete_ratio,
    )
    expected_checksum = _mock_strategy_checksum(
        case.strategy,
        source=case.source,
        sink=case.sink,
        row_count=configured_row_count,
        change_ratio=configured_change_ratio,
        delete_ratio=configured_delete_ratio,
    )
    return result_type(
        case_id=case.case_id,
        source=case.source,
        sink=case.sink,
        strategy=case.strategy,
        target_before=_mock_target_row_samples(
            source=case.source,
            row_count=configured_row_count,
            change_ratio=configured_change_ratio,
            delete_ratio=configured_delete_ratio,
        ),
        source_rows=_mock_source_row_samples_for_strategy(
            case.strategy,
            source=case.source,
            row_count=configured_row_count,
            change_ratio=configured_change_ratio,
            delete_ratio=configured_delete_ratio,
        ),
        expected_rows=_mock_actual_row_samples(
            case.strategy,
            source=case.source,
            sink=case.sink,
            row_count=configured_row_count,
            change_ratio=configured_change_ratio,
            delete_ratio=configured_delete_ratio,
        ),
        actual_rows=_mock_actual_row_samples(
            case.strategy,
            source=case.source,
            sink=case.sink,
            row_count=configured_row_count,
            change_ratio=configured_change_ratio,
            delete_ratio=configured_delete_ratio,
        ),
        target_before_count=configured_row_count,
        source_row_count=_mock_source_row_count(
            case.strategy,
            row_count=configured_row_count,
            change_ratio=configured_change_ratio,
            delete_ratio=configured_delete_ratio,
        ),
        expected_row_count=actual_row_count,
        actual_row_count=actual_row_count,
        expected_checksum=expected_checksum,
        actual_checksum=expected_checksum,
        configured_row_count=configured_row_count,
        change_ratio=configured_change_ratio,
        delete_ratio=configured_delete_ratio,
        changed_row_count=_changed_count(configured_row_count, change_ratio=configured_change_ratio),
        deleted_row_count=_delete_count(configured_row_count, delete_ratio=configured_delete_ratio),
        quality_checks=_mock_quality_checks(case.strategy, sink=case.sink),
        passed=True,
        notes=_mock_strategy_notes(case.strategy, sink=case.sink),
    )


__all__ = ["simulate_mock_strategy_behavior_for_case"]
