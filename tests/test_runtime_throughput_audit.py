from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dpone.runtime.runtime_throughput import (
    RUNTIME_THROUGHPUT_SCHEMA_VERSION,
    enrich_run_result_with_throughput,
    enrich_step_details_with_throughput,
)


def test_step_details_get_rows_and_bytes_per_second() -> None:
    started_at = datetime(2026, 7, 2, 14, 0, tzinfo=UTC)
    finished_at = started_at + timedelta(seconds=4)

    details = enrich_step_details_with_throughput(
        {"staged_rows": 20, "bytes_written": 8 * 1024 * 1024},
        started_at=started_at,
        finished_at=finished_at,
        status="succeeded",
    )

    assert details["throughput"] == {
        "schema_version": RUNTIME_THROUGHPUT_SCHEMA_VERSION,
        "scope": "load_step",
        "duration_seconds": 4.0,
        "rate_type": "counter_over_wall_clock",
        "row_count": 20,
        "row_count_source": "staged_rows",
        "rows_per_second": 5.0,
        "byte_count": 8 * 1024 * 1024,
        "byte_count_source": "bytes_written",
        "bytes_per_second": 2 * 1024 * 1024,
        "mib_per_second": 2.0,
        "confidence": "measured",
    }


def test_step_details_without_counts_are_left_compact() -> None:
    started_at = datetime(2026, 7, 2, 14, 0, tzinfo=UTC)

    details = enrich_step_details_with_throughput(
        {"selected": "direct"},
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=1),
        status="succeeded",
    )

    assert details == {"selected": "direct"}


def test_step_details_keep_zero_row_throughput_when_no_positive_counter_exists() -> None:
    started_at = datetime(2026, 7, 2, 14, 0, tzinfo=UTC)

    details = enrich_step_details_with_throughput(
        {"staged_rows": 0},
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=5),
        status="succeeded",
    )

    assert details["throughput"]["row_count"] == 0
    assert details["throughput"]["rows_per_second"] == 0.0


def test_step_details_prefer_positive_counter_over_earlier_zero_counter() -> None:
    started_at = datetime(2026, 7, 2, 14, 0, tzinfo=UTC)

    details = enrich_step_details_with_throughput(
        {"loaded_rows": 0, "staged_rows": 50},
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=10),
        status="succeeded",
    )

    assert details["throughput"]["row_count"] == 50
    assert details["throughput"]["row_count_source"] == "staged_rows"
    assert details["throughput"]["rows_per_second"] == 5.0


def test_run_result_gets_overall_rows_per_second() -> None:
    result = {
        "duration_seconds": 2.5,
        "loaded_rows": 100,
        "extracted_rows": 100,
        "status": "success",
    }

    enrich_run_result_with_throughput(result)

    assert result["run_throughput"] == {
        "schema_version": RUNTIME_THROUGHPUT_SCHEMA_VERSION,
        "scope": "run",
        "duration_seconds": 2.5,
        "rate_type": "counter_over_wall_clock",
        "row_count": 100,
        "row_count_source": "loaded_rows",
        "rows_per_second": 40.0,
        "confidence": "measured",
    }
