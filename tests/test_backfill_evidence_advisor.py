from __future__ import annotations

from dpone.backfill.advisor import BackfillPerformanceAdvisor
from dpone.backfill.evidence import BackfillEvidenceCollector


def test_advisor_uses_load_step_evidence_to_find_sink_bottleneck() -> None:
    evidence = BackfillEvidenceCollector().collect(
        chunks=[
            {
                "index": 1,
                "status": "success",
                "rows_loaded": 100_000,
                "started_at": "2026-07-01T00:00:00+00:00",
                "finished_at": "2026-07-01T00:10:00+00:00",
            },
            {
                "index": 2,
                "status": "success",
                "rows_loaded": 100_000,
                "started_at": "2026-07-01T00:10:00+00:00",
                "finished_at": "2026-07-01T00:20:00+00:00",
            },
        ],
        load_steps=[
            {
                "step_id": "source_read",
                "status": "succeeded",
                "details_json": {
                    "throughput": {
                        "rows_per_second": 25_000,
                        "row_count": 200_000,
                        "duration_seconds": 8,
                    }
                },
            },
            {
                "step_id": "sink_finalize",
                "status": "succeeded",
                "details_json": {
                    "throughput": {
                        "rows_per_second": 1_250,
                        "row_count": 200_000,
                        "duration_seconds": 160,
                    }
                },
            },
        ],
    )

    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=40,
        historical_rows_per_second=None,
        optimize_for="speed",
        current_step="1d",
        current_parallel_workers=2,
        chunk_evidence=evidence.observations,
    )

    assert evidence.to_dict()["sources"] == ["chunk_ledger", "load_steps"]
    assert advice["evidence"]["bottleneck_stage"] == "sink_finalize"
    assert advice["recommendation"] == "tune_sink_finalize_before_parallelism"
    assert advice["recommended_max_parallel_chunks"] == 1
    assert "sink finalize is slower than source read" in advice["warnings"]


def test_advisor_recommends_larger_windows_for_many_empty_chunks() -> None:
    evidence = BackfillEvidenceCollector().collect(
        chunks=[
            {"index": 1, "status": "success", "rows_loaded": 0},
            {"index": 2, "status": "success", "rows_loaded": 0},
            {
                "index": 3,
                "status": "success",
                "rows_loaded": 120_000,
                "started_at": "2026-07-01T00:00:00+00:00",
                "finished_at": "2026-07-01T00:01:00+00:00",
            },
            {"index": 4, "status": "success", "rows_loaded": 0},
        ],
    )

    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=40,
        historical_rows_per_second=None,
        optimize_for="balanced",
        current_step="1d",
        current_parallel_workers=1,
        chunk_evidence=evidence.observations,
    )

    assert advice["evidence"]["empty_chunks"] == 3
    assert advice["evidence"]["empty_chunk_ratio"] == 0.75
    assert advice["recommendation"] == "increase_window_to_reduce_empty_chunks"
    assert advice["recommended_step"] == "2d"


def test_failed_vendor_matrix_blocks_parallelism_as_certification_issue() -> None:
    evidence = BackfillEvidenceCollector().collect(
        matrix_report={
            "passed": False,
            "total_cases": 10,
            "failed_cases": 2,
            "blockers": ["matrix_rows.mismatch:clickhouse_to_mssql__replace"],
        }
    )

    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=60,
        historical_rows_per_second=250_000,
        optimize_for="speed",
        current_step="1d",
        current_parallel_workers=2,
        chunk_evidence=evidence.observations,
    )

    assert advice["recommendation"] == "resolve_vendor_certification_failures"
    assert advice["recommended_max_parallel_chunks"] == 1
    assert advice["risk"] == "high"
    assert advice["evidence"]["failed_matrix_reports"] == 1
    assert advice["evidence"]["failed_matrix_cases"] == 2
    assert advice["evidence"]["matrix_blockers"] == ["matrix_rows.mismatch:clickhouse_to_mssql__replace"]
    assert advice["evidence"]["failed_chunks"] == 0
    assert "vendor backfill certification failed; fix matrix blockers before tuning throughput" in advice["warnings"]


def test_green_vendor_matrix_does_not_count_as_empty_chunk() -> None:
    evidence = BackfillEvidenceCollector().collect(
        chunks=[{"index": 1, "status": "success", "rows_loaded": 0}],
        matrix_report={
            "passed": True,
            "total_cases": 10,
            "failed_cases": 0,
        },
    )

    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=20,
        historical_rows_per_second=None,
        optimize_for="balanced",
        current_step="1d",
        current_parallel_workers=1,
        chunk_evidence=evidence.observations,
    )

    assert advice["evidence"]["matrix_reports_observed"] == 1
    assert advice["evidence"]["matrix_cases"] == 10
    assert advice["evidence"]["empty_chunks"] == 1
    assert advice["evidence"]["empty_chunk_ratio"] == 1.0
    assert advice["recommendation"] != "increase_window_to_reduce_empty_chunks"
    assert advice["recommended_step"] == "1d"


def test_load_step_stage_rates_use_conservative_minimum() -> None:
    evidence = BackfillEvidenceCollector().collect(
        load_steps=[
            {
                "step_id": "source_read",
                "details_json": {"throughput": {"rows_per_second": 80_000, "row_count": 800_000}},
            },
            {
                "step_id": "source_read",
                "details_json": {"throughput": {"rows_per_second": 5_000, "row_count": 50_000}},
            },
            {
                "step_id": "sink_finalize",
                "details_json": {"throughput": {"rows_per_second": 20_000, "row_count": 200_000}},
            },
        ],
    )

    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=80,
        historical_rows_per_second=None,
        optimize_for="speed",
        current_step="1d",
        current_parallel_workers=2,
        chunk_evidence=evidence.observations,
    )

    assert advice["evidence"]["stage_rows_per_second"]["source_read"] == 5_000
    assert advice["evidence"]["bottleneck_stage"] == "source_read"
    assert advice["recommendation"] == "increase_parallelism"
