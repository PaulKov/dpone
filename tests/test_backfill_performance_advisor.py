from __future__ import annotations

from dpone.backfill.advisor import BackfillPerformanceAdvisor


def test_worker_safety_profile_prefers_single_parallel_chunk() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=240,
        historical_rows_per_second=50_000,
        optimize_for="worker_safety",
    )

    assert advice["schema_version"] == "dpone.backfill.advisor.v1"
    assert advice["recommended_max_parallel_chunks"] == 1
    assert advice["risk"] == "low"
    assert "worker_safety" in advice["rationale"]


def test_speed_profile_recommends_bounded_parallelism() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=240,
        historical_rows_per_second=300_000,
        optimize_for="speed",
    )

    assert advice["recommended_max_parallel_chunks"] == 4
    assert advice["risk"] == "medium"
    assert "source pressure" in advice["warnings"][0]


def test_small_campaign_keeps_existing_shape() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=2,
        historical_rows_per_second=None,
        optimize_for="balanced",
    )

    assert advice["recommended_max_parallel_chunks"] == 1
    assert advice["recommendation"] == "keep_current_shape"


def test_advisor_stabilizes_campaign_after_lease_expired_failures() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=20,
        historical_rows_per_second=None,
        optimize_for="speed",
        current_step="1d",
        current_parallel_workers=3,
        lease_ttl_minutes=60,
        chunk_evidence=[
            {
                "index": 1,
                "status": "success",
                "rows_loaded": 200_000,
                "started_at": "2026-07-01T00:00:00+00:00",
                "finished_at": "2026-07-01T00:05:00+00:00",
            },
            {"index": 2, "status": "failed", "attempts": 2, "error": "lease_expired"},
        ],
    )

    assert advice["recommendation"] == "stabilize_failed_chunks"
    assert advice["recommended_max_parallel_chunks"] == 1
    assert advice["recommended_step"] == "1d"
    assert advice["recommended_lease_ttl_minutes"] == 120
    assert advice["recommended_overrides"] == {
        "lease_ttl_minutes": 120,
        "parallel_workers": 1,
    }
    assert advice["evidence"]["failed_chunks"] == 1
    assert "lease_expired" in advice["warnings"]


def test_advisor_increases_window_for_fast_successful_campaign() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=120,
        historical_rows_per_second=None,
        optimize_for="speed",
        current_step="1d",
        current_parallel_workers=2,
        lease_ttl_minutes=60,
        chunk_evidence=[
            {
                "index": 1,
                "status": "success",
                "rows_loaded": 1_200_000,
                "started_at": "2026-07-01T00:00:00+00:00",
                "finished_at": "2026-07-01T00:02:00+00:00",
            },
            {
                "index": 2,
                "status": "success",
                "rows_loaded": 900_000,
                "started_at": "2026-07-01T00:02:00+00:00",
                "finished_at": "2026-07-01T00:03:30+00:00",
            },
        ],
    )

    assert advice["recommendation"] == "increase_window_and_parallelism"
    assert advice["recommended_step"] == "2d"
    assert advice["recommended_max_parallel_chunks"] == 4
    assert advice["recommended_overrides"] == {
        "chunk.step": "2d",
        "parallel_workers": 4,
    }
    assert advice["evidence"]["rows_per_second"] == 10_000


def test_advisor_ignores_malformed_chunk_timing() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=10,
        historical_rows_per_second=None,
        optimize_for="balanced",
        chunk_evidence=[
            {
                "status": "success",
                "rows_loaded": 1_000,
                "started_at": "not-a-timestamp",
                "finished_at": "2026-07-01T00:01:00+00:00",
            }
        ],
    )

    assert advice["evidence"]["rows_per_second"] is None
    assert "historical throughput unavailable" in advice["warnings"][0]


def test_advisor_normalizes_unknown_profile_with_warning() -> None:
    advice = BackfillPerformanceAdvisor().advise(
        chunks_total=20,
        historical_rows_per_second=100_000,
        optimize_for="speeed",
    )

    assert advice["optimize_for"] == "balanced"
    assert "unknown optimize_for=speeed; using balanced" in advice["warnings"]
