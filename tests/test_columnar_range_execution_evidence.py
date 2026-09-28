from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.columnar_range_evidence import ColumnarRangeExecutionEvidence
from dpone.contracts.columnar_range_parallelism import RangeChunkReceipt, RangeEvidenceItem, RangeStageReceipt
from dpone.runtime.columnar_range_parallelism import RangeExecutionResult, build_columnar_range_plan
from dpone.runtime.partitioning import RangePartitioner


def _digest(value: str) -> str:
    return f"sha256:{sha256(value.encode()).hexdigest()}"


def _plan(*, topology: str = "shared_per_run", logical: str = "dbo.events", execution: str = "sql:v1"):
    options = {
        "partitioning": {
            "column": "event_id",
            "num_partitions": 2,
            "export_workers": 2,
            "load_workers": 2,
            "bounds": {"lower": 0, "upper": 20},
            "range_parallelism": {
                "mode": "required",
                "reader_workers": 2,
                "upload_workers": 2,
                "load_workers": 2,
                "max_inflight_ranges": 2,
                "max_inflight_rows": 10,
                "max_inflight_bytes": 100,
                "consistency": "temporal_as_of",
                "consistency_authority": {"as_of": "synthetic-2026-09-28T00:00:00Z"},
                "staging_topology": topology,
            },
        }
    }
    return build_columnar_range_plan(
        RangePartitioner.from_options(options),
        query_identity=_digest(logical),
        execution_identity=_digest(execution),
    )


def _result(range_id: str, ordinal: int, *, rows: int = 1, retained_bytes: int = 8):
    chunks = ()
    if rows:
        chunks = (RangeChunkReceipt(0, f"object://range/{ordinal}/0", _digest(f"chunk:{ordinal}"), rows, 16),)
    return RangeExecutionResult(range_id, rows, retained_bytes, True, chunks)


def _results(plan):
    return tuple(_result(item.range_id, item.ordinal) for item in plan.ranges)


def _extracted(plan):
    return ColumnarRangeExecutionEvidence.from_results(
        plan=plan,
        results=_results(plan),
        observed_reader_concurrency=2,
        observed_upload_concurrency=2,
        rows_high_water=2,
        bytes_high_water=16,
    )


def _stages(plan):
    return {
        item.range_id: RangeStageReceipt(
            item.range_id, f"stage://range/{item.ordinal}", _digest(f"stage:{item.ordinal}"), 1
        )
        for item in plan.ranges
    }


def _shared_stages(plan):
    return {
        item.range_id: RangeStageReceipt(item.range_id, "stage://shared", _digest(f"stage:{item.ordinal}"), 1)
        for item in plan.ranges
    }


def test_logical_plan_links_preview_to_distinct_execution_identity() -> None:
    first = _plan(execution="sql:v1")
    changed_execution = _plan(execution="sql:v2")
    changed_source = _plan(logical="dbo.other", execution="sql:v1")

    assert first.plan_fingerprint == changed_execution.plan_fingerprint
    assert first.execution_plan_fingerprint != changed_execution.execution_plan_fingerprint
    assert first.plan_fingerprint != changed_source.plan_fingerprint
    assert first.to_dict()["plan_fingerprint"] == first.plan_fingerprint
    assert first.to_dict()["execution_plan_fingerprint"] == first.execution_plan_fingerprint

    preview = build_columnar_range_plan(
        RangePartitioner.from_options(
            {
                "partitioning": {
                    "column": "event_id",
                    "num_partitions": 2,
                    "bounds": {"lower": 0, "upper": 20},
                }
            }
        ),
        query_identity=_digest("dbo.events"),
    )
    assert "execution_identity" not in preview.to_dict()
    assert "execution_plan_fingerprint" not in preview.to_dict()
    with pytest.raises(ValueError, match="runtime execution identity"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=preview,
            results=_results(preview),
            observed_reader_concurrency=1,
            observed_upload_concurrency=1,
            rows_high_water=2,
            bytes_high_water=16,
        )


def test_success_requires_staging_quality_assembly_and_publication_receipts() -> None:
    plan = _plan(topology="per_partition")
    extracted = _extracted(plan)
    assert extracted.to_dict()["outcome"]["status"] == "extracted"

    staged = extracted.with_stage_receipts(_stages(plan), observed_load_concurrency=2)
    assert staged.to_dict()["outcome"]["status"] == "staged"
    assert staged.to_dict()["all_ranges_stage_confirmed"] is True
    with pytest.raises(ValueError, match="assembly_receipt_sha256"):
        staged.with_quality_receipt(
            quality_receipt_sha256=_digest("quality"),
        )

    quality_passed = staged.with_quality_receipt(
        quality_receipt_sha256=_digest("quality"),
        assembly_receipt_sha256=_digest("assembly"),
    )
    assert quality_passed.to_dict()["outcome"]["status"] == "quality_passed"
    published = quality_passed.published(publication_receipt_sha256=_digest("publication"))
    assert published.to_dict()["outcome"]["status"] == "published"
    assert published.to_dict()["outcome"]["cleanup"]["status"] == "not_started"
    complete = published.complete(
        publication_receipt_sha256=_digest("publication"),
        cleanup_status="completed",
    )
    payload = complete.to_dict()
    assert payload["outcome"]["status"] == "succeeded"
    assert payload["receipts"] == {
        "assembly": _digest("assembly"),
        "quality": _digest("quality"),
        "publication": _digest("publication"),
    }
    assert payload["consistency"]["authority"] == {"as_of": "synthetic-2026-09-28T00:00:00Z"}


def test_published_state_rejects_missing_receipt_or_premature_cleanup() -> None:
    plan = _plan()
    quality = _extracted(plan).with_stage_receipts(_shared_stages(plan), observed_load_concurrency=2)
    quality = quality.with_quality_receipt(quality_receipt_sha256=_digest("quality"))

    with pytest.raises(ValueError, match="publication_receipt_sha256"):
        replace(quality, outcome_status="published")
    with pytest.raises(ValueError, match="cleanup must remain pending"):
        replace(
            quality,
            outcome_status="published",
            publication_receipt_sha256=_digest("publication"),
            cleanup_status="completed",
        )


def test_partial_or_impossible_measurements_cannot_be_success_evidence() -> None:
    plan = _plan()
    results = _results(plan)

    with pytest.raises(ValueError, match="coverage"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=results[:-1],
            observed_reader_concurrency=1,
            rows_high_water=1,
            bytes_high_water=8,
        )
    with pytest.raises(ValueError, match="configured execution bound"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=results,
            observed_reader_concurrency=99,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="observed reader concurrency"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=results,
            observed_reader_concurrency=0,
            observed_upload_concurrency=1,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="observed upload concurrency"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=results,
            observed_reader_concurrency=2,
            observed_upload_concurrency=0,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="row high-water"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=results,
            observed_reader_concurrency=2,
            observed_upload_concurrency=2,
            rows_high_water=0,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="nonnegative"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=(replace(results[0], rows=-1), results[1]),
            observed_reader_concurrency=2,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="boolean"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=(replace(results[0], eof_confirmed="false"), results[1]),
            observed_reader_concurrency=2,
            observed_upload_concurrency=2,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="range.rows must be an integer"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=(replace(results[0], rows="1"), results[1]),
            observed_reader_concurrency=2,
            observed_upload_concurrency=2,
            rows_high_water=2,
            bytes_high_water=16,
        )
    with pytest.raises(ValueError, match="chunk receipt"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=(replace(results[0], chunks=()), results[1]),
            observed_reader_concurrency=2,
            rows_high_water=2,
            bytes_high_water=16,
        )


def test_failure_keeps_primary_outcome_cancellation_and_partial_stage_facts() -> None:
    plan = _plan()
    results = _results(plan)
    first = plan.ranges[0]
    stage = RangeStageReceipt(first.range_id, "stage://range/0", _digest("partial-stage"), 1)

    evidence = ColumnarRangeExecutionEvidence.from_failure(
        plan=plan,
        results=results,
        primary_outcome="failed",
        failure_code="columnar_range_upload_failed",
        cancellation_requested=True,
        cancellation_observed=True,
        cleanup_status="failed",
        stage_receipts={first.range_id: stage},
        cleanup_failures=("owned_object_delete_failed",),
        observed_reader_concurrency=2,
        observed_upload_concurrency=1,
        observed_load_concurrency=1,
        rows_high_water=2,
        bytes_high_water=16,
    )

    payload = evidence.to_dict()
    assert payload["outcome"]["status"] == "failed"
    assert payload["outcome"]["cancellation_requested"] is True
    assert payload["outcome"]["cancellation_observed"] is True
    assert payload["ranges"][0]["stage"]["receipt_sha256"] == _digest("partial-stage")
    assert payload["all_ranges_eof_confirmed"] is True
    assert payload["all_ranges_stage_confirmed"] is False

    quality_passed = _extracted(plan).with_stage_receipts(_shared_stages(plan), observed_load_concurrency=2)
    quality_passed = quality_passed.with_quality_receipt(quality_receipt_sha256=_digest("quality"))
    unknown = quality_passed.publication_unknown(
        failure_code="columnar_publication_outcome_unknown",
    )
    assert unknown.to_dict()["outcome"]["status"] == "publication_unknown"
    assert unknown.to_dict()["outcome"]["cleanup"]["status"] == "preserved_unknown"
    with pytest.raises(ValueError, match="primary_outcome"):
        ColumnarRangeExecutionEvidence.from_failure(
            plan=plan,
            results=results,
            primary_outcome="publication_unknown",
            failure_code="columnar_publication_outcome_unknown",
            cancellation_requested=False,
            cancellation_observed=False,
            cleanup_status="preserved_unknown",
        )


def test_stage_receipts_must_match_selected_topology() -> None:
    shared = _extracted(_plan())
    with pytest.raises(ValueError, match="one authoritative stage"):
        shared.with_stage_receipts(_stages(_plan()), observed_load_concurrency=2)

    partitioned_plan = _plan(topology="per_partition")
    partitioned = _extracted(partitioned_plan)
    same_stage = {
        item.range_id: RangeStageReceipt(item.range_id, "stage://shared", _digest(f"stage:{item.ordinal}"), 1)
        for item in partitioned_plan.ranges
    }
    with pytest.raises(ValueError, match="unique range-owned"):
        partitioned.with_stage_receipts(same_stage, observed_load_concurrency=2)

    with pytest.raises(ValueError, match="observed load concurrency"):
        partitioned.with_stage_receipts(_stages(partitioned_plan), observed_load_concurrency=0)


def test_codes_and_receipts_are_redacted_canonical_contracts() -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="stable redacted code"):
        ColumnarRangeExecutionEvidence.from_failure(
            plan=plan,
            results=(),
            primary_outcome="failed",
            failure_code="password=secret",
            cancellation_requested=False,
            cancellation_observed=False,
            cleanup_status="completed",
        )
    with pytest.raises(ValueError, match="canonical SHA-256"):
        RangeChunkReceipt(0, "object://range/0/0", "not-a-digest", 1, 1)


def test_public_models_reject_contradictory_terminal_fields() -> None:
    plan = _plan()
    result = _results(plan)[0]
    item = RangeEvidenceItem(result.range_id, result.rows, result.retained_bytes, True, result.chunks)
    with pytest.raises(ValueError, match="must be a boolean"):
        replace(item, eof_confirmed="false")
    with pytest.raises(ValueError, match="tuple of RangeChunkReceipt"):
        replace(item, chunks=list(item.chunks))

    with pytest.raises(ValueError, match="cancelled outcome requires"):
        ColumnarRangeExecutionEvidence.from_failure(
            plan=plan,
            results=(),
            primary_outcome="cancelled",
            failure_code="columnar_range_cancelled",
            cancellation_requested=False,
            cancellation_observed=False,
            cleanup_status="completed",
        )

    succeeded = _extracted(plan).with_stage_receipts(_shared_stages(plan), observed_load_concurrency=2)
    succeeded = succeeded.with_quality_receipt(quality_receipt_sha256=_digest("quality"))
    succeeded = succeeded.complete(publication_receipt_sha256=_digest("publication"), cleanup_status="completed")
    with pytest.raises(ValueError, match="if and only if"):
        replace(succeeded, cleanup_failures=("owned_object_delete_failed",))
    with pytest.raises(ValueError, match="Observed cancellation"):
        replace(succeeded, cancellation_observed=True)
