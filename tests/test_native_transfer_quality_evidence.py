from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.native_transfer_quality_evidence import (
    NativeTransferQualityScopeEvidenceError,
    normalize_native_quality_scope_summary,
)
from dpone.runtime.native_transfer_report import build_runtime_report_payload
from dpone.runtime.sinks.load_result import LoadResult


def _summary() -> dict[str, object]:
    return {
        "kind": "dpone.native_transfer.quality_scope.v1",
        "digest": f"sha256:{'a' * 64}",
        "planned_count": 2,
        "active_count": 1,
        "skipped_committed_count": 1,
        "reactivated_count": 0,
        "source_rows": 8,
        "target_rows": 8,
    }


def _context() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="quality-scope-report",
        load_config=SimpleNamespace(
            options={"source_type": "mssql", "sink_type": "clickhouse"},
            load_strategy=SimpleNamespace(value="incremental_append"),
        ),
        checkpoint_store=None,
        resume_plan=SimpleNamespace(to_dict=lambda: {"summary": {"skip": 1, "retry": 1}}),
    )


def test_native_quality_scope_summary_is_canonical_and_bounded() -> None:
    summary = _summary()

    normalized = normalize_native_quality_scope_summary(summary)

    assert normalized == summary
    assert normalized is not summary


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param({"partition_ids": ["secret-id"]}, id="extra-field"),
        pytest.param({"planned_count": True}, id="boolean-count"),
        pytest.param({"digest": f"sha256:{'A' * 64}"}, id="noncanonical-digest"),
        pytest.param({"source_rows": -1}, id="negative-rows"),
    ],
)
def test_native_quality_scope_summary_rejects_unbounded_or_noncanonical_values(
    mutation: dict[str, object],
) -> None:
    summary = {**_summary(), **mutation}

    with pytest.raises(
        NativeTransferQualityScopeEvidenceError,
        match="native_transfer_quality_scope_invalid",
    ):
        normalize_native_quality_scope_summary(summary)


def test_native_runtime_report_persists_only_validated_quality_scope() -> None:
    summary = _summary()
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=8,
        staging_rows=1,
        reconciliation_metrics={"native_transfer_quality_scope": summary},
    )

    payload = build_runtime_report_payload(_context(), load_result)

    assert payload["quality_scope"] == summary


def test_native_runtime_report_fails_closed_for_invalid_quality_scope() -> None:
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=8,
        staging_rows=1,
        reconciliation_metrics={
            "native_transfer_quality_scope": {
                **_summary(),
                "partition_ids": ["must-not-enter-evidence"],
            }
        },
    )

    with pytest.raises(
        NativeTransferQualityScopeEvidenceError,
        match="native_transfer_quality_scope_invalid",
    ):
        build_runtime_report_payload(_context(), load_result)
