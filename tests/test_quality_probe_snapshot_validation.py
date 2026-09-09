"""Fail-closed validation for quality-gate probe snapshots."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateReport,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.runtime.governance.quality_execution import quality_gate_report_evidence
from dpone.runtime.governance.service import LoadGovernanceService


class _RecordingQualityRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ) -> QualityGateReport:
        self.calls += 1
        return QualityGateRunner().run(policy, source=source, target=target)


@pytest.mark.parametrize(
    "row_count",
    [
        pytest.param(True, id="bool"),
        pytest.param(-1, id="negative"),
        pytest.param(1.0, id="float"),
        pytest.param("1", id="string"),
        pytest.param({}, id="mapping"),
        pytest.param([], id="list"),
    ],
)
def test_explicit_invalid_row_count_is_rejected_before_runner(row_count: object) -> None:
    runner = _RecordingQualityRunner()
    policy = _min_rows_policy()

    with pytest.raises(ValueError, match="row_count.*non-negative integer or None"):
        runner.run(
            policy,
            source=QualityProbeSnapshot(row_count=0),
            target=QualityProbeSnapshot(row_count=row_count),
        )

    assert runner.calls == 0


@pytest.mark.parametrize(
    "typed_hash",
    [
        pytest.param("", id="empty"),
        pytest.param(True, id="bool"),
        pytest.param(1, id="integer"),
        pytest.param({}, id="mapping"),
        pytest.param([], id="list"),
    ],
)
def test_explicit_invalid_typed_hash_is_rejected_before_runner(typed_hash: object) -> None:
    runner = _RecordingQualityRunner()
    policy = _typed_hash_policy()

    with pytest.raises(ValueError, match="typed_hash.*non-empty string or None"):
        runner.run(
            policy,
            source=QualityProbeSnapshot(typed_hash="same"),
            target=QualityProbeSnapshot(typed_hash=typed_hash),
        )

    assert runner.calls == 0


@pytest.mark.parametrize("row_count", [None, 0, 1])
@pytest.mark.parametrize("typed_hash", [None, "typed-hash"])
def test_explicit_valid_probe_values_are_preserved(
    row_count: int | None,
    typed_hash: str | None,
) -> None:
    snapshot = QualityProbeSnapshot(row_count=row_count, typed_hash=typed_hash)

    assert snapshot.row_count is row_count
    assert snapshot.typed_hash is typed_hash


@pytest.mark.parametrize(
    "row_count",
    [
        pytest.param(True, id="bool"),
        pytest.param(-1, id="negative"),
        pytest.param(1.0, id="float"),
        pytest.param("1", id="string"),
        pytest.param({}, id="mapping"),
        pytest.param([], id="list"),
    ],
)
def test_untyped_runtime_row_counts_become_unavailable_and_fail_closed(
    row_count: object,
) -> None:
    source, target = _runtime_snapshots(row_count=row_count, typed_hash="same")

    assert source.row_count is None
    assert target.row_count is None

    report = QualityGateRunner().run(_min_rows_policy(), source=source, target=target)

    assert report.passed is False
    assert report.results[0].status == "failed"
    assert report.results[0].metrics["row_count"] is None


@pytest.mark.parametrize(
    "typed_hash",
    [
        pytest.param("", id="empty"),
        pytest.param(True, id="bool"),
        pytest.param(1, id="integer"),
        pytest.param({}, id="mapping"),
        pytest.param([], id="list"),
    ],
)
def test_untyped_runtime_hashes_become_unavailable_and_fail_closed(
    typed_hash: object,
) -> None:
    source, target = _runtime_snapshots(row_count=1, typed_hash=typed_hash)

    assert source.typed_hash is None
    assert target.typed_hash is None

    report = QualityGateRunner().run(_typed_hash_policy(), source=source, target=target)

    assert report.passed is False
    assert report.results[0].status == "failed"
    assert report.results[0].metrics["source_hash"] is None
    assert report.results[0].metrics["target_hash"] is None


def test_passed_report_keeps_required_row_and_hash_metrics_in_safe_evidence() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {"id": "rows", "type": "row_count_reconciliation"},
                {"id": "hash", "type": "typed_hash_reconciliation"},
            ]
        }
    )
    snapshot = QualityProbeSnapshot(row_count=3, typed_hash="same")

    report = QualityGateRunner().run(policy, source=snapshot, target=snapshot)
    evidence = quality_gate_report_evidence(report, policy)

    assert report.passed is True
    assert evidence["passed"] is True
    assert evidence["results"][0]["metrics"] == {
        "source_row_count": 3,
        "target_row_count": 3,
        "difference": 0,
        "allowed_difference": 0.0,
    }
    digest = f"sha256:{hashlib.sha256(b'same').hexdigest()}"
    assert evidence["results"][1]["metrics"] == {
        "source_hash": digest,
        "target_hash": digest,
        "mode": "full",
    }


def _runtime_snapshots(
    *,
    row_count: object,
    typed_hash: object,
) -> tuple[QualityProbeSnapshot, QualityProbeSnapshot]:
    extract_result = SimpleNamespace(
        artifact=SimpleNamespace(rows_exported=row_count),
        typed_hash=typed_hash,
    )
    load_result = SimpleNamespace(
        staging_rows=row_count,
        total_rows=99,
        typed_hash=typed_hash,
    )
    return LoadGovernanceService().quality_probe_snapshots(
        extract_result=extract_result,
        load_result=load_result,
    )


def test_estimated_rows_alone_cannot_certify_source_row_count_gates() -> None:
    extract_result = SimpleNamespace(
        artifact=SimpleNamespace(estimated_rows=5),
        typed_hash=None,
    )
    load_result = SimpleNamespace(staging_rows=5, total_rows=5, typed_hash=None)
    source, target = LoadGovernanceService().quality_probe_snapshots(
        extract_result=extract_result,
        load_result=load_result,
    )

    assert source.row_count is None
    assert target.row_count == 5

    report = QualityGateRunner().run(
        QualityGatePolicy.from_config(
            {
                "gates": [
                    {
                        "id": "rows",
                        "type": "row_count_reconciliation",
                    }
                ]
            }
        ),
        source=source,
        target=target,
    )

    assert report.passed is False
    assert report.results[0].status == "failed"


def _min_rows_policy() -> QualityGatePolicy:
    return QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "target_rows",
                    "type": "min_rows",
                    "side": "target",
                    "threshold": 0,
                }
            ]
        }
    )


def _typed_hash_policy() -> QualityGatePolicy:
    return QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "hash",
                    "type": "typed_hash_reconciliation",
                }
            ]
        }
    )
