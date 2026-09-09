from __future__ import annotations

from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateRunner,
    QualityProbeSnapshot,
)


def test_quality_policy_builds_multiple_gate_types_from_manifest_contract() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "row_count_reconciliation",
                    "type": "row_count_reconciliation",
                    "severity": "error",
                    "tolerance": {"mode": "absolute", "value": 0},
                },
                {"id": "target_not_empty", "type": "min_rows", "side": "target", "threshold": 1},
                {"id": "typed_hash_sample", "type": "typed_hash_reconciliation", "severity": "warning"},
                {"id": "custom_check", "type": "custom_sql", "sql": "SELECT 0 AS failures"},
            ]
        }
    )

    assert [gate.type for gate in policy.gates] == [
        "row_count_reconciliation",
        "min_rows",
        "typed_hash_reconciliation",
        "custom_sql",
    ]


def test_quality_runner_reports_row_count_and_min_rows_failures() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "row_count_reconciliation",
                    "type": "row_count_reconciliation",
                    "tolerance": {"mode": "absolute", "value": 0},
                },
                {"id": "target_not_empty", "type": "min_rows", "side": "target", "threshold": 10},
            ]
        }
    )
    runner = QualityGateRunner()

    report = runner.run(
        policy,
        source=QualityProbeSnapshot(row_count=12, typed_hash="abc"),
        target=QualityProbeSnapshot(row_count=9, typed_hash="abc"),
    )

    assert report.passed is False
    assert [result.gate_id for result in report.results] == ["row_count_reconciliation", "target_not_empty"]
    assert report.results[0].status == "failed"
    assert report.results[0].metrics["difference"] == 3
    assert report.results[1].status == "failed"


def test_quality_runner_warns_for_typed_hash_sample_without_blocking() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "typed_hash_sample",
                    "type": "typed_hash_reconciliation",
                    "severity": "warning",
                }
            ]
        }
    )

    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=10, typed_hash="source"),
        target=QualityProbeSnapshot(row_count=10, typed_hash="target"),
    )

    assert report.passed is True
    assert report.results[0].status == "warning"
    assert report.results[0].metrics["source_hash"] == "source"


def test_quality_runner_does_not_pass_typed_hash_gate_when_hashes_are_missing() -> None:
    warning_policy = QualityGatePolicy.from_config(
        {"gates": [{"id": "typed_hash_sample", "type": "typed_hash_reconciliation", "severity": "warning"}]}
    )
    error_policy = QualityGatePolicy.from_config(
        {"gates": [{"id": "typed_hash_full", "type": "typed_hash_reconciliation", "severity": "error"}]}
    )

    warning_report = QualityGateRunner().run(
        warning_policy,
        source=QualityProbeSnapshot(row_count=10),
        target=QualityProbeSnapshot(row_count=10),
    )
    error_report = QualityGateRunner().run(
        error_policy,
        source=QualityProbeSnapshot(row_count=10),
        target=QualityProbeSnapshot(row_count=10),
    )

    assert warning_report.passed is True
    assert warning_report.results[0].status == "warning"
    assert warning_report.results[0].message == "typed hash unavailable"
    assert error_report.passed is False
    assert error_report.results[0].status == "failed"
