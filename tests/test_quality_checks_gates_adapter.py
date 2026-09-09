"""Contract tests for manifest quality.checks → load-governance gates adapter."""

from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from dpone.contracts.quality_failure import (
    QualityGateFailureOutcome,
    QualityGateReceiptInvalid,
    QualityGateReceiptMismatch,
    QualityGateReceiptRequired,
)
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateReport,
    QualityGateResult,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.governance.quality_normalize import normalize_quality_config
from dpone.manifest.authoring import AuthoringCompiler
from dpone.readiness.airflow_authoring_validation import validate_pipeline_source
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.quality_execution import QualityGateExecution
from dpone.runtime.governance.service import (
    LoadGovernanceService,
    QualityGateFailure,
    QualityGateReceipt,
    StagedQualityGateReceipt,
    report_from_staged_quality_receipt,
)
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sources.base import ExtractResult


def test_normalize_maps_airflow_checks_min_rows_and_source_target_count() -> None:
    normalized = normalize_quality_config(
        {
            "mode": "fail",
            "checks": [
                {"type": "min_rows", "threshold": 1},
                {"type": "source_target_count", "tolerance_pct": 0},
            ],
        }
    )

    gates = normalized["gates"]
    assert [gate["type"] for gate in gates] == ["min_rows", "row_count_reconciliation"]
    assert gates[0]["threshold"] == 1
    assert gates[0]["severity"] == "error"
    assert gates[0]["side"] == "target"
    assert gates[1]["tolerance"] == {"mode": "pct", "value": 0.0}


def test_normalize_accepts_min_rows_value_alias() -> None:
    normalized = normalize_quality_config(
        {"mode": "fail", "checks": [{"type": "min_rows", "value": 5, "side": "source"}]}
    )
    assert normalized["gates"][0]["threshold"] == 5
    assert normalized["gates"][0]["side"] == "source"


@pytest.mark.parametrize(
    "quality",
    [
        pytest.param({}, id="empty-mapping"),
        pytest.param({"mode": "fail"}, id="mode-only"),
        pytest.param({"checks": []}, id="empty-checks"),
        pytest.param({"gates": []}, id="empty-gates"),
        pytest.param({"checks": [], "gates": []}, id="both-empty"),
    ],
)
def test_authored_empty_quality_fails_closed(quality: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="authored quality"):
        normalize_quality_config(quality)


@pytest.mark.parametrize(
    "check",
    [
        pytest.param({"type": "min_rows"}, id="missing"),
        pytest.param({"type": "min_rows", "threshhold": 1}, id="misspelled"),
    ],
)
def test_normalize_min_rows_requires_threshold_or_value(check: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="min_rows requires threshold or value"):
        normalize_quality_config({"mode": "fail", "checks": [check]})


def test_normalize_warn_mode_maps_severity_warning() -> None:
    normalized = normalize_quality_config({"mode": "warn", "checks": [{"type": "min_rows", "threshold": 1}]})
    assert normalized["gates"][0]["severity"] == "warning"


def test_normalize_prefers_explicit_gates_and_keeps_them() -> None:
    normalized = normalize_quality_config(
        {
            "mode": "fail",
            "gates": [{"id": "target_not_empty", "type": "min_rows", "threshold": 2}],
        }
    )
    assert len(normalized["gates"]) == 1
    assert normalized["gates"][0]["threshold"] == 2


def test_normalize_fail_closed_on_unknown_check_type() -> None:
    with pytest.raises(ValueError, match="unsupported quality check type"):
        normalize_quality_config({"mode": "fail", "checks": [{"type": "null_counts", "columns": ["a"]}]})


def test_unknown_warning_check_is_preserved_as_non_blocking_skipped_evidence() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "mode": "warn",
            "checks": [{"id": "legacy_nulls", "type": "null_counts", "columns": ["a"]}],
        }
    )

    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=1),
        target=QualityProbeSnapshot(row_count=1),
    )

    assert report.passed is True
    assert report.results[0].gate_id == "legacy_nulls"
    assert report.results[0].type == "null_counts"
    assert report.results[0].severity == "warning"
    assert report.results[0].status == "skipped"
    assert "not implemented" in report.results[0].message


def test_normalize_fail_closed_when_checks_and_gates_both_present_under_fail() -> None:
    with pytest.raises(ValueError, match="leftover quality.checks"):
        normalize_quality_config(
            {
                "mode": "fail",
                "gates": [{"id": "g1", "type": "min_rows", "threshold": 1}],
                "checks": [{"type": "min_rows", "threshold": 1}],
            }
        )


def test_policy_from_airflow_checks_fails_empty_target() -> None:
    policy = QualityGatePolicy.from_config({"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1}]})
    assert len(policy.gates) == 1
    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=0),
        target=QualityProbeSnapshot(row_count=0),
    )
    assert report.passed is False
    assert report.results[0].gate_id
    assert report.results[0].status == "failed"


def test_load_governance_service_raises_on_empty_load_with_checks() -> None:
    class _Extract:
        artifact = type("A", (), {"rows_exported": 0})()
        typed_hash = None

    class _Load:
        staging_rows = 0
        total_rows = 0
        typed_hash = None

    class _Config:
        options = {"quality": {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1}]}}

    with pytest.raises(RuntimeError, match="quality gates failed"):
        LoadGovernanceService().run_quality_gates(
            load_config=_Config(),
            extract_result=_Extract(),
            load_result=_Load(),
        )


def test_quality_gate_failure_report_survives_run_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from dpone.commands.run_cmd import _write_run_failure

    report = QualityGateReport(
        results=(
            QualityGateResult(
                gate_id="target_not_empty",
                type="min_rows",
                status="failed",
                severity="error",
                metrics={"target_row_count": 0, "password": "must-not-leak"},
                message="target row count is below threshold",
            ),
        )
    )

    _write_run_failure(
        SimpleNamespace(
            format="json",
            path=tmp_path / "pipelines/orders/pipeline.yaml",
            selector=None,
            run_id="orders-run",
            retry_attempts=0,
            retry_backoff_seconds=0.0,
        ),
        QualityGateFailure(report),
    )

    payload = json.loads(capsys.readouterr().out)
    quality_gates = payload["result"]["quality_gates"]
    assert payload["passed"] is False
    assert payload["manifest"] == "pipeline.yaml"
    assert tmp_path.as_posix() not in json.dumps(payload)
    assert quality_gates["passed"] is False
    assert quality_gates["results"][0]["gate_id"] == "target_not_empty"
    assert quality_gates["results"][0]["metrics"]["password"] == "[REDACTED]"
    assert "must-not-leak" not in json.dumps(payload)


def test_checks_reach_compiler_builder_processor_governance_and_cli_json(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from dpone.commands.run_cmd import _write_run_failure

    source_path = tmp_path / "pipelines/orders/pipeline.yaml"
    payload = _flow_payload(
        quality={
            "mode": "fail",
            "checks": [{"id": "target_not_empty", "type": "min_rows", "threshold": 1}],
        }
    )
    compilation = AuthoringCompiler().compile(payload, source_path=source_path, project_root=tmp_path)
    load_config = LoadConfigBuilder().build(dict(compilation.processes[0]))
    source = _ProcessorSource()
    load_service = _ZeroLoadService()

    with pytest.raises(QualityGateFailure) as raised:
        ETLProcessor(
            source,
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=load_service,
        ).run(load_config, dag_id="orders")

    _write_run_failure(
        SimpleNamespace(
            format="json",
            path=source_path,
            selector=None,
            run_id="orders-run",
            retry_attempts=0,
            retry_backoff_seconds=0.0,
        ),
        raised.value,
    )

    cli_payload = json.loads(capsys.readouterr().out)
    quality_gates = cli_payload["result"]["quality_gates"]
    assert source.extract_calls == 1
    assert load_service.load_calls == 1
    assert cli_payload["passed"] is False
    assert cli_payload["result"]["error_code"] == "DPONE_QUALITY_GATES_FAILED"
    assert quality_gates["passed"] is False
    assert quality_gates["results"][0]["gate_id"] == "target_not_empty"
    assert quality_gates["results"][0]["metrics"]["row_count"] == 0


@pytest.mark.parametrize(
    "untrusted_report",
    [
        pytest.param(None, id="none"),
        pytest.param("passed", id="string"),
        pytest.param({"passed": False}, id="failed-mapping"),
        pytest.param({}, id="empty-mapping"),
    ],
)
def test_reconciliation_quality_mapping_never_suppresses_processor_policy(
    untrusted_report: object,
) -> None:
    load_config = LoadConfigBuilder().build(
        _compiled_process(
            quality={
                "gates": [
                    {
                        "id": "target_not_empty",
                        "type": "min_rows",
                        "threshold": 1,
                        "severity": "error",
                    }
                ]
            }
        )
    )

    with pytest.raises(QualityGateReceiptRequired):
        ETLProcessor(
            _ProcessorSource(),
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=_FixedLoadService(
                LoadResult(
                    inserted_rows=0,
                    updated_rows=0,
                    total_rows=0,
                    staging_rows=0,
                    reconciliation_metrics={"quality_gates": untrusted_report},
                )
            ),
        ).run(load_config, dag_id="orders")


@pytest.mark.parametrize("row_count", [True, -1, "1"])
def test_invalid_target_row_count_is_unavailable_for_min_rows(row_count: object) -> None:
    load_config = SimpleNamespace(
        options={
            "quality": {
                "gates": [
                    {
                        "id": "target_min_rows",
                        "type": "min_rows",
                        "side": "target",
                        "threshold": 0,
                        "severity": "error",
                    }
                ]
            }
        }
    )

    with pytest.raises(QualityGateFailure) as raised:
        LoadGovernanceService().run_quality_gates(
            load_config=load_config,
            extract_result=_quality_extract_result(1),
            load_result=SimpleNamespace(staging_rows=row_count, total_rows=row_count),
        )

    assert raised.value.report.results[0].metrics["row_count"] is None


@pytest.mark.parametrize("row_count", [True, -1, "1"])
def test_invalid_row_counts_are_unavailable_for_reconciliation(row_count: object) -> None:
    load_config = SimpleNamespace(
        options={
            "quality": {
                "gates": [
                    {
                        "id": "row_count_reconciliation",
                        "type": "row_count_reconciliation",
                        "severity": "error",
                    }
                ]
            }
        }
    )

    with pytest.raises(QualityGateFailure) as raised:
        LoadGovernanceService().run_quality_gates(
            load_config=load_config,
            extract_result=_quality_extract_result(row_count),
            load_result=SimpleNamespace(staging_rows=row_count, total_rows=row_count),
        )

    assert raised.value.report.results[0].metrics["source_row_count"] is None
    assert raised.value.report.results[0].metrics["target_row_count"] is None


def test_staged_typed_quality_receipt_executes_policy_exactly_once() -> None:
    load_config = LoadConfigBuilder().build(
        _compiled_process(
            quality={
                "gates": [
                    {
                        "id": "target_not_empty",
                        "type": "min_rows",
                        "threshold": 1,
                        "severity": "error",
                    }
                ]
            }
        )
    )
    governance = _CountingGovernanceService()
    source_state = _RecordingSourceStateService()

    result = ETLProcessor(
        _ProcessorSource(),
        object(),
        etl_logger=_SilentLogger(),
        extracted_payload_load_service=_AuthorityLoadService(boundary="pre_commit"),
        load_governance_service=governance,
        source_state_service=source_state,
    ).run(load_config, dag_id="orders")

    assert result["status"] == "success"
    assert governance.quality_calls == 1
    assert source_state.persist_calls == 1


def test_processor_revalidates_execution_snapshot_before_source_state() -> None:
    load_config = _quality_load_config()
    source_state = _RecordingSourceStateService()

    with pytest.raises(QualityGateReceiptMismatch):
        ETLProcessor(
            _ProcessorSource(),
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=_AuthorityLoadService(
                boundary="pre_commit",
                drift_after_payload=True,
            ),
            source_state_service=source_state,
        ).run(load_config, dag_id="orders")

    assert source_state.persist_calls == 0


@pytest.mark.parametrize("boundary", ["pre_commit", "post_commit", "resume_validation"])
def test_direct_typed_quality_receipt_cannot_suppress_processor_authority(boundary: str) -> None:
    load_config = _quality_load_config()
    passing_report = QualityGateRunner().run(
        QualityGatePolicy.from_config(load_config.options["quality"]),
        source=QualityProbeSnapshot(row_count=1),
        target=QualityProbeSnapshot(row_count=1),
    )
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=1,
        staging_rows=1,
        quality_gate_receipt=QualityGateReceipt(
            report=passing_report,
            boundary=boundary,
        ),
    )
    governance = _CountingGovernanceService()

    with pytest.raises(QualityGateReceiptInvalid):
        ETLProcessor(
            _ProcessorSource(),
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=_FixedLoadService(load_result),
            load_governance_service=governance,
        ).run(load_config, dag_id="orders")

    assert governance.quality_calls == 0


def test_staged_quality_receipt_and_report_facade_remain_compatible_aliases() -> None:
    report = QualityGateReport(results=())
    receipt = StagedQualityGateReceipt(report=report)

    assert StagedQualityGateReceipt is QualityGateReceipt
    assert receipt.boundary == "pre_commit"
    assert report_from_staged_quality_receipt(receipt) is report


def test_failed_staged_typed_quality_receipt_cannot_yield_success() -> None:
    load_config = _quality_load_config()
    failed_report = QualityGateRunner().run(
        QualityGatePolicy.from_config(load_config.options["quality"]),
        source=QualityProbeSnapshot(row_count=0),
        target=QualityProbeSnapshot(row_count=0),
    )
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=1,
        staging_rows=1,
        quality_gate_receipt=StagedQualityGateReceipt(report=failed_report),
    )
    governance = _CountingGovernanceService()

    with pytest.raises(QualityGateFailure) as raised:
        ETLProcessor(
            _ProcessorSource(),
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=_FixedLoadService(load_result),
            load_governance_service=governance,
        ).run(load_config, dag_id="orders")

    assert raised.value.report == failed_report
    assert governance.quality_calls == 0


def test_quality_gate_failure_old_constructor_does_not_invent_mutation_outcome() -> None:
    report = QualityGateReport(results=())

    failure = QualityGateFailure(report)

    assert failure.report is report
    assert failure.outcome is None
    assert str(failure) == "quality gates failed"


def test_quality_gate_failure_outcome_canonicalizes_untrusted_numeric_values() -> None:
    outcome = QualityGateFailureOutcome(
        failure_boundary="post_commit",
        target_state="mutation_returned_success",
        checkpoint_state="not_applicable",
        source_state="not_advanced",
        retry_classification="retry_may_repeat_target_mutation",
        inserted_rows=True,
        updated_rows=-1,
        final_rows="9",
        extracted_rows=7,
        attempts=0,
    )

    assert outcome.inserted_rows is None
    assert outcome.updated_rows is None
    assert outcome.final_rows is None
    assert outcome.extracted_rows == 7
    assert outcome.attempts == 1
    assert outcome.failure_context() == {
        "failure_boundary": "post_commit",
        "target_state": "mutation_returned_success",
        "checkpoint_state": "not_applicable",
        "source_state": "not_advanced",
        "retry_classification": "retry_may_repeat_target_mutation",
    }


@pytest.mark.parametrize(
    "receipt_factory",
    [
        pytest.param(lambda report: SimpleNamespace(report=report), id="untyped-receipt"),
        pytest.param(
            lambda report: _malformed_typed_receipt(report),
            id="typed-receipt-with-untyped-report",
        ),
    ],
)
def test_malformed_staged_quality_receipt_cannot_yield_success(receipt_factory) -> None:  # noqa: ANN001
    passing_report = QualityGateReport(results=())
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=1,
        staging_rows=1,
        quality_gate_receipt=receipt_factory(passing_report),
    )
    governance = _CountingGovernanceService()

    with pytest.raises(RuntimeError, match="staged quality gate receipt is invalid"):
        ETLProcessor(
            _ProcessorSource(),
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=_FixedLoadService(load_result),
            load_governance_service=governance,
        ).run(_quality_load_config(), dag_id="orders")

    assert governance.quality_calls == 0


@pytest.mark.parametrize("quality", [{}, {"checks": []}, {"gates": []}])
def test_processor_rejects_authored_empty_quality_before_extract(quality: dict[str, object]) -> None:
    load_config = LoadConfigBuilder().build(_compiled_process(quality=quality))
    source = _ProcessorSource()
    load_service = _ZeroLoadService()

    with pytest.raises(ValueError, match="authored quality"):
        ETLProcessor(
            source,
            object(),
            etl_logger=_SilentLogger(),
            extracted_payload_load_service=load_service,
        ).run(load_config, dag_id="orders")

    assert source.extract_calls == 0
    assert load_service.load_calls == 0


def test_unimplemented_error_gate_type_is_fail_closed_not_skipped() -> None:
    policy = QualityGatePolicy.from_config(
        {"gates": [{"id": "custom_check", "type": "custom_sql", "severity": "error", "sql": "SELECT 1"}]}
    )
    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=1),
        target=QualityProbeSnapshot(row_count=1),
    )
    assert report.passed is False
    assert report.results[0].status == "failed"
    assert "not implemented" in report.results[0].message


@pytest.mark.parametrize("gate_type", ["not_null", "unique"])
def test_declared_studio_gate_type_is_fail_closed_until_an_evaluator_exists(gate_type: str) -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": f"selected_key_{gate_type}",
                    "type": gate_type,
                    "side": "target",
                    "severity": "error",
                }
            ]
        }
    )

    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=1),
        target=QualityProbeSnapshot(row_count=1),
    )

    assert report.passed is False
    assert report.results[0].status == "failed"
    assert "not implemented" in report.results[0].message


@pytest.mark.parametrize("field", ["checks", "gates"])
def test_quality_normalization_rejects_malformed_entries_instead_of_dropping_them(field: str) -> None:
    entry = {"type": "min_rows", "threshold": 1}

    with pytest.raises(ValueError, match=rf"quality\.{field}\[1\] must be a mapping"):
        normalize_quality_config({"mode": "fail", field: [entry, "not-a-mapping"]})


def test_quality_policy_rejects_unknown_severity_instead_of_allowing_false_success() -> None:
    with pytest.raises(ValueError, match="severity must be one of"):
        QualityGatePolicy.from_config(
            {
                "gates": [
                    {
                        "id": "target_not_empty",
                        "type": "min_rows",
                        "threshold": 1,
                        "severity": "typo",
                    }
                ]
            }
        )


def test_quality_policy_rejects_unknown_min_rows_side_instead_of_using_target() -> None:
    with pytest.raises(ValueError, match="side must be one of"):
        QualityGatePolicy.from_config(
            {
                "gates": [
                    {
                        "id": "source_not_empty",
                        "type": "min_rows",
                        "threshold": 1,
                        "side": "soruce",
                    }
                ]
            }
        )


def test_quality_normalization_rejects_unknown_top_level_mode() -> None:
    with pytest.raises(ValueError, match="quality mode must be one of"):
        normalize_quality_config({"mode": "silently-ignore", "checks": [{"type": "min_rows", "threshold": 1}]})


def test_warn_mode_maps_leftover_checks_when_canonical_gates_are_present() -> None:
    normalized = normalize_quality_config(
        {
            "mode": "warn",
            "gates": [{"id": "canonical", "type": "min_rows", "threshold": 1}],
            "checks": [{"id": "compatibility", "type": "source_target_count", "tolerance_pct": 5}],
        }
    )

    assert [gate["id"] for gate in normalized["gates"]] == ["canonical", "compatibility"]
    assert normalized["gates"][1]["severity"] == "warning"
    assert normalized["gates"][1]["type"] == "row_count_reconciliation"


@pytest.mark.parametrize("threshold", [-1, 0.9, True, "1.5"])
def test_quality_normalization_rejects_non_integer_or_negative_thresholds(threshold: object) -> None:
    with pytest.raises(ValueError, match="threshold"):
        normalize_quality_config({"mode": "fail", "checks": [{"type": "min_rows", "threshold": threshold}]})


@pytest.mark.parametrize("tolerance", [-1, math.inf, -math.inf, math.nan])
def test_quality_normalization_rejects_negative_or_non_finite_tolerance(tolerance: float) -> None:
    with pytest.raises(ValueError, match="tolerance_pct"):
        normalize_quality_config(
            {
                "mode": "fail",
                "checks": [{"type": "source_target_count", "tolerance_pct": tolerance}],
            }
        )


def test_quality_policy_rejects_unknown_tolerance_mode() -> None:
    with pytest.raises(ValueError, match="tolerance mode"):
        QualityGatePolicy.from_config(
            {
                "gates": [
                    {
                        "id": "rows",
                        "type": "row_count_reconciliation",
                        "tolerance": {"mode": "percentish", "value": 1},
                    }
                ]
            }
        )


@pytest.mark.parametrize(
    "gate",
    [
        {"id": "rows", "type": "row_count_reconciliation", "side": "typo"},
        {"id": "rows", "type": "row_count_reconciliation", "mode": "typo"},
    ],
)
def test_quality_policy_rejects_invalid_optional_enum_values(gate: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="must be one of"):
        QualityGatePolicy.from_config({"gates": [gate]})


@pytest.mark.parametrize(
    ("gate", "field"),
    [
        (
            {"id": "rows", "type": "min_rows", "threshold": 1, "severity": "  "},
            "severity",
        ),
        (
            {"id": "rows", "type": "min_rows", "threshold": 1, "side": ""},
            "side",
        ),
        (
            {
                "id": "rows",
                "type": "row_count_reconciliation",
                "tolerance": {"mode": " ", "value": 0},
            },
            "tolerance mode",
        ),
        (
            {"id": "hash", "type": "typed_hash_reconciliation", "mode": ""},
            "mode",
        ),
    ],
)
def test_quality_policy_rejects_present_but_blank_enum_values(
    gate: dict[str, object],
    field: str,
) -> None:
    with pytest.raises(ValueError, match=field):
        QualityGatePolicy.from_config({"gates": [gate]})


def test_quality_policy_rejects_duplicate_gate_ids() -> None:
    gate = {"id": "rows", "type": "row_count_reconciliation"}
    with pytest.raises(ValueError, match="duplicate"):
        QualityGatePolicy.from_config({"gates": [gate, dict(gate)]})


def test_quality_policy_rejects_min_rows_gate_without_threshold() -> None:
    with pytest.raises(ValueError, match="min_rows requires threshold"):
        QualityGatePolicy.from_config({"gates": [{"id": "target_rows", "type": "min_rows"}]})


def test_error_severity_skipped_gate_cannot_make_report_pass() -> None:
    report = QualityGateReport(
        results=(
            QualityGateResult(
                gate_id="required_gate",
                type="custom_sql",
                status="skipped",
                severity="error",
            ),
        )
    )

    assert report.passed is False


def test_row_count_reconciliation_fails_when_a_required_probe_is_missing() -> None:
    policy = QualityGatePolicy.from_config({"gates": [{"id": "rows", "type": "row_count_reconciliation"}]})

    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=None),
        target=QualityProbeSnapshot(row_count=None),
    )

    assert report.passed is False
    assert report.results[0].status == "failed"
    assert report.results[0].metrics["source_row_count"] is None
    assert report.results[0].metrics["target_row_count"] is None
    assert report.results[0].message == "row count probe unavailable"


def test_min_rows_warning_is_explicit_when_probe_is_missing() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "target_rows",
                    "type": "min_rows",
                    "threshold": 0,
                    "severity": "warning",
                }
            ]
        }
    )

    report = QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=0),
        target=QualityProbeSnapshot(row_count=None),
    )

    assert report.passed is True
    assert report.results[0].status == "warning"
    assert report.results[0].metrics["row_count"] is None
    assert report.results[0].message == "row count probe unavailable"


def test_classic_and_flow_authoring_share_the_same_quality_schema_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    schema_paths = (
        root / "src/dpone/schema/etl-batch-manifest.schema.json",
        root / "src/dpone/schema/etl-flow-manifest.schema.json",
    )
    quality_schemas = [
        json.loads(path.read_text(encoding="utf-8"))["definitions"]["managed_quality"] for path in schema_paths
    ]

    assert quality_schemas[0] == quality_schemas[1]
    validator = Draft7Validator(quality_schemas[0])
    validator.validate({"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1}]})
    validator.validate(
        {
            "gates": [
                {
                    "id": "target_not_empty",
                    "type": "min_rows",
                    "side": "target",
                    "threshold": 1,
                    "severity": "error",
                }
            ]
        }
    )
    with pytest.raises(ValidationError):
        validator.validate({"gates": [{"id": "target_not_empty", "type": "min_rows", "severity": "typo"}]})
    with pytest.raises(ValidationError):
        validator.validate({"checks": ["not-a-mapping"]})
    with pytest.raises(ValidationError):
        validator.validate({"checks": [{"type": "min_rows", "threshhold": 1}]})


def test_flow_and_batch_process_quality_must_be_mappings() -> None:
    root = Path(__file__).resolve().parents[1]
    flow_schema = json.loads((root / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads((root / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    flow_process_schema = {
        "$ref": "#/definitions/process",
        "definitions": flow_schema["definitions"],
    }
    batch_process_schema = {
        "$ref": "#/definitions/process_fragment",
        "definitions": batch_schema["definitions"],
    }

    with pytest.raises(ValidationError):
        Draft7Validator(flow_process_schema).validate(
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_ref": "source",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "sink",
                    "table": {"schema": "raw", "name": "orders"},
                    "strategy": {"mode": "full_refresh"},
                },
                "quality": [],
            }
        )
    with pytest.raises(ValidationError):
        Draft7Validator(batch_process_schema).validate({"quality": "invalid"})


def test_static_authoring_check_rejects_unknown_fail_mode_compatibility_check(tmp_path: Path) -> None:
    path = tmp_path / "pipelines/orders/pipeline.yaml"
    errors = validate_pipeline_source(
        _flow_payload(
            quality={
                "mode": "fail",
                "checks": [{"type": "provider_check"}],
            }
        ),
        path,
        root=tmp_path,
    )

    assert [error["code"] for error in errors] == ["DPONE_AUTHORING_COMPILATION_FAILED"]
    assert "unsupported quality check type" in errors[0]["message"]


def test_quality_schema_preserves_valid_v1_extension_and_implicit_gate_id_objects() -> None:
    root = Path(__file__).resolve().parents[1]
    schema = json.loads((root / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))[
        "definitions"
    ]["managed_quality"]
    validator = Draft7Validator(schema)

    validator.validate(
        {
            "mode": "warn",
            "checks": [
                {
                    "type": "vendor_quality_check",
                    "threshold": "provider-defined",
                    "provider_options": {"enabled": True},
                }
            ],
        }
    )
    validator.validate({"gates": [{"type": "min_rows", "threshold": 1}]})


def _flow_payload(*, quality: dict[str, object]) -> dict[str, object]:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipelines/orders/pipeline.yaml"},
        "metadata": {"id": "orders", "domain": "sales"},
        "quality": quality,
        "processes": [_compiled_process()],
    }


def _compiled_process(*, quality: dict[str, object] | None = None) -> dict[str, object]:
    process: dict[str, object] = {
        "name": "orders",
        "source": {
            "type": "mssql",
            "connection_id": "source",
            "table": {"schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "sink",
            "table": {"schema": "raw", "name": "orders"},
            "strategy": {"mode": "full_refresh"},
        },
    }
    if quality is not None:
        process["quality"] = quality
    return process


class _ProcessorSource:
    def __init__(self) -> None:
        self.extract_calls = 0

    def extract(self, load_config: object, state: object) -> ExtractResult:
        del load_config, state
        self.extract_calls += 1
        return ExtractResult(artifact=InMemoryRowsArtifact([]), schema=[])


class _ZeroLoadService:
    def __init__(self) -> None:
        self.load_calls = 0

    def load_extracted_payload(self, **kwargs: object) -> tuple[LoadResult, None]:
        self.load_calls += 1
        load_result = LoadResult(
            inserted_rows=0,
            updated_rows=0,
            total_rows=0,
            staging_rows=0,
        )
        LoadGovernanceService().run_quality_gates(
            load_config=kwargs["load_config"],
            extract_result=kwargs["extract_result"],
            load_result=load_result,
        )
        return load_result, None


class _FixedLoadService:
    def __init__(self, load_result: LoadResult) -> None:
        self._load_result = load_result

    def load_extracted_payload(self, **kwargs: object) -> tuple[LoadResult, dict[str, object] | None]:
        del kwargs
        metrics = self._load_result.reconciliation_metrics
        return self._load_result, dict(metrics) if metrics is not None else None


class _AuthorityLoadService:
    def __init__(self, *, boundary: str, drift_after_payload: bool = False) -> None:
        self._boundary = boundary
        self._drift_after_payload = drift_after_payload

    def load_extracted_payload(self, **kwargs: object) -> tuple[LoadResult, dict[str, object]]:
        execution = kwargs["quality_execution"]
        assert isinstance(execution, QualityGateExecution)
        load_config = kwargs["load_config"]
        execution.select_boundary(self._boundary, load_config=load_config)
        receipt = execution.evaluate(
            load_config=load_config,
            boundary=self._boundary,
            source_snapshot=QualityProbeSnapshot(row_count=1),
            target_snapshot=QualityProbeSnapshot(row_count=1),
        )
        evidence = execution.evidence_projection(receipt, load_config=load_config)
        execution.accept_payload(receipt, load_config=load_config)
        if self._drift_after_payload:
            load_config.options.pop("quality")
        result = LoadResult(
            inserted_rows=1,
            updated_rows=0,
            total_rows=1,
            staging_rows=1,
            quality_gate_receipt=receipt,
            reconciliation_metrics={"quality_gates": evidence},
        )
        return result, {"quality_gates": evidence}


class _CountingQualityRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, *args: object, **kwargs: object) -> QualityGateReport:
        self.calls += 1
        return QualityGateRunner().run(*args, **kwargs)


class _CountingGovernanceService(LoadGovernanceService):
    def __init__(self) -> None:
        self._counting_runner = _CountingQualityRunner()
        super().__init__(quality_runner=self._counting_runner)

    @property
    def quality_calls(self) -> int:
        return self._counting_runner.calls


class _RecordingSourceStateService:
    def __init__(self) -> None:
        self.persist_calls = 0

    def load_for_extract(self, source: object, load_config: object) -> None:
        del source, load_config

    def persist_after_load(self, **kwargs: object) -> None:
        del kwargs
        self.persist_calls += 1


def _quality_extract_result(row_count: object) -> SimpleNamespace:
    return SimpleNamespace(
        artifact=SimpleNamespace(rows_exported=row_count),
        typed_hash=None,
    )


def _quality_load_config():
    return LoadConfigBuilder().build(
        _compiled_process(
            quality={
                "gates": [
                    {
                        "id": "target_not_empty",
                        "type": "min_rows",
                        "threshold": 1,
                        "severity": "error",
                    }
                ]
            }
        )
    )


def _malformed_typed_receipt(report: QualityGateReport):
    del report
    return StagedQualityGateReceipt(report={})


class _SilentLogger:
    def info(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def warning(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def log_etl_start(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def log_etl_end(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def log_etl_error(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def log_etl_progress(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
