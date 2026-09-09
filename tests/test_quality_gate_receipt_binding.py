"""Fail-closed contracts for producer-owned quality-gate receipts."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.contracts.quality_failure import (
    QualityGateFailure,
    QualityGateFailureOutcome,
    QualityGateReceiptInvalid,
    QualityGateReceiptMismatch,
    QualityGateReceiptRequired,
)
from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateResult,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.payload_loader import PayloadLoadService
from dpone.runtime.etl.result_metrics import (
    enrich_post_commit_quality_failure_result,
    staged_quality_gate_report,
)
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.legacy_acceptance import LegacyLoadGovernanceCoordinator
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.governance.quality_execution import (
    QualityExecutionSnapshot,
    QualityGateExecution,
)
from dpone.runtime.governance.quality_receipt import (
    QualityGateReceipt,
    validate_quality_gate_receipt,
)
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sources.base import ExtractResult

_SCOPE_SUMMARY = {
    "kind": "dpone.native_transfer.quality_scope.v1",
    "digest": "sha256:" + "a" * 64,
    "planned_count": 2,
    "active_count": 1,
    "skipped_committed_count": 1,
    "reactivated_count": 0,
    "source_rows": 11,
    "target_rows": 11,
}


def _policy(*gates: dict[str, object]) -> QualityGatePolicy:
    return QualityGatePolicy.from_config({"gates": list(gates)})


def _run(policy: QualityGatePolicy):
    return QualityGateRunner().run(
        policy,
        source=QualityProbeSnapshot(row_count=11, typed_hash="same"),
        target=QualityProbeSnapshot(row_count=11, typed_hash="same"),
    )


def test_canonical_runner_stamps_deterministic_normalized_policy_binding() -> None:
    canonical_policy = QualityGatePolicy.from_config(
        {
            "gates": [
                {
                    "id": "rows",
                    "type": "row_count_reconciliation",
                    "severity": "error",
                    "tolerance": {"mode": "pct", "value": 1},
                }
            ]
        }
    )
    compatibility_policy = QualityGatePolicy.from_config(
        {
            "mode": "fail",
            "checks": [
                {
                    "id": "rows",
                    "type": "source_target_count",
                    "tolerance_pct": 1,
                }
            ],
        }
    )
    sql_policy = _policy(
        {
            "id": "source_sql",
            "type": "custom_sql",
            "side": "source",
            "severity": "warning",
            "sql": "SELECT password FROM secret_table",
        }
    )

    canonical_report = _run(canonical_policy)
    compatibility_report = _run(compatibility_policy)

    assert canonical_report.policy_fingerprint == compatibility_report.policy_fingerprint
    assert canonical_report.policy_fingerprint.startswith("sha256:")
    assert len(canonical_report.policy_fingerprint) == len("sha256:") + 64
    assert canonical_report.gate_contract == (
        {"gate_id": "rows", "type": "row_count_reconciliation", "severity": "error"},
    )
    serialized = str(_run(sql_policy).to_jsonable())
    assert "SELECT password" not in serialized
    assert "secret_table" not in serialized


def test_gate_order_and_sql_changes_change_the_policy_fingerprint() -> None:
    first = _policy(
        {"id": "one", "type": "custom_sql", "severity": "warning", "sql": "SELECT 1"},
        {"id": "two", "type": "min_rows", "threshold": 1},
    )
    reordered = _policy(
        {"id": "two", "type": "min_rows", "threshold": 1},
        {"id": "one", "type": "custom_sql", "severity": "warning", "sql": "SELECT 1"},
    )
    changed_sql = _policy(
        {"id": "one", "type": "custom_sql", "severity": "warning", "sql": "SELECT 2"},
        {"id": "two", "type": "min_rows", "threshold": 1},
    )

    assert _run(first).policy_fingerprint != _run(reordered).policy_fingerprint
    assert _run(first).policy_fingerprint != _run(changed_sql).policy_fingerprint


def test_non_empty_policy_rejects_missing_and_legacy_unbound_receipts() -> None:
    policy = _policy({"id": "rows", "type": "min_rows", "threshold": 1})
    unbound_report = replace(
        _run(QualityGatePolicy()),
        policy_fingerprint=None,
        gate_contract=(),
    )

    for receipt in (None, QualityGateReceipt(report=unbound_report)):
        with pytest.raises(QualityGateReceiptRequired) as raised:
            validate_quality_gate_receipt(receipt, policy=policy)

        assert raised.value.code == "DPONE_QUALITY_GATE_RECEIPT_REQUIRED"


def test_empty_policy_keeps_explicit_legacy_compatibility() -> None:
    policy = QualityGatePolicy()
    legacy_receipt = QualityGateReceipt(report=replace(_run(policy), policy_fingerprint=None))
    legacy_receipt = replace(legacy_receipt, report=replace(legacy_receipt.report, gate_contract=()))

    assert validate_quality_gate_receipt(None, policy=policy) is None
    assert validate_quality_gate_receipt(legacy_receipt, policy=policy) == legacy_receipt


def test_quality_execution_snapshot_is_deterministic_and_immutable() -> None:
    config = _load_config()

    first = QualityExecutionSnapshot.from_load_config(config)
    second = QualityExecutionSnapshot.from_load_config(config)

    assert first == second
    assert first.policy_snapshot_id.startswith("sha256:")
    assert len(first.policy_snapshot_id) == len("sha256:") + 64
    assert first.gate_policy.gates[0].severity == "error"
    assert first.acceptance_policy.enabled is False
    with pytest.raises((AttributeError, TypeError)):
        first.gate_policy.gates[0].raw["severity"] = "warning"


@pytest.mark.parametrize(
    "boundary",
    ["before_target", "before_evidence", "before_payload", "before_state"],
)
def test_non_empty_to_empty_policy_drift_fails_at_every_irreversible_boundary(
    boundary: str,
) -> None:
    config = _load_config()
    execution = _quality_execution(config)

    if boundary == "before_target":
        _remove_quality_policy(config)
        with pytest.raises(QualityGateReceiptMismatch):
            execution.select_boundary("pre_commit", load_config=config)
        return

    execution.select_boundary("pre_commit", load_config=config)
    receipt = _evaluate(execution, config, boundary="pre_commit")
    _remove_quality_policy(config)

    with pytest.raises(QualityGateReceiptMismatch):
        if boundary == "before_evidence":
            execution.evidence_projection(receipt, load_config=config)
        elif boundary == "before_payload":
            execution.accept_payload(receipt, load_config=config)
        else:
            stable_config = _load_config()
            stable_execution = _quality_execution(stable_config)
            stable_execution.select_boundary("pre_commit", load_config=stable_config)
            stable_receipt = _evaluate(stable_execution, stable_config, boundary="pre_commit")
            stable_execution.accept_payload(stable_receipt, load_config=stable_config)
            _remove_quality_policy(stable_config)
            stable_execution.accept_state(stable_receipt, load_config=stable_config)


def test_execution_rejects_direct_and_dataclass_replaced_receipts() -> None:
    config = _load_config()
    execution = _quality_execution(config)
    execution.select_boundary("pre_commit", load_config=config)
    receipt = _evaluate(execution, config, boundary="pre_commit")
    direct = QualityGateReceipt(
        report=receipt.report,
        boundary=receipt.boundary,
        scope_summary=receipt.scope_summary,
        policy_snapshot_id=receipt.policy_snapshot_id,
        run_id=receipt.run_id,
        load_id=receipt.load_id,
    )

    for forged in (direct, replace(receipt)):
        with pytest.raises(QualityGateReceiptInvalid):
            execution.accept_payload(forged, load_config=config)

    assert execution.accept_payload(receipt, load_config=config) is receipt


def test_execution_rejects_cross_execution_receipt_replay() -> None:
    config = _load_config()
    first = _quality_execution(config, run_id="run-1")
    second = _quality_execution(config, run_id="run-2")
    first.select_boundary("pre_commit", load_config=config)
    second.select_boundary("pre_commit", load_config=config)
    first_receipt = _evaluate(first, config, boundary="pre_commit")
    second_receipt = _evaluate(second, config, boundary="pre_commit")

    with pytest.raises(QualityGateReceiptInvalid):
        second.accept_payload(first_receipt, load_config=config)

    assert second.accept_payload(second_receipt, load_config=config) is second_receipt


def test_execution_rejects_wrong_boundary_and_wrong_probe_snapshot() -> None:
    config = _load_config()
    execution = _quality_execution(config)
    execution.select_boundary("pre_commit", load_config=config)

    with pytest.raises(QualityGateReceiptInvalid):
        _evaluate(execution, config, boundary="post_commit")
    with pytest.raises(QualityGateReceiptInvalid):
        execution.evaluate(
            load_config=config,
            boundary="pre_commit",
            source_snapshot=SimpleNamespace(row_count=11),
            target_snapshot=QualityProbeSnapshot(row_count=11),
        )


def test_execution_rejects_out_of_order_and_replayed_consumption() -> None:
    config = _load_config()
    execution = _quality_execution(config)
    execution.select_boundary("resume_validation", load_config=config)
    receipt = _evaluate(execution, config, boundary="resume_validation")

    with pytest.raises(QualityGateReceiptInvalid):
        execution.accept_state(receipt, load_config=config)

    execution.accept_payload(receipt, load_config=config)
    with pytest.raises(QualityGateReceiptInvalid):
        execution.accept_payload(receipt, load_config=config)
    execution.accept_state(receipt, load_config=config)
    with pytest.raises(QualityGateReceiptInvalid):
        execution.accept_state(receipt, load_config=config)


def test_execution_allows_exactly_one_concurrent_payload_consumer() -> None:
    config = _load_config()
    execution = _quality_execution(config)
    execution.select_boundary("post_commit", load_config=config)
    receipt = _evaluate(execution, config, boundary="post_commit")
    barrier = Barrier(2)

    def consume() -> str:
        barrier.wait()
        try:
            execution.accept_payload(receipt, load_config=config)
        except QualityGateReceiptInvalid:
            return "rejected"
        return "accepted"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(consume) for _ in range(2)]
        outcomes = sorted(future.result() for future in futures)

    assert outcomes == ["accepted", "rejected"]


@pytest.mark.parametrize(
    ("runner_kind", "first_error"),
    [
        ("failed", QualityGateFailure),
        ("throwing", RuntimeError),
        ("invalid", QualityGateReceiptInvalid),
    ],
)
def test_failed_evaluation_terminally_consumes_authority(
    runner_kind: str,
    first_error: type[Exception],
) -> None:
    config = _load_config()
    runner = {
        "failed": _FailingRunner,
        "throwing": _ThrowingRunner,
        "invalid": _InvalidStatusRunner,
    }[runner_kind]()
    execution = _quality_execution(config, runner=runner)
    execution.select_boundary("pre_commit", load_config=config)

    with pytest.raises(first_error):
        _evaluate(execution, config, boundary="pre_commit")
    with pytest.raises(QualityGateReceiptInvalid):
        _evaluate(execution, config, boundary="pre_commit")


def test_execution_allows_exactly_one_concurrent_evaluator() -> None:
    config = _load_config()
    runner = _CountingRunner()
    execution = _quality_execution(config, runner=runner)
    execution.select_boundary("pre_commit", load_config=config)
    barrier = Barrier(2)

    def evaluate() -> str:
        barrier.wait()
        try:
            _evaluate(execution, config, boundary="pre_commit")
        except QualityGateReceiptInvalid:
            return "rejected"
        return "issued"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(evaluate) for _ in range(2)]
        outcomes = sorted(future.result() for future in futures)

    assert outcomes == ["issued", "rejected"]
    assert runner.calls == 1


def test_invalid_runner_report_is_rejected_before_acceptance_recorder_or_finalize() -> None:
    config = _load_config()
    recorder = _CountingMetricRecorder()
    runner = _InvalidStatusRunner()
    execution = _quality_execution(config, runner=runner)
    sink = _StagedSink()

    with pytest.raises(QualityGateReceiptInvalid):
        LoadGovernanceFinalizationCoordinator(metric_recorder=recorder).load(
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
            quality_execution=execution,
        )

    assert recorder.start_calls == 0
    assert sink.events == ["stage", "abort"]


def test_evidence_projection_is_bounded_and_drops_large_nested_diagnostics() -> None:
    config = _load_config()
    secret = "password=must-not-leak-" + ("x" * 100_000)
    execution = _quality_execution(config, runner=_HostileDiagnosticsRunner(secret))
    execution.select_boundary("pre_commit", load_config=config)
    receipt = _evaluate(execution, config, boundary="pre_commit")

    projection = execution.evidence_projection(receipt, load_config=config)
    serialized = json.dumps(projection)

    assert len(serialized) < 4_096
    assert "must-not-leak" not in serialized
    assert "nested" not in serialized
    assert len(projection["results"][0]["message"]) <= 64


def test_typed_hash_evidence_projection_contains_digests_only() -> None:
    config = _typed_hash_load_config()
    execution = _quality_execution(config)
    execution.select_boundary("pre_commit", load_config=config)
    receipt = execution.evaluate(
        load_config=config,
        boundary="pre_commit",
        source_snapshot=QualityProbeSnapshot(typed_hash="source-secret-hash"),
        target_snapshot=QualityProbeSnapshot(typed_hash="source-secret-hash"),
    )

    projection = execution.evidence_projection(receipt, load_config=config)
    serialized = json.dumps(projection)
    metrics = projection["results"][0]["metrics"]

    assert "source-secret-hash" not in serialized
    assert metrics["source_hash"].startswith("sha256:")
    assert metrics["target_hash"].startswith("sha256:")
    assert len(metrics["source_hash"]) == len("sha256:") + 64


@pytest.mark.parametrize(
    "config_factory",
    [lambda: _load_config(), lambda: _acceptance_only_load_config()],
    ids=["quality-gates", "quality-acceptance"],
)
def test_empty_policy_compatibility_cannot_authorize_non_empty_execution(
    config_factory: Any,
) -> None:
    config = config_factory()
    execution = _quality_execution(config)

    with pytest.raises(QualityGateReceiptInvalid):
        execution.accept_payload(None, load_config=config)


@pytest.mark.parametrize(
    "tamper",
    [
        pytest.param(
            lambda report: replace(report, policy_fingerprint="sha256:" + "f" * 64),
            id="stale-fingerprint",
        ),
        pytest.param(
            lambda report: replace(
                report,
                gate_contract=tuple(reversed(report.gate_contract)),
            ),
            id="reordered-contract",
        ),
        pytest.param(
            lambda report: replace(report, results=tuple(reversed(report.results))),
            id="reordered-results",
        ),
        pytest.param(
            lambda report: replace(report, results=report.results[:-1]),
            id="missing-result",
        ),
        pytest.param(
            lambda report: replace(report, results=report.results + (report.results[-1],)),
            id="duplicate-result",
        ),
        pytest.param(
            lambda report: replace(
                report,
                results=(replace(report.results[0], type="min_rows"), *report.results[1:]),
            ),
            id="type-downgrade",
        ),
        pytest.param(
            lambda report: replace(
                report,
                results=(replace(report.results[0], severity="warning"), *report.results[1:]),
            ),
            id="severity-downgrade",
        ),
    ],
)
def test_bound_receipt_rejects_policy_contract_and_result_coverage_mismatch(tamper) -> None:  # noqa: ANN001
    policy = _policy(
        {"id": "rows", "type": "row_count_reconciliation"},
        {"id": "minimum", "type": "min_rows", "threshold": 1},
    )
    receipt = QualityGateReceipt(report=tamper(_run(policy)))

    with pytest.raises(QualityGateReceiptMismatch) as raised:
        validate_quality_gate_receipt(receipt, policy=policy)

    assert raised.value.code == "DPONE_QUALITY_GATE_RECEIPT_MISMATCH"


@pytest.mark.parametrize(
    "receipt",
    [
        pytest.param(
            QualityGateReceipt(report=replace(_run(QualityGatePolicy()), kind="attacker.report")),
            id="wrong-kind",
        ),
        pytest.param(
            QualityGateReceipt(
                report=replace(
                    _run(_policy({"id": "rows", "type": "min_rows", "threshold": 1})),
                    results=(
                        QualityGateResult(
                            gate_id="rows",
                            type="min_rows",
                            status="successful",
                            severity="error",
                        ),
                    ),
                )
            ),
            id="unknown-status",
        ),
        pytest.param(
            QualityGateReceipt(report=_run(QualityGatePolicy()), boundary="after_commit"),
            id="unknown-boundary",
        ),
        pytest.param(
            QualityGateReceipt(
                report=_run(QualityGatePolicy()),
                scope_summary={**_SCOPE_SUMMARY, "partition_ids": ["secret-partition"]},
            ),
            id="unbounded-scope",
        ),
    ],
)
def test_malformed_receipt_uses_stable_safe_invalid_error(receipt: object) -> None:
    with pytest.raises(QualityGateReceiptInvalid) as raised:
        validate_quality_gate_receipt(receipt, policy=QualityGatePolicy())

    assert raised.value.code == "DPONE_QUALITY_GATE_RECEIPT_INVALID"
    assert "secret-partition" not in str(raised.value)


def test_valid_receipt_preserves_a_strict_copied_scope_summary() -> None:
    policy = _policy({"id": "rows", "type": "row_count_reconciliation"})
    authored_scope = dict(_SCOPE_SUMMARY)
    receipt = QualityGateReceipt(
        report=_run(policy),
        boundary="resume_validation",
        scope_summary=authored_scope,
    )

    validated = validate_quality_gate_receipt(receipt, policy=policy)

    assert validated is not None
    assert validated.scope_summary == _SCOPE_SUMMARY
    assert validated.scope_summary is not authored_scope
    assert "partition_ids" not in validated.scope_summary


def test_final_processor_receipt_read_rejects_missing_non_empty_policy() -> None:
    load_result = LoadResult(inserted_rows=11, updated_rows=0, total_rows=11)

    with pytest.raises(QualityGateReceiptRequired):
        staged_quality_gate_report(load_result, load_config=_load_config())


def test_payload_loader_rejects_unbound_resume_result_before_success_publication() -> None:
    native = _ResumeNativeService()
    identity = _LoadIdentity()
    service = PayloadLoadService(
        source=None,
        sink=_LegacySink(),
        logger=SimpleNamespace(),
        load_identity_service=identity,
        strategy_metadata_enricher=_PassthroughEnricher(),
        native_transfer_runtime_service=native,
        load_governance_service=LoadGovernanceService(),
        legacy_governance_coordinator=_UnboundResumeCoordinator(),
    )

    with pytest.raises(QualityGateReceiptRequired):
        service.load_single_payload(
            _load_config(),
            _payload(),
            _extract_result(),
            _load_record(),
        )

    assert native.publish_calls == 0
    assert identity.events == []


def test_staged_result_persists_the_validated_native_scope_mapping() -> None:
    sink = _StagedSink()
    result = LoadGovernanceFinalizationCoordinator().load(
        sink=sink,
        load_config=_load_config(),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
        quality_scope=_QualityScope(),
    )

    assert sink.events == ["stage", "finalize", "cleanup"]
    assert result.quality_gate_receipt is not None
    assert result.quality_gate_receipt.scope_summary == _SCOPE_SUMMARY
    assert result.reconciliation_metrics["native_transfer_quality_scope"] == _SCOPE_SUMMARY
    assert "partition_ids" not in result.reconciliation_metrics["native_transfer_quality_scope"]


@pytest.mark.parametrize("resume_only", [False, True], ids=["legacy", "resume"])
def test_legacy_and_resume_results_persist_the_validated_native_scope_mapping(
    resume_only: bool,
) -> None:
    sink = _LegacySink()
    coordinator = LegacyLoadGovernanceCoordinator()
    arguments = {
        "source": None,
        "sink": sink,
        "load_config": _load_config(),
        "payload": _payload(),
        "extract_result": _extract_result(),
        "load_record": _load_record(),
        "quality_scope": _QualityScope(),
    }
    if resume_only:
        result = coordinator.validate_resume_only(
            **arguments,
            load_result=LoadResult(inserted_rows=0, updated_rows=0, total_rows=0),
        )
    else:
        result = coordinator.load(**arguments)

    assert sink.load_calls == (0 if resume_only else 1)
    assert result.quality_gate_receipt is not None
    assert result.quality_gate_receipt.scope_summary == _SCOPE_SUMMARY
    assert result.reconciliation_metrics["native_transfer_quality_scope"] == _SCOPE_SUMMARY


def test_staged_policy_drift_is_rejected_before_finalization() -> None:
    sink = _StagedSink()
    config = _load_config()
    coordinator = LoadGovernanceFinalizationCoordinator(metric_recorder=_PolicyDriftingMetricRecorder(config))

    with pytest.raises(QualityGateReceiptMismatch):
        coordinator.load(
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.events == ["stage", "abort"]


def test_legacy_policy_drift_is_rejected_before_target_mutation() -> None:
    sink = _LegacySink()
    config = _load_config()
    coordinator = LegacyLoadGovernanceCoordinator(metric_recorder=_PolicyDriftingMetricRecorder(config))

    with pytest.raises(QualityGateReceiptMismatch):
        coordinator.load(
            source=None,
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.load_calls == 0


def test_post_commit_receipt_mismatch_preserves_real_mutation_outcome() -> None:
    config = _load_config()
    sink = _PolicyDriftingLegacySink(config)

    with pytest.raises(QualityGateReceiptMismatch) as raised:
        LegacyLoadGovernanceCoordinator().load(
            source=None,
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.load_calls == 1
    assert isinstance(raised.value.outcome, QualityGateFailureOutcome)
    assert raised.value.outcome.failure_boundary == "post_commit"
    assert raised.value.outcome.target_state == "mutation_returned_success"
    assert raised.value.outcome.inserted_rows == 11
    result: dict[str, object] = {}
    enrich_post_commit_quality_failure_result(result, raised.value)
    assert result["inserted_rows"] == 11
    assert result["failure_context"] == raised.value.outcome.failure_context()


def test_staged_post_target_drift_blocks_native_checkpoint_callback() -> None:
    config = _load_config()
    sink = _PolicyDriftingStagedSink(config)
    execution = _quality_execution(config)
    committed: list[LoadResult] = []

    def on_target_committed(load_result: LoadResult) -> LoadResult:
        committed.append(load_result)
        return load_result

    with pytest.raises(QualityGateReceiptMismatch):
        LoadGovernanceFinalizationCoordinator().load(
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
            on_target_committed=on_target_committed,
            quality_execution=execution,
        )

    assert sink.events == ["stage", "finalize"]
    assert committed == []


def test_legacy_post_target_drift_blocks_native_checkpoint_callback() -> None:
    config = _load_config()
    sink = _PolicyDriftingLegacySink(config)
    execution = _quality_execution(config)
    committed: list[LoadResult] = []

    def on_target_committed(load_result: LoadResult) -> LoadResult:
        committed.append(load_result)
        return load_result

    with pytest.raises(QualityGateReceiptMismatch) as raised:
        LegacyLoadGovernanceCoordinator().load(
            source=None,
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
            on_target_committed=on_target_committed,
            quality_scope=_QualityScope(),
            quality_execution=execution,
        )

    assert sink.load_calls == 1
    assert committed == []
    assert raised.value.outcome is not None
    assert raised.value.outcome.checkpoint_state == "unknown"


def test_resume_receipt_mismatch_retains_resume_outcome_without_sink_mutation() -> None:
    config = _load_config()
    sink = _LegacySink()
    coordinator = LegacyLoadGovernanceCoordinator(metric_recorder=_PolicyDriftingMetricRecorder(config))

    with pytest.raises(QualityGateReceiptMismatch) as raised:
        coordinator.validate_resume_only(
            source=None,
            sink=sink,
            load_config=config,
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
            load_result=LoadResult(inserted_rows=0, updated_rows=0, total_rows=0),
            quality_scope=_QualityScope(),
        )

    assert sink.load_calls == 0
    assert isinstance(raised.value.outcome, QualityGateFailureOutcome)
    assert raised.value.outcome.failure_boundary == "resume_validation"
    assert raised.value.outcome.target_state == "not_mutated"
    assert raised.value.outcome.checkpoint_state == "committed"


def _load_config() -> SimpleNamespace:
    return SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "lineage": False,
            "quality": {
                "gates": [
                    {
                        "id": "rows",
                        "type": "row_count_reconciliation",
                        "severity": "error",
                    }
                ]
            },
        },
    )


def _typed_hash_load_config() -> SimpleNamespace:
    config = _load_config()
    config.options["quality"]["gates"] = [
        {
            "id": "hashes",
            "type": "typed_hash_reconciliation",
            "severity": "error",
        }
    ]
    return config


def _acceptance_only_load_config() -> SimpleNamespace:
    config = _load_config()
    config.options["quality"] = {
        "acceptance": {
            "enabled": True,
            "mode": "required",
            "capture": {"source": True, "staged": False, "target": True},
            "checks": {"row_count": True},
        }
    }
    return config


def _quality_execution(
    config: SimpleNamespace,
    *,
    run_id: str = "run-1",
    runner: object | None = None,
) -> QualityGateExecution:
    return QualityGateExecution(
        QualityExecutionSnapshot.from_load_config(config),
        run_id=run_id,
        load_id="load-1",
        runner=runner,
    )


def _evaluate(
    execution: QualityGateExecution,
    config: SimpleNamespace,
    *,
    boundary: str,
) -> QualityGateReceipt:
    return execution.evaluate(
        load_config=config,
        boundary=boundary,
        source_snapshot=QualityProbeSnapshot(row_count=11, typed_hash="same"),
        target_snapshot=QualityProbeSnapshot(row_count=11, typed_hash="same"),
    )


def _remove_quality_policy(config: SimpleNamespace) -> None:
    config.options["quality"]["gates"] = []


def _payload() -> LoadPayload:
    return LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": index} for index in range(11)]),
        schema=[("id", "bigint")],
    )


def _extract_result() -> ExtractResult:
    return ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": index} for index in range(11)]),
        schema=[("id", "bigint")],
    )


def _load_record() -> SimpleNamespace:
    return SimpleNamespace(run_id="run-1", load_id="load-1")


class _ScopeSummary:
    def to_jsonable(self) -> dict[str, object]:
        return dict(_SCOPE_SUMMARY)


class _ScopeSnapshots:
    source = QualityProbeSnapshot(row_count=11)
    target = QualityProbeSnapshot(row_count=11)
    scope_summary = _ScopeSummary()


class _QualityScope:
    def projected_snapshots(self, staged_rows: object) -> _ScopeSnapshots:
        assert staged_rows == 11
        return _ScopeSnapshots()

    def committed_snapshots(self) -> _ScopeSnapshots:
        return _ScopeSnapshots()

    def resume_snapshots(self) -> _ScopeSnapshots:
        return _ScopeSnapshots()


class _StagedSink:
    def __init__(self) -> None:
        self.events: list[str] = []

    def stage_payload(self, load_config: Any, payload: LoadPayload) -> StagedLoadHandle:
        self.events.append("stage")
        return StagedLoadHandle(
            staging_config=load_config,
            payload_schema=payload.schema,
            staged_rows=11,
        )

    def finalize_staged_load(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        del load_config, handle
        self.events.append("finalize")
        return LoadResult(inserted_rows=11, updated_rows=0, total_rows=11, staging_rows=11)

    def abort_staged_load(self, handle: StagedLoadHandle) -> None:
        del handle
        self.events.append("abort")

    def cleanup_staged_load(self, handle: StagedLoadHandle) -> None:
        del handle
        self.events.append("cleanup")


class _LegacySink:
    def __init__(self) -> None:
        self.load_calls = 0

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        del load_config, payload
        self.load_calls += 1
        return LoadResult(inserted_rows=11, updated_rows=0, total_rows=11, staging_rows=11)


class _PolicyDriftingStagedSink(_StagedSink):
    def __init__(self, load_config: SimpleNamespace) -> None:
        super().__init__()
        self._load_config = load_config

    def finalize_staged_load(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        result = super().finalize_staged_load(load_config, handle)
        self._load_config.options["quality"]["gates"][0]["severity"] = "warning"
        return result


class _PolicyDriftingLegacySink(_LegacySink):
    def __init__(self, load_config: SimpleNamespace) -> None:
        super().__init__()
        self._load_config = load_config

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        result = super().load(load_config, payload)
        self._load_config.options["quality"]["gates"][0]["severity"] = "warning"
        return result


class _NoopAcceptanceRun:
    def metrics(self, load_result: LoadResult) -> dict[str, object]:
        del load_result
        return {}


class _PolicyDriftingMetricRecorder:
    def __init__(self, load_config: SimpleNamespace) -> None:
        self._load_config = load_config
        self._drifted = False

    def start(self, **kwargs: object) -> _NoopAcceptanceRun:
        del kwargs
        return _NoopAcceptanceRun()

    def capture(self, run: object, **kwargs: object) -> None:
        del run, kwargs
        if self._drifted:
            return
        self._drifted = True
        self._load_config.options["quality"]["gates"][0]["severity"] = "warning"


class _CountingMetricRecorder:
    def __init__(self) -> None:
        self.start_calls = 0

    def start(self, **kwargs: object) -> _NoopAcceptanceRun:
        del kwargs
        self.start_calls += 1
        return _NoopAcceptanceRun()

    def capture(self, run: object, **kwargs: object) -> None:
        del run, kwargs


class _InvalidStatusRunner:
    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ):
        report = QualityGateRunner().run(policy, source=source, target=target)
        return replace(
            report,
            results=(replace(report.results[0], status="successful"),),
        )


class _FailingRunner:
    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ):
        del target
        return QualityGateRunner().run(
            policy,
            source=source,
            target=QualityProbeSnapshot(row_count=0, typed_hash="different"),
        )


class _ThrowingRunner:
    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ):
        del policy, source, target
        raise RuntimeError("quality runner failed")


class _CountingRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ):
        self.calls += 1
        return QualityGateRunner().run(policy, source=source, target=target)


class _HostileDiagnosticsRunner:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ):
        report = QualityGateRunner().run(policy, source=source, target=target)
        return replace(
            report,
            results=(
                replace(
                    report.results[0],
                    metrics={
                        "source_row_count": 11,
                        "target_row_count": 11,
                        "difference": 0,
                        "allowed_difference": 0,
                        "nested": {"secret": self._secret},
                        "oversized": self._secret,
                    },
                    message=self._secret,
                ),
            ),
        )


class _ResumeContext:
    payload = _payload()
    should_skip_load = True

    def skipped_load_result(self) -> LoadResult:
        return LoadResult(inserted_rows=0, updated_rows=0, total_rows=0)


class _ResumeNativeService:
    def __init__(self) -> None:
        self.publish_calls = 0

    def prepare_before_load(self, **kwargs: object) -> _ResumeContext:
        del kwargs
        return _ResumeContext()

    def publish_success_report(self, context: object, load_result: LoadResult) -> LoadResult:
        del context
        self.publish_calls += 1
        return load_result


class _UnboundResumeCoordinator:
    def validate_resume_only(self, **kwargs: object) -> LoadResult:
        return kwargs["load_result"]  # type: ignore[return-value]


class _LoadIdentity:
    def __init__(self) -> None:
        self.events: list[str] = []

    def mark_staged(self, load_record: object, *, extracted_rows: object) -> None:
        del load_record, extracted_rows
        self.events.append("staged")

    def mark_committed(self, load_record: object, load_result: object) -> None:
        del load_record, load_result
        self.events.append("committed")


class _PassthroughEnricher:
    def enrich_payload(self, payload: LoadPayload, *, load_config: object) -> LoadPayload:
        del load_config
        return payload
