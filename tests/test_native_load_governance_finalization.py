from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.governance.hooks import InMemoryLoadStepAuditStorage, LoadStepAuditRecord
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricSnapshot
from dpone.runtime.governance.audit_tap import RuntimeLoadStepAuditCollector
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.legacy_acceptance import LegacyLoadGovernanceCoordinator
from dpone.runtime.governance.ports import (
    LineageProjectionResult,
    StagedLoadHandle,
    finalize_staged_load_after_validation,
    validate_staged_load_if_supported,
)
from dpone.runtime.governance.service import LoadGovernanceService, QualityGateFailure, QualityGateReceipt
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.clickhouse_lineage_projection import ClickHouseSinkSideLineageProjector
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.clickhouse_staging_finalizer import (
    CLICKHOUSE_STAGING_UNIQUE_KEY_NULL,
    ClickHouseStagingKeyError,
)
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sources.base import ExtractResult
from dpone.security_redaction import REDACTION_TOKEN

_MISSING = object()


def test_pre_finalize_quality_failure_aborts_staging_before_target_mutation() -> None:
    events: list[str] = []
    load_config = _load_config(
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "quality": {"gates": [_strict_row_count_gate()]},
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": 1}, {"id": 2}]),
        schema=[("id", "bigint")],
    )
    sink = _StagedSink(events, staged_rows=1)

    with pytest.raises(RuntimeError, match="quality gates failed"):
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService()).load(
            sink=sink,
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert events == ["stage", "project", "abort"]


def test_pre_finalize_quality_failure_records_structured_gate_evidence() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    governance = LoadGovernanceService(audit_storage=audit)
    load_config = _load_config(
        options={
            "quality": {"gates": [_strict_row_count_gate()]},
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": 1}, {"id": 2}]),
        schema=[("id", "bigint")],
    )

    with pytest.raises(RuntimeError, match="quality gates failed"):
        LoadGovernanceFinalizationCoordinator(governance).load(
            sink=_StagedSink(events, staged_rows=1),
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=_load_record(),
            projector=_Projector(events),
        )

    quality_step = next(record for record in audit.records if record.step_id == "quality_checked")
    assert quality_step.status == "failed"
    assert quality_step.details["passed"] is False
    assert quality_step.details["results"][0]["gate_id"] == "row_count_reconciliation"
    assert quality_step.details["results"][0]["metrics"] == {
        "source_row_count": 2,
        "target_row_count": 1,
        "difference": 1,
        "allowed_difference": 0.0,
    }
    assert "password" not in str(quality_step.details).lower()


def test_projected_key_failure_records_cleanup_recovery_before_target_guard() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    sink = _KeyValidationFailureSink(events)

    with pytest.raises(ClickHouseStagingKeyError) as raised:
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            sink=sink,
            load_config=_load_config(load_strategy=LoadStrategy.INCREMENTAL_MERGE, unique_key=["id"]),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_ProjectedTableProjector(events),
            before_target_mutation=lambda: events.append("target_guard"),
        )

    assert events == ["stage", "project", "validate:orders__projected", "abort"]
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.details == {
        "failure_boundary": "pre_commit",
        "target_outcome": "failed_before_target",
        "error_code": CLICKHOUSE_STAGING_UNIQUE_KEY_NULL,
        "operation_tables": {
            "staging": "landing.orders__raw",
            "projected": "landing.orders__projected",
        },
        "cleanup_attempted": True,
        "cleanup_status": "succeeded",
        "cleanup_verification_required": True,
    }
    assert raised.value.details == failure.details


def test_governed_validation_runs_once_before_guard_and_validated_finalize() -> None:
    events: list[str] = []
    sink = _ValidatedFinalizationSink(events)

    result = LoadGovernanceFinalizationCoordinator().load(
        sink=sink,
        load_config=_load_config(load_strategy=LoadStrategy.INCREMENTAL_MERGE, unique_key=["id"]),
        payload=LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        extract_result=ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        load_record=_load_record(),
        projector=_ProjectedTableProjector(events),
        before_target_mutation=lambda: events.append("target_guard"),
    )

    assert result.inserted_rows == 1
    assert events == [
        "stage",
        "project",
        "validate:orders__projected",
        "target_guard",
        "finalize_validated",
        "cleanup",
    ]


def test_target_invocation_failure_retains_staging_and_records_commit_unknown() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    sink = _TargetMutationFailureSink(events)

    with pytest.raises(RuntimeError, match="target mutation failed") as raised:
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            sink=sink,
            load_config=_load_config(load_strategy=LoadStrategy.INCREMENTAL_MERGE, unique_key=["id"]),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_ProjectedTableProjector(events),
            before_target_mutation=lambda: events.append("target_guard"),
        )

    assert events == [
        "stage",
        "project",
        "validate:orders__projected",
        "target_guard",
        "target_mutation",
    ]
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.details == {
        "failure_boundary": "commit_unknown",
        "target_outcome": "commit_unknown",
        "error_code": "RuntimeError",
        "operation_tables": {
            "staging": "landing.orders__raw",
            "projected": "landing.orders__projected",
        },
        "cleanup_attempted": False,
        "cleanup_status": "retained_for_reconciliation",
        "cleanup_verification_required": True,
        "safe_to_retry": False,
    }
    assert raised.value.details == failure.details


def test_staged_validation_receipt_cannot_cross_sink_or_load_config() -> None:
    events: list[str] = []
    sink = _ValidatedFinalizationSink(events)
    load_config = _load_config()
    handle = replace(
        sink.stage_payload(
            load_config,
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
        ),
        finalization_config=load_config,
    )
    receipt = validate_staged_load_if_supported(sink, load_config, handle)

    with pytest.raises(ValueError, match="staged_load_validation_receipt_invalid"):
        finalize_staged_load_after_validation(
            _ValidatedFinalizationSink([]),
            load_config,
            receipt,
        )
    with pytest.raises(ValueError, match="staged_load_validation_receipt_invalid"):
        finalize_staged_load_after_validation(
            sink,
            replace(load_config, target_table="other"),
            receipt,
        )

    assert events == ["stage", "validate:orders"]


def test_staged_validation_receipt_finalizes_frozen_validated_semantics() -> None:
    events: list[str] = []
    sink = _ValidatedFinalizationSink(events)
    load_config = _load_config(load_strategy=LoadStrategy.INCREMENTAL_MERGE, unique_key=["id"])
    projected_config = replace(load_config, target_table="orders__projected")
    handle = replace(
        sink.stage_payload(
            load_config,
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
        ),
        finalization_config=projected_config,
    )
    receipt = validate_staged_load_if_supported(sink, load_config, handle)

    load_config.target_table = "other_target"
    load_config.unique_key = ["other_key"]
    projected_config.target_table = "other_projected"
    finalize_staged_load_after_validation(sink, load_config, receipt)

    assert events == ["stage", "validate:orders__projected", "finalize_validated"]
    assert sink.finalized_semantics == ("orders", ("id",), "orders__projected")


def test_staged_validation_receipt_preserves_runtime_connector_identity() -> None:
    class UncopyableRuntimeConnector:
        def __deepcopy__(self, memo):  # noqa: ANN001
            del memo
            raise TypeError("runtime connector cannot be copied")

    events: list[str] = []
    sink = _ValidatedFinalizationSink(events)
    source_connector = UncopyableRuntimeConnector()
    load_config = _load_config(options={"_source_connector": source_connector})
    handle = replace(
        sink.stage_payload(
            load_config,
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
        ),
        finalization_config=load_config,
    )

    receipt = validate_staged_load_if_supported(sink, load_config, handle)
    _token, validated_config, validated_handle = receipt.frozen_inputs(
        sink=sink,
        load_config=load_config,
    )

    assert validated_config.options["_source_connector"] is source_connector
    assert validated_handle.finalization_config.options["_source_connector"] is source_connector


@pytest.mark.parametrize(
    "staged_rows",
    [
        pytest.param(_MISSING, id="missing"),
        pytest.param(None, id="none"),
        pytest.param("0", id="non-integer"),
        pytest.param(True, id="boolean"),
        pytest.param(-1, id="negative"),
    ],
)
def test_pre_finalize_rejects_invalid_staged_rows_instead_of_coercing_to_zero(staged_rows: object) -> None:
    events: list[str] = []

    with pytest.raises(ValueError, match="staged_rows must be a non-negative integer"):
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService()).load(
            sink=_InvalidRowsStagedSink(events, staged_rows=staged_rows),
            load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert events == ["stage", "abort"]


def test_quality_failure_preserves_primary_when_abort_and_failure_evidence_also_fail() -> None:
    events: list[str] = []
    audit = _FaultingAuditStorage(
        events,
        fail_steps={"quality_checked", "load_governance_failed"},
    )
    sink = _FaultingStagedSink(
        events,
        staged_rows=1,
        abort_error=RuntimeError("abort failed"),
    )

    with pytest.raises(QualityGateFailure) as raised:
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            sink=sink,
            load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}, {"id": 2}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert str(raised.value) == "quality gates failed"
    assert events == [
        "stage",
        "evidence:staged",
        "project",
        "evidence:lineage_projected",
        "abort",
        "evidence:quality_checked",
        "evidence:load_governance_failed",
    ]


def test_post_commit_target_metric_failure_never_aborts_finalized_target() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    load_config = _load_config(
        options={
            "quality": {
                "gates": [_strict_row_count_gate()],
                "acceptance": {
                    "enabled": True,
                    "mode": "required",
                    "checks": {"row_count": True},
                },
            },
        }
    )
    sink = _PostCommitMetricFailureSink(events, staged_rows=1)

    with pytest.raises(RuntimeError, match="target metric evidence failed"):
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            source=_MetricSource(),
            sink=sink,
            load_config=load_config,
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert "finalize" in events
    assert "abort" not in events
    assert events[-1] == "cleanup"
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.error_message == "target_acceptance_metric_probe_failed"
    assert failure.details == {
        "failure_boundary": "post_commit",
        "acceptance_side": "target",
        "error_code": "target_acceptance_metric_probe_failed",
    }


def test_post_commit_finalized_evidence_failure_never_aborts_finalized_target() -> None:
    events: list[str] = []
    audit = _FaultingAuditStorage(events, fail_steps={"finalized"})
    sink = _FaultingStagedSink(events, staged_rows=1)

    with pytest.raises(RuntimeError, match="finalized evidence failed"):
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            sink=sink,
            load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert "finalize" in events
    assert "abort" not in events
    assert events[-2:] == ["evidence:load_governance_failed", "cleanup"]
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.error_message == "finalized_evidence_failed"
    assert failure.details == {
        "failure_boundary": "post_commit",
        "error_code": "finalized_evidence_failed",
    }
    assert "finalized evidence failed" not in str(failure)


def test_post_commit_cleanup_failure_never_aborts_finalized_target() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()

    with pytest.raises(RuntimeError, match="staged cleanup failed after confirmed target commit") as raised:
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            sink=_CleanupFailureSink(events, staged_rows=1),
            load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert events == ["stage", "project", "finalize", "cleanup"]
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.error_message == "staged_cleanup_failed"
    assert failure.details == {
        "failure_boundary": "post_commit",
        "target_outcome": "committed",
        "error_code": "staged_cleanup_failed",
        "cleanup_error_type": "RuntimeError",
        "operation_tables": {},
        "cleanup_attempted": True,
        "cleanup_status": "failed",
        "cleanup_verification_required": True,
        "safe_to_retry": False,
    }
    assert raised.value.details == failure.details


def test_post_commit_acceptance_failure_survives_cleanup_and_failure_evidence_errors() -> None:
    events: list[str] = []
    audit = _FaultingAuditStorage(events, fail_steps={"load_governance_failed"})
    sink = _PostCommitMetricAndCleanupFailureSink(events, staged_rows=1)

    with pytest.raises(RuntimeError, match="target metric evidence failed") as raised:
        LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
            source=_MetricSource(),
            sink=sink,
            load_config=_load_config(
                options={
                    "quality": {
                        "gates": [_strict_row_count_gate()],
                        "acceptance": {
                            "enabled": True,
                            "mode": "required",
                            "checks": {"row_count": True},
                        },
                    }
                }
            ),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert str(raised.value) == "target metric evidence failed"
    assert "finalize" in events
    assert "abort" not in events
    assert "cleanup" in events
    assert events.count("evidence:load_governance_failed") == 2


def test_pre_finalize_success_projects_lineage_then_finalizes() -> None:
    events: list[str] = []
    load_config = _load_config(
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "quality": {"gates": [_strict_row_count_gate()]},
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    sink = _StagedSink(events, staged_rows=1)

    result = LoadGovernanceFinalizationCoordinator(LoadGovernanceService()).load(
        sink=sink,
        load_config=load_config,
        payload=payload,
        extract_result=extract_result,
        load_record=_load_record(),
        projector=_Projector(events),
    )

    assert events == ["stage", "project", "finalize"]
    assert result.inserted_rows == 1
    assert result.reconciliation_metrics is not None
    assert result.reconciliation_metrics["governance_finalization"] == "pre_finalize"
    assert result.reconciliation_metrics["lineage_projection"]["projected"] is True
    assert result.reconciliation_metrics["quality_gates"]["passed"] is True
    assert result.quality_gate_receipt is not None
    assert isinstance(result.quality_gate_receipt, QualityGateReceipt)
    assert result.quality_gate_receipt.boundary == "pre_commit"
    assert result.quality_gate_receipt.report.passed is True


def test_staged_target_guard_is_armed_immediately_before_finalization() -> None:
    events: list[str] = []
    sink = _StagedSink(events, staged_rows=1)

    result = LoadGovernanceFinalizationCoordinator(LoadGovernanceService()).load(
        sink=sink,
        load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
        payload=LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        extract_result=ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        load_record=_load_record(),
        projector=_Projector(events),
        before_target_mutation=lambda: events.append("guard"),
        on_target_committed=lambda load_result: events.append("checkpoint") or load_result,
    )

    assert result.total_rows == 1
    assert events == ["stage", "project", "guard", "finalize", "checkpoint"]


def test_legacy_coordinator_attaches_post_commit_quality_receipt() -> None:
    load_result = LoadResult(
        inserted_rows=4,
        updated_rows=1,
        total_rows=5,
        staging_rows=5,
    )
    sink = _LegacyQualitySink(load_result)

    result = LegacyLoadGovernanceCoordinator(LoadGovernanceService()).load(
        source=None,
        sink=sink,
        load_config=_load_config(options={"quality": {"gates": [_strict_row_count_gate()]}}),
        payload=LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": index} for index in range(5)]),
            schema=[("id", "bigint")],
        ),
        extract_result=ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": index} for index in range(5)]),
            schema=[("id", "bigint")],
        ),
        load_record=_load_record(),
    )

    assert sink.load_calls == 1
    assert isinstance(result.quality_gate_receipt, QualityGateReceipt)
    assert result.quality_gate_receipt.boundary == "post_commit"
    assert result.quality_gate_receipt.report.passed is True
    assert result.reconciliation_metrics["quality_gates"]["passed"] is True


def test_legacy_target_guard_is_armed_immediately_before_load() -> None:
    events: list[str] = []
    load_result = LoadResult(
        inserted_rows=1,
        updated_rows=0,
        total_rows=1,
        staging_rows=1,
    )
    sink = _OrderedLegacySink(events, load_result)

    LegacyLoadGovernanceCoordinator(LoadGovernanceService()).load(
        source=None,
        sink=sink,
        load_config=_load_config(options={}),
        payload=LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        extract_result=ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        ),
        load_record=_load_record(),
        before_target_mutation=lambda: events.append("guard"),
        on_target_committed=lambda result: events.append("checkpoint") or result,
    )

    assert events == ["guard", "load", "checkpoint"]


def test_legacy_post_commit_quality_failure_preserves_actual_safe_outcome() -> None:
    load_result = LoadResult(
        inserted_rows=7,
        updated_rows=2,
        total_rows=11,
        staging_rows=9,
    )
    sink = _LegacyQualitySink(load_result)
    load_config = _load_config(
        options={
            "quality": {
                "gates": [
                    {
                        "id": "target_minimum",
                        "type": "min_rows",
                        "side": "target",
                        "threshold": 12,
                        "severity": "error",
                    }
                ]
            }
        }
    )

    with pytest.raises(QualityGateFailure) as raised:
        LegacyLoadGovernanceCoordinator(LoadGovernanceService()).load(
            source=None,
            sink=sink,
            load_config=load_config,
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": index} for index in range(9)]),
                schema=[("id", "bigint")],
            ),
            extract_result=ExtractResult(
                artifact=InMemoryRowsArtifact([{"id": index} for index in range(9)]),
                schema=[("id", "bigint")],
            ),
            load_record=_load_record(),
        )

    assert sink.load_calls == 1
    outcome = raised.value.outcome
    assert outcome is not None
    assert outcome.failure_boundary == "post_commit"
    assert outcome.target_state == "mutation_returned_success"
    assert outcome.checkpoint_state == "not_applicable"
    assert outcome.source_state == "not_advanced"
    assert outcome.retry_classification == "retry_may_repeat_target_mutation"
    assert outcome.inserted_rows == 7
    assert outcome.updated_rows == 2
    assert outcome.final_rows == 11
    assert outcome.extracted_rows == 9
    assert outcome.attempts == 1


def test_pre_finalize_records_runtime_load_steps() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    governance = LoadGovernanceService(audit_storage=audit)
    load_config = _load_config(
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "quality": {"gates": [_strict_row_count_gate()]},
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    LoadGovernanceFinalizationCoordinator(governance).load(
        sink=_StagedSink(events, staged_rows=1),
        load_config=load_config,
        payload=payload,
        extract_result=extract_result,
        load_record=_load_record(),
        projector=_Projector(events),
    )

    succeeded_steps = [record.step_id for record in audit.records if record.status == "succeeded"]
    assert succeeded_steps == ["staged", "lineage_projected", "quality_checked", "finalized"]
    details_by_step = {record.step_id: record.details for record in audit.records}
    assert details_by_step["staged"]["staged_rows"] == 1
    assert details_by_step["staged"]["schema_columns"] == 1
    assert details_by_step["lineage_projected"]["projected"] is True
    assert details_by_step["quality_checked"]["passed"] is True
    assert details_by_step["finalized"]["total_rows"] == 1


@pytest.mark.parametrize("status", ["succeeded", "warning"])
def test_public_quality_audit_output_uses_shared_redaction_boundary(status: str) -> None:
    collector = RuntimeLoadStepAuditCollector()
    now = datetime.now(UTC)
    collector.record_step(
        LoadStepAuditRecord(
            run_id="run_1",
            load_id="load_1",
            step_id="quality_checked",
            phase="load_governance",
            kind="data_quality_evidence",
            status=status,
            started_at=now,
            finished_at=now,
            error_message="token=audit-token at /Users/operator/private/runtime.log",
            details={
                "passed": True,
                "results": [
                    {
                        "metrics": {"password": "audit-password"},
                        "message": "token=result-token at /Users/operator/private/report.json",
                    }
                ],
            },
        )
    )

    payload = collector.to_jsonable()
    rendered = str(payload)

    assert REDACTION_TOKEN in rendered
    assert "audit-token" not in rendered
    assert "audit-password" not in rendered
    assert "result-token" not in rendered
    assert "/Users/operator" not in rendered


def test_pre_cleanup_acceptance_metrics_are_recorded_for_source_staged_and_target() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    governance = LoadGovernanceService(audit_storage=audit)
    load_config = _load_config(
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "quality": {
                "gates": [_strict_row_count_gate()],
                "acceptance": {
                    "enabled": True,
                    "checks": {
                        "row_count": True,
                        "null_counts": "all_columns",
                        "distinct_counts": "all_columns",
                    },
                },
            },
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    source = _MetricSource()
    sink = _MetricSink(events, staged_rows=1)

    result = LoadGovernanceFinalizationCoordinator(governance).load(
        source=source,
        sink=sink,
        load_config=load_config,
        payload=payload,
        extract_result=extract_result,
        load_record=_load_record(),
        projector=_Projector(events),
    )

    assert events == ["stage", "project", "finalize", "cleanup"]
    metric_steps = [record for record in audit.records if record.kind == "data_quality_evidence"]
    assert [record.step_id for record in metric_steps] == [
        "source_metrics_captured",
        "staged_metrics_captured",
        "target_metrics_captured",
    ]
    assert metric_steps[0].details["side"] == "source"
    assert metric_steps[0].details["row_count"] == 1
    assert metric_steps[0].details["null_counts"] == {"id": 0}
    assert metric_steps[0].details["distinct_counts"] == {"id": 1}
    assert metric_steps[0].details["captured_before_cleanup"] is True
    assert source.metric_probe.requests[0].side == "source"
    assert sink.metric_probe.requests[0].side == "staged"
    assert sink.metric_probe.requests[1].side == "target"
    assert sorted(result.reconciliation_metrics["acceptance_metrics"]["snapshots"]) == [
        "source",
        "staged",
        "target",
    ]


def test_required_acceptance_metrics_fail_closed_when_probe_is_missing() -> None:
    events: list[str] = []
    audit = InMemoryLoadStepAuditStorage()
    governance = LoadGovernanceService(audit_storage=audit)
    load_config = _load_config(
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "quality": {
                "gates": [_strict_row_count_gate()],
                "acceptance": {
                    "enabled": True,
                    "mode": "required",
                    "checks": {"row_count": True},
                },
            },
        }
    )
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
    extract_result = ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    with pytest.raises(RuntimeError, match="source_acceptance_metric_probe_unavailable"):
        LoadGovernanceFinalizationCoordinator(governance).load(
            source=object(),
            sink=_StagedSink(events, staged_rows=1),
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=_load_record(),
            projector=_Projector(events),
        )

    assert events == ["stage", "project", "abort"]
    failure = next(record for record in audit.records if record.step_id == "load_governance_failed")
    assert failure.error_message == "source_acceptance_metric_probe_unavailable"
    assert failure.details == {
        "failure_boundary": "pre_commit",
        "acceptance_side": "source",
        "error_code": "source_acceptance_metric_probe_unavailable",
    }


def test_clickhouse_sink_side_projection_adds_core_lineage_without_unstable_row_id() -> None:
    connector = _ClickHouseConnector()
    sink = _ClickHouseProjectionSink(connector)
    load_config = _load_config(options={"lineage": {"enabled": True, "preset": "standard"}})
    staging_config = replace(load_config, target_table="orders__dpone_staging_abcd")
    handle = StagedLoadHandle(
        staging_config=staging_config,
        payload_schema=(("id", "Int64"), ("name", "String")),
        staged_rows=2,
    )

    result = ClickHouseSinkSideLineageProjector(sink).project(
        load_config=load_config,
        handle=handle,
        lineage_options=LineageOptions.from_config(load_config.options["lineage"]),
        load_record=_load_record(),
    )

    assert result.projected is True
    assert result.warnings == ("row_identity_unique_key_missing",)
    assert ("__dpone__run_id", "String") in result.handle.payload_schema
    assert ("__dpone__load_id", "String") in result.handle.payload_schema
    assert ("__dpone__loaded_at", "DateTime64(6, 'UTC')") in result.handle.payload_schema
    assert ("__dpone__extracted_at", "DateTime64(6, 'UTC')") in result.handle.payload_schema
    assert "__dpone__row_id" not in {column for column, _ in result.handle.payload_schema}
    insert_sql = "\n".join(connector.queries)
    assert "SELECT *" not in insert_sql
    assert "`id`, `name`, `__dpone__run_id`, `__dpone__load_id`" in insert_sql
    assert "'run_1'" in insert_sql
    assert "'load_1'" in insert_sql
    assert sink.target_lookups == 0


def test_clickhouse_sink_side_projection_uses_unique_key_for_row_id() -> None:
    connector = _ClickHouseConnector()
    sink = _ClickHouseProjectionSink(connector)
    load_config = _load_config(
        unique_key="id",
        options={"lineage": {"enabled": True, "preset": "standard"}},
    )
    staging_config = replace(load_config, target_table="orders__dpone_staging_abcd")
    handle = StagedLoadHandle(
        staging_config=staging_config,
        payload_schema=(("id", "Int64"), ("name", "String")),
        staged_rows=2,
    )

    result = ClickHouseSinkSideLineageProjector(sink).project(
        load_config=load_config,
        handle=handle,
        lineage_options=LineageOptions.from_config(load_config.options["lineage"]),
        load_record=_load_record(),
    )

    assert result.warnings == ()
    assert ("__dpone__row_id", "String") in result.handle.payload_schema
    joined = "\n".join(connector.queries)
    assert "hex(SHA256" in joined
    assert "'source|dbo|orders'" in joined
    assert "toString(`id`)" in joined


def test_clickhouse_sink_side_projection_skips_existing_row_lineage_columns() -> None:
    connector = _ClickHouseConnector()
    sink = _ClickHouseProjectionSink(connector)
    load_config = _load_config(options={"lineage": {"enabled": True, "preset": "standard"}})
    staging_config = replace(load_config, target_table="orders__dpone_staging_abcd")
    handle = StagedLoadHandle(
        staging_config=staging_config,
        payload_schema=(
            ("id", "Int64"),
            ("__dpone__load_id", "String"),
            ("__dpone__loaded_at", "DateTime64(6, 'UTC')"),
            ("__dpone__row_id", "String"),
            ("__dpone__extracted_at", "DateTime64(6, 'UTC')"),
        ),
        staged_rows=1,
    )

    result = ClickHouseSinkSideLineageProjector(sink).project(
        load_config=load_config,
        handle=handle,
        lineage_options=LineageOptions.from_config(load_config.options["lineage"]),
        load_record=_load_record(),
    )

    columns = [column for column, _ in result.handle.payload_schema]
    assert columns.count("__dpone__load_id") == 1
    assert columns.count("__dpone__row_id") == 1
    assert columns.count("__dpone__run_id") == 1
    assert result.warnings == ()


def test_clickhouse_sink_stages_full_refresh_before_target_rename(tmp_path) -> None:
    data_file = tmp_path / "orders.tsv"
    data_file.write_text("1\tAda\n", encoding="utf-8")
    connector = _ClickHouseLoadConnector()
    sink = ClickHouseSink(connector)
    load_config = _load_config(options={"lineage": False})
    payload = LoadPayload(
        artifact=FileExportArtifact(str(data_file), ["id", "name"], format="mssql-delimited", estimated_rows=1),
        schema=[("id", "bigint"), ("name", "nvarchar(100)")],
    )

    handle = sink.stage_payload(load_config, payload)

    assert handle.staged_rows == 1
    assert handle.finalization_config is not None
    assert not any("RENAME TABLE" in query for query in connector.queries)

    result = sink.finalize_staged_load(load_config, handle)
    sink.cleanup_staged_load(handle)

    assert result.inserted_rows == 1
    assert any("RENAME TABLE" in query for query in connector.queries)
    assert any("DROP TABLE IF EXISTS" in query for query in connector.queries)


def _load_config(**overrides) -> LoadConfig:
    base = LoadConfig(
        source_conn_id="source",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={},
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def _load_record() -> LoadAuditRecord:
    return LoadAuditRecord(
        run_id="run_1",
        load_id="load_1",
        status="staged",
        process_name="orders",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="full_refresh",
        started_at=SimpleNamespace(),
    )


def _strict_row_count_gate() -> dict[str, object]:
    return {
        "id": "row_count_reconciliation",
        "type": "row_count_reconciliation",
        "severity": "error",
        "tolerance": {"mode": "absolute", "value": 0},
    }


class _Projector:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def project(self, *, load_config, handle, lineage_options, load_record):  # noqa: ANN001
        del load_config, lineage_options, load_record
        self._events.append("project")
        return LineageProjectionResult(
            handle=handle,
            projected=True,
            columns=("__dpone__run_id", "__dpone__load_id"),
        )


class _ProjectedTableProjector(_Projector):
    def project(self, *, load_config, handle, lineage_options, load_record):  # noqa: ANN001
        result = super().project(
            load_config=load_config,
            handle=handle,
            lineage_options=lineage_options,
            load_record=load_record,
        )
        projected = SimpleNamespace(
            target_schema=handle.staging_config.target_schema,
            target_table="orders__projected",
        )
        projected_handle = replace(
            result.handle,
            finalization_config=projected,
            metadata={
                **dict(result.handle.metadata),
                "operation_tables": {
                    "staging": "landing.orders__raw",
                    "projected": "landing.orders__projected",
                },
            },
        )
        return replace(result, handle=projected_handle)


class _StagedSink:
    def __init__(self, events: list[str], *, staged_rows: int) -> None:
        self._events = events
        self._staged_rows = staged_rows

    def stage_payload(self, load_config, payload):  # noqa: ANN001
        del load_config
        self._events.append("stage")
        return StagedLoadHandle(
            staging_config=SimpleNamespace(target_schema="landing", target_table="orders__staging"),
            payload_schema=tuple(payload.schema),
            staged_rows=self._staged_rows,
        )

    def finalize_staged_load(self, load_config, handle):  # noqa: ANN001
        del load_config
        self._events.append("finalize")
        return LoadResult(
            inserted_rows=handle.staged_rows,
            updated_rows=0,
            total_rows=handle.staged_rows,
            staging_rows=handle.staged_rows,
        )

    def abort_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("abort")


class _KeyValidationFailureSink(_StagedSink):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events, staged_rows=1)

    def stage_payload(self, load_config, payload):  # noqa: ANN001
        handle = super().stage_payload(load_config, payload)
        raw = replace(load_config, target_table="orders__raw")
        return replace(
            handle,
            staging_config=raw,
            metadata={"operation_tables": {"staging": "landing.orders__raw"}},
        )

    def validate_staged_load(self, load_config, handle):  # noqa: ANN001
        del load_config
        self._events.append(f"validate:{handle.finalization_config.target_table}")
        raise ClickHouseStagingKeyError(
            CLICKHOUSE_STAGING_UNIQUE_KEY_NULL,
            "unsafe staged key",
        )


class _ValidatedFinalizationSink(_StagedSink):
    supports_staged_validation_receipts = True

    def __init__(self, events: list[str]) -> None:
        super().__init__(events, staged_rows=1)
        self.finalized_semantics: tuple[str, tuple[str, ...], str] | None = None

    def validate_staged_load(self, load_config, handle):  # noqa: ANN001
        del load_config
        self._events.append(f"validate:{handle.finalization_config.target_table}")
        return "validated"

    def finalize_staged_load(self, load_config, receipt):  # noqa: ANN001
        _token, validated_config, handle = receipt.frozen_inputs(sink=self, load_config=load_config)
        self.finalized_semantics = (
            validated_config.target_table,
            tuple(validated_config.unique_key or ()),
            handle.finalization_config.target_table,
        )
        self._events.append("finalize_validated")
        return LoadResult(
            inserted_rows=handle.staged_rows,
            updated_rows=0,
            total_rows=handle.staged_rows,
            staging_rows=handle.staged_rows,
        )

    def cleanup_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("cleanup")


class _TargetMutationFailureSink(_ValidatedFinalizationSink):
    def finalize_staged_load(self, load_config, receipt):  # noqa: ANN001
        receipt.frozen_inputs(sink=self, load_config=load_config)
        self._events.append("target_mutation")
        raise RuntimeError("target mutation failed")


class _LegacyQualitySink:
    def __init__(self, result: LoadResult) -> None:
        self._result = result
        self.load_calls = 0

    def load(self, load_config, payload):  # noqa: ANN001
        del load_config, payload
        self.load_calls += 1
        return self._result


class _OrderedLegacySink(_LegacyQualitySink):
    def __init__(self, events: list[str], result: LoadResult) -> None:
        super().__init__(result)
        self._events = events

    def load(self, load_config, payload):  # noqa: ANN001
        self._events.append("load")
        return super().load(load_config, payload)


class _InvalidRowsStagedSink(_StagedSink):
    def __init__(self, events: list[str], *, staged_rows: object) -> None:
        super().__init__(events, staged_rows=0)
        self._invalid_staged_rows = staged_rows

    def stage_payload(self, load_config, payload):  # noqa: ANN001
        del load_config
        self._events.append("stage")
        values = {
            "staging_config": SimpleNamespace(
                target_schema="landing",
                target_table="orders__staging",
            ),
            "payload_schema": tuple(payload.schema),
        }
        if self._invalid_staged_rows is not _MISSING:
            values["staged_rows"] = self._invalid_staged_rows
        return SimpleNamespace(**values)


class _FaultingStagedSink(_StagedSink):
    def __init__(
        self,
        events: list[str],
        *,
        staged_rows: int,
        abort_error: Exception | None = None,
    ) -> None:
        super().__init__(events, staged_rows=staged_rows)
        self._abort_error = abort_error

    def abort_staged_load(self, handle):  # noqa: ANN001
        super().abort_staged_load(handle)
        if self._abort_error is not None:
            raise self._abort_error

    def cleanup_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("cleanup")


class _FaultingAuditStorage:
    def __init__(self, events: list[str], *, fail_steps: set[str]) -> None:
        self._events = events
        self._fail_steps = fail_steps
        self.records = []

    def record_step(self, record) -> None:  # noqa: ANN001
        self._events.append(f"evidence:{record.step_id}")
        if record.step_id in self._fail_steps:
            raise RuntimeError(f"{record.step_id} evidence failed")
        self.records.append(record)


class _MetricProbe:
    def __init__(self, side_rows: dict[str, int]) -> None:
        self._side_rows = side_rows
        self.requests = []

    def collect(self, request):  # noqa: ANN001
        self.requests.append(request)
        columns = tuple(request.columns)
        return AcceptanceMetricSnapshot(
            side=request.side,
            row_count=self._side_rows[request.side],
            null_counts={column: 0 for column in columns},
            distinct_counts={column: 1 for column in columns},
            columns=columns,
            dataset=request.dataset_identity,
        )


class _MetricSource:
    def __init__(self) -> None:
        self.metric_probe = _MetricProbe({"source": 1})


class _MetricSink(_StagedSink):
    def __init__(self, events: list[str], *, staged_rows: int) -> None:
        super().__init__(events, staged_rows=staged_rows)
        self.metric_probe = _MetricProbe({"staged": staged_rows, "target": staged_rows})

    def cleanup_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("cleanup")


class _PostCommitMetricFailureProbe(_MetricProbe):
    def collect(self, request):  # noqa: ANN001
        if request.side == "target":
            raise RuntimeError("target metric evidence failed")
        return super().collect(request)


class _PostCommitMetricFailureSink(_FaultingStagedSink):
    def __init__(self, events: list[str], *, staged_rows: int) -> None:
        super().__init__(events, staged_rows=staged_rows)
        self.metric_probe = _PostCommitMetricFailureProbe({"staged": staged_rows, "target": staged_rows})


class _PostCommitMetricAndCleanupFailureSink(_PostCommitMetricFailureSink):
    def cleanup_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("cleanup")
        raise RuntimeError("cleanup failed")


class _CleanupFailureSink(_StagedSink):
    def cleanup_staged_load(self, handle):  # noqa: ANN001
        del handle
        self._events.append("cleanup")
        raise RuntimeError("cleanup failed")


class _ClickHouseConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query, params=None):  # noqa: ANN001
        del params
        self.queries.append(query)
        return 0

    def get_records(self, query, params=None, as_dict=False):  # noqa: ANN001
        del query, params, as_dict
        return [(2,)]


class _ClickHouseProjectionSink:
    def __init__(self, connector: _ClickHouseConnector) -> None:
        self.connector = connector
        self.target_lookups = 0

    def _table(self, load_config) -> str:  # noqa: ANN001
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    def _table_exists(self, load_config) -> bool:  # noqa: ANN001
        del load_config
        self.target_lookups += 1
        return False

    def _operation_table_name(self, target_table: str, operation: str) -> str:
        return f"{target_table}__dpone_{operation}_test"

    def _create_table_with_clickhouse_types(self, load_config, schema, *, if_not_exists):  # noqa: ANN001
        del if_not_exists
        columns_sql = ", ".join(f"`{column}` {dtype}" for column, dtype in schema)
        self.connector.execute_query(f"CREATE TABLE {self._table(load_config)} ({columns_sql}) ENGINE = Memory")


class _ClickHouseLoadClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, object]] = []

    def execute(self, query, params=None, **kwargs):  # noqa: ANN001
        del kwargs
        if str(query).startswith("INSERT"):
            self.inserts.append((query, params))
        return [(1,)]


class _ClickHouseLoadConnector:
    def __init__(self) -> None:
        self.connection = _ClickHouseLoadClient()
        self.queries: list[str] = []

    def execute_query(self, query, params=None):  # noqa: ANN001
        del params
        self.queries.append(query)
        return 0

    def get_records(self, query, params=None, as_dict=False):  # noqa: ANN001
        del params, as_dict
        if "EXISTS TABLE" in str(query):
            return [(0,)]
        return [(1,)]
