from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.payload_loader import PayloadLoadService
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.acceptance_metrics import (
    AcceptanceMetricConfigurationError,
    AcceptanceMetricPolicy,
    AcceptanceMetricSnapshot,
)
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.kafka.offsets import KafkaOffsetState
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sources.base import ExtractResult


def _load_config(acceptance: object = None, *, include_acceptance: bool = True) -> Any:
    quality = {"acceptance": acceptance} if include_acceptance else {}
    return SimpleNamespace(options={"quality": quality})


def test_acceptance_policy_normalizes_the_schema_valid_contract() -> None:
    policy = AcceptanceMetricPolicy.from_load_config(
        _load_config(
            {
                "enabled": True,
                "mode": "required",
                "capture": {"source": True, "staged": False, "target": True},
                "checks": {
                    "row_count": True,
                    "null_counts": ["project_id", "item_id"],
                    "distinct_counts": "business_columns",
                },
            }
        )
    )

    assert policy.enabled is True
    assert policy.mode == "required"
    assert policy.requested_sides == ("source", "target")
    assert policy.row_count is True
    assert policy.null_counts == ("project_id", "item_id")
    assert policy.distinct_counts == "business_columns"


def test_acceptance_policy_is_disabled_when_the_block_is_absent() -> None:
    policy = AcceptanceMetricPolicy.from_load_config(_load_config(include_acceptance=False))

    assert policy.enabled is False


@pytest.mark.parametrize(
    ("acceptance", "field_path"),
    [
        pytest.param({}, "quality.acceptance.enabled", id="missing-enabled"),
        pytest.param({"enabled": "true"}, "quality.acceptance.enabled", id="invalid-enabled"),
        pytest.param(
            {"enabled": True, "mode": "require"},
            "quality.acceptance.mode",
            id="misspelled-mode",
        ),
        pytest.param(
            {"enabled": True, "unexpected": True},
            "quality.acceptance.unexpected",
            id="unknown-acceptance-key",
        ),
        pytest.param(
            {"enabled": True, "capture": ["source"]},
            "quality.acceptance.capture",
            id="capture-not-object",
        ),
        pytest.param(
            {"enabled": True, "capture": {"destination": True}},
            "quality.acceptance.capture.destination",
            id="unknown-capture-key",
        ),
        pytest.param(
            {"enabled": True, "capture": {"source": 1}},
            "quality.acceptance.capture.source",
            id="invalid-capture-boolean",
        ),
        pytest.param(
            {"enabled": True, "checks": ["row_count"]},
            "quality.acceptance.checks",
            id="checks-not-object",
        ),
        pytest.param(
            {"enabled": True, "checks": {"rows": True}},
            "quality.acceptance.checks.rows",
            id="unknown-check-key",
        ),
        pytest.param(
            {"enabled": True, "checks": {"row_count": "exact"}},
            "quality.acceptance.checks.row_count",
            id="invalid-row-count-boolean",
        ),
        pytest.param(
            {"enabled": True, "checks": {"null_counts": "everything"}},
            "quality.acceptance.checks.null_counts",
            id="unknown-null-selector",
        ),
        pytest.param(
            {"enabled": True, "checks": {"distinct_counts": []}},
            "quality.acceptance.checks.distinct_counts",
            id="empty-distinct-selector",
        ),
        pytest.param(
            {"enabled": True, "checks": {"null_counts": ["project_id", "project_id"]}},
            "quality.acceptance.checks.null_counts",
            id="duplicate-null-selector",
        ),
        pytest.param(
            {"enabled": True, "checks": {"distinct_counts": [""]}},
            "quality.acceptance.checks.distinct_counts",
            id="empty-column-name",
        ),
        pytest.param(
            {
                "enabled": True,
                "capture": {"source": False, "staged": False, "target": False},
            },
            "quality.acceptance.capture",
            id="no-requested-side",
        ),
        pytest.param(
            {
                "enabled": True,
                "checks": {
                    "row_count": False,
                    "null_counts": "off",
                    "distinct_counts": False,
                },
            },
            "quality.acceptance.checks",
            id="no-requested-check",
        ),
        pytest.param("required", "quality.acceptance", id="acceptance-not-object"),
    ],
)
def test_acceptance_policy_rejects_schema_invalid_authoring(
    acceptance: object,
    field_path: str,
) -> None:
    with pytest.raises(AcceptanceMetricConfigurationError) as raised:
        AcceptanceMetricPolicy.from_load_config(_load_config(acceptance))

    assert raised.value.code == "DPONE_ACCEPTANCE_METRIC_CONFIGURATION_INVALID"
    assert raised.value.field_path == field_path


def test_disabled_acceptance_policy_still_validates_its_authored_shape() -> None:
    with pytest.raises(AcceptanceMetricConfigurationError) as raised:
        AcceptanceMetricPolicy.from_load_config(
            _load_config(
                {
                    "enabled": False,
                    "capture": {"source": "false"},
                }
            )
        )

    assert raised.value.field_path == "quality.acceptance.capture.source"


@pytest.mark.parametrize(
    "selector",
    [
        False,
        True,
        "off",
        "none",
        "all_columns",
        "business_columns",
        "source_and_binary_semantics",
        "strict",
        ["project_id", "item_id"],
    ],
)
def test_acceptance_policy_accepts_every_schema_supported_column_selector(selector: object) -> None:
    policy = AcceptanceMetricPolicy.from_load_config(
        _load_config(
            {
                "enabled": True,
                "checks": {
                    "row_count": True,
                    "null_counts": selector,
                    "distinct_counts": selector,
                },
            }
        )
    )

    assert policy.enabled is True


def test_legacy_required_acceptance_captures_source_and_target_without_staged_success() -> None:
    audit = InMemoryLoadStepAuditStorage()
    source = _MetricSource(_MetricProbe())
    sink = _LegacySink(_MetricProbe())

    result = _legacy_coordinator(audit).load(
        source=source,
        sink=sink,
        load_config=_runtime_config(capture={"source": True, "staged": False, "target": True}),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    assert sink.load_calls == 1
    assert [request.side for request in source.metric_probe.requests] == ["source"]
    assert [request.side for request in sink.metric_probe.requests] == ["target"]
    assert sink.metric_probe.requests[0].columns == ("id", "name")
    assert source.metric_probe.requests[0].database == "DWH_Raw"
    assert source.metric_probe.requests[0].dataset_identity == "DWH_Raw.dbo.orders"
    assert sink.metric_probe.requests[0].database == "DWH_Dev"
    assert sink.metric_probe.requests[0].dataset_identity == "DWH_Dev.landing.orders"
    assert result.reconciliation_metrics is not None
    assert result.reconciliation_metrics["governance_finalization"] == "legacy_post_finalize"
    snapshots = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]
    assert set(snapshots) == {"source", "target"}
    assert all(snapshot["warnings"] == [] for snapshot in snapshots.values())


def test_non_mssql_acceptance_identity_remains_schema_table_compatible() -> None:
    source_probe = _MetricProbe()
    config = _runtime_config(capture={"source": True, "staged": False, "target": False})
    config.options["source_type"] = "clickhouse"

    _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
        source=_MetricSource(source_probe),
        sink=_LegacySink(_MetricProbe()),
        load_config=config,
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    request = source_probe.requests[0]
    assert request.database == "DWH_Raw"
    assert request.dataset_identity == "dbo.orders"


def test_legacy_required_implicit_staged_side_fails_before_sink_mutation() -> None:
    audit = InMemoryLoadStepAuditStorage()
    source = _MetricSource(_MetricProbe())
    sink = _LegacySink(_MetricProbe())

    with pytest.raises(RuntimeError, match="staged_acceptance_metric_unavailable"):
        _legacy_coordinator(audit).load(
            source=source,
            sink=sink,
            load_config=_runtime_config(),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.load_calls == 0
    assert source.metric_probe.requests == []
    failure = _failure_record(audit)
    assert failure.error_message == "staged_acceptance_metric_unavailable"
    assert failure.details == {
        "failure_boundary": "pre_commit",
        "acceptance_side": "staged",
        "error_code": "staged_acceptance_metric_unavailable",
    }


def test_legacy_required_missing_target_probe_fails_before_sink_mutation() -> None:
    audit = InMemoryLoadStepAuditStorage()
    sink = _LegacySink()

    with pytest.raises(RuntimeError, match="target_acceptance_metric_probe_unavailable"):
        _legacy_coordinator(audit).load(
            source=_MetricSource(_MetricProbe()),
            sink=sink,
            load_config=_runtime_config(capture={"source": True, "staged": False, "target": True}),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.load_calls == 0
    assert _failure_record(audit).details["acceptance_side"] == "target"


def test_legacy_target_probe_failure_is_post_commit_and_preserves_primary_error() -> None:
    audit = InMemoryLoadStepAuditStorage()
    primary = RuntimeError("connector query failed password=do-not-record")
    sink = _LegacySink(_MetricProbe(failure_side="target", error=primary))

    with pytest.raises(RuntimeError) as raised:
        _legacy_coordinator(audit).load(
            source=_MetricSource(_MetricProbe()),
            sink=sink,
            load_config=_runtime_config(capture={"source": True, "staged": False, "target": True}),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert raised.value is primary
    assert sink.load_calls == 1
    failure = _failure_record(audit)
    assert failure.error_message == "target_acceptance_metric_probe_failed"
    assert failure.details == {
        "failure_boundary": "post_commit",
        "acceptance_side": "target",
        "error_code": "target_acceptance_metric_probe_failed",
    }
    assert "do-not-record" not in str(failure)


def test_legacy_warn_only_records_one_warning_snapshot_for_each_unavailable_side() -> None:
    audit = InMemoryLoadStepAuditStorage()
    source = _MetricSource(_MetricProbe())
    sink = _LegacySink()

    result = _legacy_coordinator(audit).load(
        source=source,
        sink=sink,
        load_config=_runtime_config(mode="warn_only"),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    assert sink.load_calls == 1
    snapshots = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]
    assert set(snapshots) == {"source", "staged", "target"}
    assert snapshots["source"]["warnings"] == []
    assert snapshots["staged"]["warnings"] == ["staged_acceptance_metric_unavailable"]
    assert snapshots["target"]["warnings"] == ["target_acceptance_metric_probe_unavailable"]
    warning_steps = [record.step_id for record in audit.records if record.status == "warning"]
    assert warning_steps == ["staged_metrics_captured", "target_metrics_captured"]


def test_warn_only_unknown_explicit_columns_are_excluded_and_reported() -> None:
    source_probe = _MetricProbe()
    sink_probe = _MetricProbe()
    result = _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
        source=_MetricSource(source_probe),
        sink=_LegacySink(sink_probe),
        load_config=_runtime_config(
            mode="warn_only",
            capture={"source": True, "staged": False, "target": True},
            checks={"row_count": True, "null_counts": ["id", "missing"]},
        ),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    assert source_probe.requests[0].null_count_columns == ("id",)
    assert sink_probe.requests[0].null_count_columns == ("id",)
    snapshots = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]
    assert snapshots["source"]["warnings"] == ["acceptance_metric_unknown_columns"]
    assert snapshots["target"]["warnings"] == ["acceptance_metric_unknown_columns"]


def test_required_unknown_explicit_columns_fail_before_legacy_sink_mutation() -> None:
    sink = _LegacySink(_MetricProbe())

    with pytest.raises(AcceptanceMetricConfigurationError) as raised:
        _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
            source=_MetricSource(_MetricProbe()),
            sink=sink,
            load_config=_runtime_config(
                capture={"source": True, "staged": False, "target": True},
                checks={"row_count": True, "distinct_counts": ["missing"]},
            ),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert raised.value.field_path == "quality.acceptance.checks.distinct_counts"
    assert sink.load_calls == 0


@pytest.mark.parametrize(
    "snapshot",
    [
        pytest.param(AcceptanceMetricSnapshot(side="source", row_count=None), id="missing-row-count"),
        pytest.param(AcceptanceMetricSnapshot(side="source", row_count=-1), id="negative-row-count"),
        pytest.param(AcceptanceMetricSnapshot(side="source", row_count=True), id="boolean-row-count"),
        pytest.param(
            AcceptanceMetricSnapshot(side="source", row_count=1, null_counts={}),
            id="missing-null-count",
        ),
        pytest.param(
            AcceptanceMetricSnapshot(side="source", row_count=1, null_counts={"id": -1}),
            id="negative-null-count",
        ),
        pytest.param(
            AcceptanceMetricSnapshot(side="source", row_count=1, warnings=("connector secret=raw",)),
            id="connector-warning",
        ),
    ],
)
def test_required_acceptance_rejects_incomplete_or_warning_probe_snapshot(
    snapshot: AcceptanceMetricSnapshot,
) -> None:
    audit = InMemoryLoadStepAuditStorage()
    sink = _LegacySink(_MetricProbe())
    checks: dict[str, object] = {"row_count": True}
    if snapshot.null_counts or snapshot.row_count == 1 and not snapshot.warnings:
        checks["null_counts"] = ["id"]

    with pytest.raises(RuntimeError, match="source_acceptance_metric_snapshot_invalid"):
        _legacy_coordinator(audit).load(
            source=_MetricSource(_StaticMetricProbe(snapshot)),
            sink=sink,
            load_config=_runtime_config(
                capture={"source": True, "staged": False, "target": True},
                checks=checks,
            ),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert sink.load_calls == 0
    failure = _failure_record(audit)
    assert failure.error_message == "source_acceptance_metric_snapshot_invalid"
    assert "secret=raw" not in str(failure)


def test_warn_only_invalid_probe_snapshot_is_canonical_warning() -> None:
    result = _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
        source=_MetricSource(
            _StaticMetricProbe(
                AcceptanceMetricSnapshot(
                    side="attacker",
                    row_count=None,
                    dataset="SELECT password FROM vault",
                    columns=("/vault/secret",),
                    warnings=("token=must-not-leak",),
                    kind="attacker.controlled",
                )
            )
        ),
        sink=_LegacySink(_MetricProbe()),
        load_config=_runtime_config(
            mode="warn_only",
            capture={"source": True, "staged": False, "target": True},
        ),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    snapshot = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]["source"]
    assert snapshot == {
        "kind": "dpone.acceptance.metric_snapshot.v1",
        "side": "source",
        "dataset": "DWH_Raw.dbo.orders",
        "columns": ["id", "name"],
        "row_count": None,
        "null_counts": {},
        "distinct_counts": {},
        "warnings": ["acceptance_metric_snapshot_invalid"],
        "captured_before_cleanup": True,
    }
    assert "password" not in str(result.reconciliation_metrics)
    assert "vault" not in str(result.reconciliation_metrics)
    assert "must-not-leak" not in str(result.reconciliation_metrics)


def test_required_source_selector_is_validated_against_source_schema() -> None:
    sink = _LegacySink(_MetricProbe())
    extract_result = ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": 1}]),
        schema=[("id", "bigint")],
    )
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1, "derived_column": "x"}]),
        schema=[("id", "bigint"), ("derived_column", "text")],
    )

    with pytest.raises(AcceptanceMetricConfigurationError):
        _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
            source=_MetricSource(_MetricProbe()),
            sink=sink,
            load_config=_runtime_config(
                capture={"source": True, "staged": False, "target": False},
                checks={"row_count": True, "null_counts": ["derived_column"]},
            ),
            payload=payload,
            extract_result=extract_result,
            load_record=_load_record(),
        )

    assert sink.load_calls == 0


def test_required_source_only_column_does_not_require_target_schema_membership() -> None:
    source_probe = _MetricProbe()
    extract_result = ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": 1, "source_only": "x"}]),
        schema=[("id", "bigint"), ("source_only", "text")],
    )

    result = _legacy_coordinator(InMemoryLoadStepAuditStorage()).load(
        source=_MetricSource(source_probe),
        sink=_LegacySink(_MetricProbe()),
        load_config=_runtime_config(
            capture={"source": True, "staged": False, "target": False},
            checks={"row_count": True, "distinct_counts": ["source_only"]},
        ),
        payload=_payload(),
        extract_result=extract_result,
        load_record=_load_record(),
    )

    assert source_probe.requests[0].distinct_count_columns == ("source_only",)
    snapshot = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]["source"]
    assert snapshot["distinct_counts"] == {"source_only": 1}


def test_probe_controlled_identity_and_extra_metrics_never_enter_evidence() -> None:
    audit = InMemoryLoadStepAuditStorage()
    hostile = AcceptanceMetricSnapshot(
        side="target",
        row_count=1,
        null_counts={"id": 0, "/vault/secret": 1},
        distinct_counts={"id": 1, "password": 99},
        dataset="SELECT * FROM secret password=raw",
        columns=("/vault/secret",),
        warnings=(),
        kind="attacker.controlled",
    )
    result = _legacy_coordinator(audit).load(
        source=_MetricSource(_StaticMetricProbe(hostile)),
        sink=_LegacySink(_StaticMetricProbe(hostile)),
        load_config=_runtime_config(
            capture={"source": True, "staged": False, "target": True},
            checks={"row_count": True, "null_counts": ["id"], "distinct_counts": ["id"]},
        ),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    serialized = str(result.reconciliation_metrics) + str([record.details for record in audit.records])
    assert "attacker.controlled" not in serialized
    assert "SELECT *" not in serialized
    assert "/vault/secret" not in serialized
    assert "password" not in serialized
    snapshots = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]
    assert snapshots["source"]["dataset"] == "DWH_Raw.dbo.orders"
    assert snapshots["target"]["dataset"] == "DWH_Dev.landing.orders"
    assert snapshots["source"]["null_counts"] == {"id": 0}
    assert snapshots["target"]["distinct_counts"] == {"id": 1}


def test_resume_only_required_acceptance_runs_without_sink_or_checkpoint_mutation() -> None:
    audit = InMemoryLoadStepAuditStorage()
    native = _NativeTransferService(_payload())
    identity = _LoadIdentityService()
    sink = _LegacySink(_MetricProbe())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=sink,
        native=native,
        identity=identity,
        governance=LoadGovernanceService(audit_storage=audit),
    )

    result = service.load_single_payload(
        _runtime_config(capture={"source": True, "staged": False, "target": True}),
        _payload(),
        _extract_result(),
        _load_record(),
    )

    assert sink.load_calls == 0
    assert native.mark_failed_calls == []
    assert native.mark_committed_calls == []
    assert identity.events == ["staged", "committed"]
    snapshots = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]
    assert set(snapshots) == {"source", "target"}


def test_resume_only_required_failure_never_rewrites_committed_checkpoints() -> None:
    audit = InMemoryLoadStepAuditStorage()
    native = _NativeTransferService(_payload())
    identity = _LoadIdentityService()
    sink = _LegacySink(_MetricProbe())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=sink,
        native=native,
        identity=identity,
        governance=LoadGovernanceService(audit_storage=audit),
    )

    with pytest.raises(RuntimeError, match="staged_acceptance_metric_unavailable"):
        service.load_single_payload(
            _runtime_config(),
            _payload(),
            _extract_result(),
            _load_record(),
        )

    assert sink.load_calls == 0
    assert native.mark_failed_calls == []
    assert native.mark_committed_calls == []
    assert identity.events == []
    assert _failure_record(audit).details["failure_boundary"] == "resume_validation"


def test_direct_payload_load_rejects_malformed_acceptance_before_native_prepare() -> None:
    native = _NativeTransferService(_payload())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=_LegacySink(_MetricProbe()),
        native=native,
        identity=_LoadIdentityService(),
        governance=LoadGovernanceService(),
    )

    with pytest.raises(AcceptanceMetricConfigurationError):
        service.load_single_payload(
            _runtime_config(acceptance={"enabled": "true"}),
            _payload(),
            _extract_result(),
            _load_record(),
        )

    assert native.prepare_calls == 0


def test_legacy_governance_double_without_validation_method_remains_compatible() -> None:
    native = _NativeTransferService(_payload())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=_LegacySink(_MetricProbe()),
        native=native,
        identity=_LoadIdentityService(),
        governance=_LegacyGovernanceDouble(),
    )

    result = service.load_single_payload(
        SimpleNamespace(options={}),
        _payload(),
        _extract_result(),
        _load_record(),
    )

    assert result.total_rows == 0
    assert native.prepare_calls == 1


def test_legacy_governance_double_still_uses_strict_acceptance_fallback() -> None:
    native = _NativeTransferService(_payload())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=_LegacySink(_MetricProbe()),
        native=native,
        identity=_LoadIdentityService(),
        governance=_LegacyGovernanceDouble(),
    )

    with pytest.raises(AcceptanceMetricConfigurationError):
        service.load_single_payload(
            _runtime_config(acceptance={"enabled": "true"}),
            _payload(),
            _extract_result(),
            _load_record(),
        )

    assert native.prepare_calls == 0


def test_native_failure_adapter_does_not_retry_internal_type_error() -> None:
    native = _KeywordNativeTransferService(_payload())
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=_LegacySink(_MetricProbe()),
        native=native,
        identity=_LoadIdentityService(),
        governance=_LegacyGovernanceDouble(),
    )

    with pytest.raises(TypeError, match="native failure implementation error"):
        service._mark_native_failed(SimpleNamespace(), RuntimeError("load failed"))

    assert native.mark_failed_attempts == 1


def test_warn_only_probe_failure_records_warning_and_keeps_legacy_load_available() -> None:
    audit = InMemoryLoadStepAuditStorage()
    source_probe = _MetricProbe(failure_side="source")
    result = _legacy_coordinator(audit).load(
        source=_MetricSource(source_probe),
        sink=_LegacySink(_MetricProbe()),
        load_config=_runtime_config(
            mode="warn_only",
            capture={"source": True, "staged": False, "target": True},
        ),
        payload=_payload(),
        extract_result=_extract_result(),
        load_record=_load_record(),
    )

    source_snapshot = result.reconciliation_metrics["acceptance_metrics"]["snapshots"]["source"]
    assert source_snapshot["warnings"] == ["acceptance_metric_probe_failed"]
    source_step = next(record for record in audit.records if record.step_id == "source_metrics_captured")
    assert source_step.status == "warning"


def test_secondary_audit_failure_preserves_primary_required_probe_exception() -> None:
    primary = RuntimeError("connector query failed password=do-not-record")
    audit = _FaultingAuditStorage({"load_governance_failed": RuntimeError("audit unavailable")})

    with pytest.raises(RuntimeError) as raised:
        _legacy_coordinator(audit).load(
            source=_MetricSource(_MetricProbe()),
            sink=_LegacySink(_MetricProbe(failure_side="target", error=primary)),
            load_config=_runtime_config(capture={"source": True, "staged": False, "target": True}),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert raised.value is primary
    assert audit.attempted_steps[-1] == "load_governance_failed"


def test_acceptance_evidence_failure_preserves_itself_and_prevents_legacy_mutation() -> None:
    primary = RuntimeError("source acceptance evidence unavailable")
    audit = _FaultingAuditStorage(
        {
            "source_metrics_captured": primary,
            "load_governance_failed": RuntimeError("secondary evidence unavailable"),
        }
    )
    sink = _LegacySink(_MetricProbe())

    with pytest.raises(RuntimeError) as raised:
        _legacy_coordinator(audit).load(
            source=_MetricSource(_MetricProbe()),
            sink=sink,
            load_config=_runtime_config(capture={"source": True, "staged": False, "target": True}),
            payload=_payload(),
            extract_result=_extract_result(),
            load_record=_load_record(),
        )

    assert raised.value is primary
    assert sink.load_calls == 0
    assert audit.attempted_steps == ["source_metrics_captured", "load_governance_failed"]


def test_resume_target_probe_failure_is_resume_validation_without_checkpoint_rewrite() -> None:
    audit = InMemoryLoadStepAuditStorage()
    primary = RuntimeError("target metric query failed")
    native = _NativeTransferService(_payload())
    identity = _LoadIdentityService()
    service = _payload_service(
        source=_MetricSource(_MetricProbe()),
        sink=_LegacySink(_MetricProbe(failure_side="target", error=primary)),
        native=native,
        identity=identity,
        governance=LoadGovernanceService(audit_storage=audit),
    )

    with pytest.raises(RuntimeError) as raised:
        service.load_single_payload(
            _runtime_config(capture={"source": True, "staged": False, "target": True}),
            _payload(),
            _extract_result(),
            _load_record(),
        )

    assert raised.value is primary
    assert native.mark_failed_calls == []
    assert native.mark_committed_calls == []
    assert identity.events == []
    assert _failure_record(audit).details["failure_boundary"] == "resume_validation"


def test_post_commit_legacy_acceptance_failure_prevents_processor_source_state_persistence() -> None:
    audit = InMemoryLoadStepAuditStorage()
    primary = RuntimeError("target metric query failed")
    source = _ProcessorSource(_MetricProbe())
    sink = _LegacySink(_MetricProbe(failure_side="target", error=primary))

    with pytest.raises(RuntimeError) as raised:
        ETLProcessor(
            source,
            sink,
            etl_logger=_ProcessorLogger(),
            load_governance_service=LoadGovernanceService(audit_storage=audit),
        ).run(_processor_config())

    assert raised.value is primary
    assert sink.load_calls == 1
    assert source.saved is False
    assert _failure_record(audit).details["failure_boundary"] == "post_commit"


def _legacy_coordinator(audit: Any) -> Any:
    from dpone.runtime.governance.legacy_acceptance import LegacyLoadGovernanceCoordinator

    return LegacyLoadGovernanceCoordinator(LoadGovernanceService(audit_storage=audit))


def _runtime_config(
    *,
    mode: str = "required",
    capture: dict[str, bool] | None = None,
    checks: dict[str, object] | None = None,
    acceptance: object | None = None,
) -> Any:
    resolved_acceptance = (
        acceptance
        if acceptance is not None
        else {
            "enabled": True,
            "mode": mode,
            **({"capture": capture} if capture is not None else {}),
            "checks": checks or {"row_count": True},
        }
    )
    return SimpleNamespace(
        source_database="DWH_Raw",
        source_schema="dbo",
        source_table="orders",
        target_database="DWH_Dev",
        target_schema="DWH_Dev.landing",
        target_table="orders",
        options={
            "source_type": "mssql",
            "sink_type": "mssql",
            "quality": {"acceptance": resolved_acceptance},
        },
    )


def _payload() -> LoadPayload:
    return LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
        schema=[("id", "bigint"), ("name", "text")],
    )


def _extract_result() -> ExtractResult:
    return ExtractResult(
        artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
        schema=[("id", "bigint"), ("name", "text")],
    )


def _load_record() -> Any:
    return SimpleNamespace(run_id="run-1", load_id="load-1")


def _failure_record(audit: InMemoryLoadStepAuditStorage) -> Any:
    return next(record for record in reversed(audit.records) if record.step_id == "load_governance_failed")


class _MetricProbe:
    def __init__(self, *, failure_side: str | None = None, error: Exception | None = None) -> None:
        self.failure_side = failure_side
        self.error = error or RuntimeError("metric probe failed")
        self.requests: list[Any] = []

    def collect(self, request: Any) -> AcceptanceMetricSnapshot:
        self.requests.append(request)
        if request.side == self.failure_side:
            raise self.error
        return AcceptanceMetricSnapshot(
            side=request.side,
            row_count=1,
            null_counts={column: 0 for column in request.null_count_columns},
            distinct_counts={column: 1 for column in request.distinct_count_columns},
            columns=request.columns,
            dataset=request.dataset_identity,
        )


class _StaticMetricProbe(_MetricProbe):
    def __init__(self, snapshot: AcceptanceMetricSnapshot) -> None:
        super().__init__()
        self.snapshot = snapshot

    def collect(self, request: Any) -> AcceptanceMetricSnapshot:
        self.requests.append(request)
        return self.snapshot


class _MetricSource:
    def __init__(self, probe: _MetricProbe) -> None:
        self.metric_probe = probe


class _LegacySink:
    def __init__(self, probe: _MetricProbe | None = None) -> None:
        if probe is not None:
            self.metric_probe = probe
        self.load_calls = 0

    def load(self, load_config: Any, payload: Any) -> LoadResult:
        del load_config, payload
        self.load_calls += 1
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


class _NativeTransferContext:
    def __init__(self, payload: LoadPayload) -> None:
        self.payload = payload
        self.should_skip_load = True

    def skipped_load_result(self) -> LoadResult:
        return LoadResult(
            inserted_rows=0,
            updated_rows=0,
            total_rows=0,
            staging_rows=0,
            reconciliation_metrics={"native_transfer_resume": {"summary": {"skip": 1, "retry": 0}}},
        )


class _NativeTransferService:
    def __init__(self, payload: LoadPayload) -> None:
        self.context = _NativeTransferContext(payload)
        self.prepare_calls = 0
        self.mark_failed_calls: list[Exception] = []
        self.mark_committed_calls: list[Any] = []

    def prepare_before_load(self, **kwargs: Any) -> _NativeTransferContext:
        del kwargs
        self.prepare_calls += 1
        return self.context

    def mark_failed(self, context: Any, exc: Exception) -> None:
        del context
        self.mark_failed_calls.append(exc)

    def mark_committed(self, context: Any, result: Any) -> Any:
        del context
        self.mark_committed_calls.append(result)
        return result


class _KeywordNativeTransferService(_NativeTransferService):
    def __init__(self, payload: LoadPayload) -> None:
        super().__init__(payload)
        self.mark_failed_attempts = 0

    def mark_failed(
        self,
        context: Any,
        exc: Exception,
        *,
        safe_error_code: str | None = None,
    ) -> None:
        del context, exc, safe_error_code
        self.mark_failed_attempts += 1
        raise TypeError("native failure implementation error")


class _LoadIdentityService:
    def __init__(self) -> None:
        self.events: list[str] = []

    def mark_staged(self, load_record: Any, *, extracted_rows: int | None) -> None:
        del load_record, extracted_rows
        self.events.append("staged")

    def mark_committed(self, load_record: Any, load_result: Any) -> None:
        del load_record, load_result
        self.events.append("committed")


class _FaultingAuditStorage:
    def __init__(self, failures: dict[str, Exception]) -> None:
        self.failures = failures
        self.attempted_steps: list[str] = []

    def record_step(self, record: Any) -> None:
        self.attempted_steps.append(record.step_id)
        failure = self.failures.get(record.step_id)
        if failure is not None:
            raise failure


class _LegacyGovernanceDouble:
    """Old dependency-injected service shape without validate_quality_config."""


class _ProcessorSource(_MetricSource):
    def __init__(self, probe: _MetricProbe) -> None:
        super().__init__(probe)
        self.saved = False

    def get_incremental_state(self, load_config: Any) -> None:
        del load_config
        return None

    def extract(self, load_config: Any, last_state: Any) -> ExtractResult:
        del load_config, last_state
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
            schema=[("id", "bigint"), ("name", "text")],
            state=KafkaOffsetState(
                topic="orders",
                group_id="dpone.orders",
                partition_offsets={0: 1},
                high_watermarks={0: 1},
                read_mode="offsets",
            ),
        )

    def save_state(self, load_config: Any, saved_state: Any) -> None:
        del load_config, saved_state
        self.saved = True


class _ProcessorLogger:
    def log_etl_start(self, payload: Any) -> None:
        del payload

    def log_etl_progress(self, event: str, payload: Any) -> None:
        del event, payload

    def log_etl_error(self, message: str, payload: Any) -> None:
        del message, payload

    def log_etl_end(self, payload: Any) -> None:
        del payload


class _PassthroughStrategyMetadataEnricher:
    def enrich_payload(self, payload: Any, *, load_config: Any) -> Any:
        del load_config
        return payload


def _payload_service(
    *,
    source: Any,
    sink: Any,
    native: _NativeTransferService,
    identity: _LoadIdentityService,
    governance: Any,
) -> PayloadLoadService:
    return PayloadLoadService(
        source=source,
        sink=sink,
        logger=SimpleNamespace(),
        load_identity_service=identity,
        strategy_metadata_enricher=_PassthroughStrategyMetadataEnricher(),
        schema_identity_service=SimpleNamespace(),
        schema_evolution_service=SimpleNamespace(),
        runtime_lifecycle_service=SimpleNamespace(),
        native_transfer_runtime_service=native,
        load_governance_service=governance,
    )


def _processor_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "lineage": False,
            "quality": {
                "acceptance": {
                    "enabled": True,
                    "mode": "required",
                    "capture": {"source": True, "staged": False, "target": True},
                    "checks": {"row_count": True},
                }
            },
        },
    )
