"""Regression tests for the immutable snapshot-envelope payload lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.manifest.validation_reconciliation import validate_key_snapshot_reconciliation
from dpone.runtime.etl.payload_loader import PayloadLoadService
from dpone.runtime.etl.snapshot_envelope_lifecycle import (
    SnapshotEnvelopeLifecycleError,
    SnapshotEnvelopeLifecyclePolicy,
)
from dpone.runtime.incremental_snapshot import (
    DeltaSnapshotReceipt,
    IncrementalSnapshotEnvelope,
    KeySnapshotReceipt,
)
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult

_TOKEN = "sha256:" + "a" * 64
_SCOPE = "sha256:" + "b" * 64


@dataclass(frozen=True)
class _Artifact:
    receipt: object
    estimated_rows: int = 1

    def cleanup(self) -> None:
        return None


def _envelope() -> IncrementalSnapshotEnvelope[object, object]:
    delta_schema = (
        ("guid", "uniqueidentifier"),
        ("value", "int"),
        ("__dpone__xmin", "bigint"),
        ("__dpone__delta_hash", "varchar(64)"),
    )
    delta_receipt = DeltaSnapshotReceipt(
        snapshot_token=_TOKEN,
        scope_hash=_SCOPE,
        columns=tuple(name for name, _dtype in delta_schema),
        row_count=1,
        checksum="sha256:" + "c" * 64,
        complete=True,
    )
    key_receipt = KeySnapshotReceipt(
        snapshot_token=_TOKEN,
        scope_hash=_SCOPE,
        key_columns=("guid",),
        row_count=1,
        checksum="sha256:" + "d" * 64,
        complete=True,
    )
    return IncrementalSnapshotEnvelope(
        delta_artifact=_Artifact(delta_receipt),
        key_artifact=_Artifact(key_receipt),
        delta_schema=delta_schema,
        key_schema=(("guid", "uniqueidentifier"), ("__dpone__key_hash", "varchar(64)")),
        previous_checkpoint=None,
        candidate_checkpoint=object(),
        snapshot_token=_TOKEN,
        scope_hash=_SCOPE,
        state_key=object(),
        baseline=True,
    )


def _load_config(*, predicate: str | None = None) -> LoadConfig:
    options: dict[str, Any] = {
        "physical_design": {"apply_runtime": False},
        "reconciliation": {
            "enabled": True,
            "mode": "key_snapshot",
            "cadence": "every_run",
            "consistency": "same_source_snapshot",
            "delete_policy": "soft_delete",
            "empty_snapshot": {"policy": "fail"},
        },
    }
    if predicate is not None:
        options["source_custom_predicate"] = predicate
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_value",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
        options=options,
    )


class _ForbiddenMutableLifecycle:
    def prepare_before_schema_evolution(self, **_kwargs: object) -> object:
        raise AssertionError("generic lifecycle must not receive a snapshot envelope")

    def prepare_after_schema_evolution(self, **_kwargs: object) -> object:
        raise AssertionError("generic lifecycle must not receive a snapshot envelope")

    def runtime_metrics(self, _context: object) -> dict[str, object]:
        return {}

    def write_evidence(self, **_kwargs: object) -> None:
        return None


class _ForbiddenProjection:
    def prepare_payload(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic schema projection/evolution must be skipped")


class _NativeContext:
    enabled = False
    should_skip_load = False

    def __init__(self, payload: LoadPayload) -> None:
        self.payload = payload


class _NativeService:
    def prepare_before_load(self, *, payload: LoadPayload, **_kwargs: object) -> _NativeContext:
        return _NativeContext(payload)


class _Governance:
    def validate_quality_config(self, **_kwargs: object) -> None:
        return None

    def validate_quality_gate_receipt(self, *, receipt: object, **_kwargs: object) -> object:
        return receipt


class _Coordinator:
    def __init__(self) -> None:
        self.payload: LoadPayload | None = None
        self.load_record: object | None = None

    def load(self, **kwargs: object) -> LoadResult:
        self.payload = kwargs["payload"]  # type: ignore[assignment]
        self.load_record = kwargs["load_record"]
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1)


class _LoadIdentity:
    def __init__(self) -> None:
        self.staged_record = SimpleNamespace(run_id="run-1", load_id="load-1", phase="staged")
        self.committed_record: object | None = None

    def mark_staged(self, _record: object, *, extracted_rows: object) -> object:
        assert extracted_rows == 1
        return self.staged_record

    def mark_committed(self, record: object, _result: object) -> None:
        self.committed_record = record


def test_standard_payload_loader_skips_every_mutable_lifecycle_for_envelope() -> None:
    envelope = _envelope()
    payload = LoadPayload(artifact=envelope, schema=list(envelope.delta_schema))
    coordinator = _Coordinator()
    identity = _LoadIdentity()
    service = PayloadLoadService(
        sink=SimpleNamespace(),
        logger=SimpleNamespace(),
        load_identity_service=identity,
        strategy_metadata_enricher=SimpleNamespace(enrich_payload=lambda incoming, **_kwargs: incoming),
        schema_identity_service=_ForbiddenProjection(),
        schema_evolution_service=_ForbiddenProjection(),
        runtime_lifecycle_service=_ForbiddenMutableLifecycle(),
        native_transfer_runtime_service=_NativeService(),
        load_governance_service=_Governance(),
        legacy_governance_coordinator=coordinator,
    )

    result = service.load_single_payload(
        _load_config(),
        payload,
        SimpleNamespace(artifact=envelope),
        SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.inserted_rows == 1
    assert coordinator.payload is not None
    assert coordinator.payload.artifact is envelope
    assert tuple(coordinator.payload.schema) == envelope.delta_schema
    assert "__dpone__xmin" in {name for name, _dtype in coordinator.payload.schema}
    assert coordinator.load_record is identity.staged_record
    assert identity.committed_record is identity.staged_record


def test_snapshot_lifecycle_rejects_payload_schema_drift() -> None:
    envelope = _envelope()
    payload = LoadPayload(artifact=envelope, schema=[("guid", "uniqueidentifier")])

    with pytest.raises(
        SnapshotEnvelopeLifecycleError,
        match="payload_schema_does_not_match_envelope",
    ):
        SnapshotEnvelopeLifecyclePolicy().prepare(load_config=_load_config(), payload=payload)


def test_snapshot_lifecycle_rejects_unscoped_target_delete_for_source_predicate() -> None:
    envelope = _envelope()
    payload = LoadPayload(artifact=envelope, schema=envelope.delta_schema)

    with pytest.raises(
        SnapshotEnvelopeLifecycleError,
        match="scoped_predicate_is_unsupported",
    ):
        SnapshotEnvelopeLifecyclePolicy().prepare(
            load_config=_load_config(predicate="date_calculated >= CURRENT_DATE"),
            payload=payload,
        )


def test_manifest_validation_rejects_scoped_key_snapshot_before_source_io() -> None:
    load_config = _load_config(predicate="date_calculated >= CURRENT_DATE")
    load_config.options.update(
        {
            "source_options": {
                "incremental_strategy": "xmin",
                "batch_commit_mode": "whole",
                "export_format": "csv",
                "compress_export": False,
                "custom_predicate": "date_calculated >= CURRENT_DATE",
            },
            "state": {
                "type": "mssql",
                "connection_ref": "mssql_sample_metrics_state",
                "atomicity": "target_atomic",
                "provisioning": "external",
            },
            "soft_delete": {"mode": "timestamp_only"},
            "technical_columns": "required",
        }
    )
    spec = SimpleNamespace(
        name="sample_metrics_metrics_value",
        selector="public.metrics_value",
        raw_config={"source": {"type": "postgres"}, "sink": {"type": "mssql"}},
    )

    issues = validate_key_snapshot_reconciliation(
        spec,
        manifest_path=Path("workloads/platform/sample_metrics_metrics_value/pipeline.yaml"),
        selector=spec.selector,
        load_cfg=load_config,
        options=load_config.options,
    )

    assert "RECONCILIATION_SCOPED_PREDICATE_UNSUPPORTED" in {issue.code for issue in issues}


@pytest.mark.parametrize(
    "mssql_type",
    ("mssql", "MSSQL", "microsoft mssql", "microsoft_mssql", "odbc", "sqlserver", "sql_server", "sql-server"),
)
def test_manifest_reconciliation_accepts_canonical_endpoint_aliases(mssql_type: str) -> None:
    load_config = _load_config()
    load_config.options.update(
        {
            "source_options": {
                "incremental_strategy": "xmin",
                "batch_commit_mode": "whole",
                "export_format": "csv",
                "compress_export": False,
            },
            "state": {
                "type": mssql_type,
                "connection_ref": "mssql_sample_metrics_state",
                "atomicity": "target_atomic",
                "provisioning": "external",
            },
            "soft_delete": {"mode": "timestamp_only"},
            "technical_columns": "required",
        }
    )
    spec = SimpleNamespace(
        name="sample_metrics_metrics_value",
        selector="public.metrics_value",
        raw_config={"source": {"type": "PostgreSQL"}, "sink": {"type": mssql_type}},
    )

    issues = validate_key_snapshot_reconciliation(
        spec,
        manifest_path=Path("workloads/platform/sample_metrics_metrics_value/pipeline.yaml"),
        selector=spec.selector,
        load_cfg=load_config,
        options=load_config.options,
    )

    assert issues == []


class _AuditStorage:
    def __init__(self) -> None:
        self.statuses: list[str] = []

    def record_load_started(self, record: object) -> None:
        self.statuses.append(record.status)  # type: ignore[attr-defined]

    def record_load_staged(self, record: object) -> None:
        self.statuses.append(record.status)  # type: ignore[attr-defined]

    def record_load_committed(self, record: object) -> None:
        self.statuses.append(record.status)  # type: ignore[attr-defined]

    def record_load_failed(self, record: object) -> None:
        self.statuses.append(record.status)  # type: ignore[attr-defined]


def test_receipt_backed_audit_commit_cannot_be_downgraded_by_post_hook_failure() -> None:
    storage = _AuditStorage()
    identity = SimpleNamespace(new_run_id=lambda: "run-1", new_load_id=lambda: "load-1")
    service = LoadIdentityService(identity_service=identity, audit_storage=storage)
    started = service.start(_load_config(), process_name="sample_metrics_metrics_value")
    committed = service.mark_committed(
        started,
        LoadResult(
            inserted_rows=1,
            updated_rows=0,
            total_rows=1,
            commit_receipt_id="load-1",
            commit_outcome=AtomicCommitOutcome.COMMITTED,
        ),
    )

    after_post_hook_failure = service.mark_failed(started, RuntimeError("post-hook failed"))

    assert after_post_hook_failure is committed
    assert storage.statuses == ["started", "committed"]
