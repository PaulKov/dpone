from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig, LoadStrategy
from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.decision_audit import (
    CompositeDecisionPublisher,
    DecisionAuditPolicy,
    RuntimeDecision,
    RuntimeDecisionContext,
    RuntimeDecisionSummary,
    normalize_runtime_decision,
    publish_runtime_decision,
)
from dpone.runtime.direct_ingest import DIRECT_INGEST_SCHEMA_VERSION
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.sinks.clickhouse_bulk_mixin import ClickHouseBulkMixin
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializedSnapshot,
)
from dpone.runtime.source_materialization_cleanup import SourceMaterializationCleanupResult
from dpone.runtime.sources.base import ExtractResult

ROOT = Path(__file__).resolve().parents[1]


def test_runtime_decision_redacts_secrets_and_records_audit_log_and_summary() -> None:
    audit = InMemoryLoadStepAuditStorage()
    logger = _DecisionLogger()
    summary = RuntimeDecisionSummary()
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=_load_record(),
        logger=logger,
        summary=summary,
    )
    decision = RuntimeDecision(
        decision_id="test.secret_redaction",
        phase="load",
        component="unit",
        category="backend_selection",
        requested="auto",
        selected="client",
        fallback_allowed=True,
        fallback_reason="provider_missing",
        release_gate="warning",
        warnings=("provider_missing",),
        details={
            "password": "plain-secret",
            "nested": {"api_token": "token-value"},
            "url": "https://svc:secret@example.com/path",
        },
    )

    publisher.publish(decision)

    assert len(audit.records) == 1
    record = audit.records[0]
    assert record.step_id == "test.secret_redaction"
    assert record.kind == "runtime_decision"
    assert record.phase == "load"
    assert record.status == "succeeded"
    assert record.details["fallback_reason"] == "provider_missing"
    assert record.details["details"]["password"] == "***"
    assert record.details["details"]["nested"]["api_token"] == "***"
    assert "plain-secret" not in str(record.details)
    assert "token-value" not in str(record.details)
    assert logger.warnings
    assert "event=dpone.runtime_decision" in logger.warnings[0]
    assert "fallback_reason=provider_missing" in logger.warnings[0]
    assert summary.to_jsonable()["by_release_gate"]["warning"] == 1


def test_runtime_decision_normalizer_adapts_existing_decision_evidence() -> None:
    raw = SimpleNamespace(
        to_evidence=lambda: {
            "schema_version": "custom.v1",
            "requested_backend": "auto",
            "selected_backend": "client",
            "fallback_reason": "direct_ingest_provider_missing",
            "release_gate": "warning",
            "warnings": ["direct_ingest_provider_missing"],
            "blockers": [],
            "input_format": "Native",
        }
    )

    decision = normalize_runtime_decision(
        raw,
        decision_id="clickhouse.direct_ingest",
        phase="load",
        component="clickhouse_sink",
        category="backend_selection",
        fallback_allowed=True,
        details={"route_certified": False},
    )

    assert decision.requested == "auto"
    assert decision.selected == "client"
    assert decision.fallback_reason == "direct_ingest_provider_missing"
    assert decision.details["input_format"] == "Native"
    assert decision.details["route_certified"] is False


def test_direct_ingest_auto_fallback_is_published_before_client_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(__import__("sys").modules, "dpone_native_accel", None)
    audit = InMemoryLoadStepAuditStorage()
    summary = RuntimeDecisionSummary()
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=_load_record(),
        logger=_DecisionLogger(),
        summary=summary,
    )
    artifact = ByteStreamArtifact(
        lambda: [b"native-block"], columns=["id"], format="clickhouse-native", estimated_rows=1
    )
    artifact.bulk_wire_contract = _bulk_wire_contract(route_certified=True)

    with RuntimeDecisionContext.activate(publisher):
        inserted = _FakeBulkSink()._insert_stream_with_client(
            _native_tcp_load_config(backend="auto"),
            artifact,
            [("id", "Int32")],
        )

    assert inserted == 1
    direct_records = [record for record in audit.records if record.step_id == "clickhouse.direct_ingest"]
    assert len(direct_records) == 1
    details = direct_records[0].details
    assert details["selected"] == "client"
    assert details["fallback_allowed"] is True
    assert details["fallback_reason"] == "direct_ingest_provider_missing"
    assert details["details"]["input_format"] == "Native"
    assert summary.to_jsonable()["decisions"][0]["decision_id"] == "clickhouse.direct_ingest"


def test_forced_direct_ingest_blocker_is_published_before_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(__import__("sys").modules, "dpone_native_accel", _uncertified_direct_provider())
    audit = InMemoryLoadStepAuditStorage()
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=_load_record(),
        logger=_DecisionLogger(),
        summary=RuntimeDecisionSummary(),
    )
    artifact = ByteStreamArtifact(
        lambda: [b"native-block"], columns=["id"], format="clickhouse-native", estimated_rows=1
    )
    artifact.bulk_wire_contract = _bulk_wire_contract(route_certified=True)

    with RuntimeDecisionContext.activate(publisher), pytest.raises(RuntimeError, match="direct_ingest_required"):
        _FakeBulkSink()._insert_stream_with_client(
            _native_tcp_load_config(backend="direct"),
            artifact,
            [("id", "Int32")],
        )

    details = audit.records[0].details
    assert details["selected"] is None
    assert details["release_gate"] == "blocked"
    assert "direct_ingest_required_unavailable" in details["blockers"]


def test_etl_processor_returns_runtime_decision_summary_without_audit_storage() -> None:
    result = ETLProcessor(_DecisionSource(), _DecisionSink(), etl_logger=_DecisionLogger()).run(_etl_load_config())

    summary = result["runtime_decisions"]

    assert summary["schema_version"] == "dpone.runtime.decision_audit.v1"
    assert summary["total"] == 2
    assert summary["by_release_gate"]["warning"] == 2
    by_id = {item["decision_id"]: item for item in summary["decisions"]}
    assert by_id["load_governance.finalization"]["fallback_reason"] == "staged_load_port_unavailable"
    assert by_id["test.runtime_branch"]["fallback_reason"] == "test_auto_fallback"


def test_failed_load_persists_snapshot_cleanup_inside_decision_context(monkeypatch: pytest.MonkeyPatch) -> None:
    audit = InMemoryLoadStepAuditStorage()
    primary = RuntimeError("target load failed")
    artifact = PreparedSourceArtifact(
        InMemoryRowsArtifact([{"id": 1}]),
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_test]",
            evidence={},
            cleanup=lambda: SourceMaterializationCleanupResult(status="deferred", reason="mssql_cleanup_lock_timeout"),
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green", provider="mssql_work_table"),
        cleanup_policy="eager",
    )
    source = _DecisionSource()
    sink = _DecisionSink()
    monkeypatch.setattr(source, "extract", lambda *args: ExtractResult(artifact=artifact, schema=[("id", "bigint")]))

    def fail(*args, **kwargs):
        raise primary

    monkeypatch.setattr(sink, "load", fail)
    processor = ETLProcessor(
        source,
        sink,
        etl_logger=_DecisionLogger(),
        load_governance_service=LoadGovernanceService(audit_storage=audit),
    )

    with pytest.raises(RuntimeError) as caught:
        processor.run(_etl_load_config())

    assert caught.value is primary
    cleanup = [record for record in audit.records if record.step_id == "source_materialization.cleanup"]
    assert len(cleanup) == 1
    assert cleanup[0].details["release_gate"] == "warning"
    assert cleanup[0].details["details"]["cleanup_result"]["status"] == "deferred"


def test_manifest_schemas_expose_runtime_decision_audit_policy() -> None:
    for relative in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        policy = _find_decision_audit_properties(schema)

        assert policy["enabled"]["default"] is True
        assert policy["persist"]["default"] is True
        assert policy["log_level"]["enum"] == ["always", "warning_on_fallback", "error_only"]
        assert policy["include_successful_decisions"]["default"] is True


@dataclass
class _LoadRecord:
    run_id: str = "run-1"
    load_id: str = "load-1"


class _DecisionLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs
        self.infos.append(message)

    def warning(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs
        self.warnings.append(message)

    def log_etl_start(self, payload):  # noqa: ANN001
        del payload

    def log_etl_progress(self, event, payload):  # noqa: ANN001
        del event, payload

    def log_etl_error(self, message, payload):  # noqa: ANN001
        del message, payload

    def log_etl_end(self, payload):  # noqa: ANN001
        del payload


class _FakeClientRunner:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def insert_stream(self, table, columns, chunks):  # noqa: ANN001
        assert table == "landing.orders"
        assert tuple(columns) == ("id",)
        assert b"".join(chunks) == b"native-block"


class _FakeBulkSink(ClickHouseBulkMixin):
    _client_runner_cls = _FakeClientRunner
    _http_runner_cls = object
    connector = SimpleNamespace(
        host="clickhouse.local",
        port=8123,
        database="default",
        user="default",
        password="secret",
        secure=False,
    )

    @staticmethod
    def _table(load_config):  # noqa: ANN001
        return f"{load_config.target_schema}.{load_config.target_table}"

    @staticmethod
    def _count(load_config):  # noqa: ANN001
        del load_config
        return 1


def _load_record() -> _LoadRecord:
    return _LoadRecord()


def _bulk_wire_contract(*, route_certified: bool) -> SimpleNamespace:
    return SimpleNamespace(
        input_format="Native",
        route_certified=route_certified,
        delimiter_profile=SimpleNamespace(clickhouse_settings={}),
    )


def _native_tcp_load_config(*, backend: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "clickhouse_bulk": {
                "mode": "native_tcp",
                "native_tcp": {"enabled": True, "backend": backend, "compression": "lz4"},
            }
        },
    )


def _uncertified_direct_provider() -> SimpleNamespace:
    return SimpleNamespace(
        direct_ingest_capabilities=lambda: {
            "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "clickhouse_native_tcp_direct",
                    "sink": "clickhouse",
                    "input_format": "Native",
                    "certified": False,
                }
            ],
        },
        insert_clickhouse_native=lambda request: {"rows": 1},
    )


def _find_decision_audit_properties(schema: dict[str, object]) -> dict[str, object]:
    stack = [schema]
    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        properties = current.get("properties")
        if isinstance(properties, dict) and "load_governance" in properties:
            governance = properties["load_governance"]
            if isinstance(governance, dict):
                decision_audit = governance.get("properties", {}).get("decision_audit", {})
                if isinstance(decision_audit, dict):
                    return decision_audit.get("properties", {})
        stack.extend(value for value in current.values() if isinstance(value, dict))
        stack.extend(
            item for value in current.values() if isinstance(value, list) for item in value if isinstance(item, dict)
        )
    raise AssertionError("load_governance.decision_audit schema not found")


class _DecisionSource:
    def get_incremental_state(self, load_config):  # noqa: ANN001
        del load_config
        return None

    def extract(self, load_config, last_state):  # noqa: ANN001
        del load_config, last_state
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        )


class _DecisionSink:
    def load(self, load_config, payload):  # noqa: ANN001
        del load_config
        publish_runtime_decision(
            {
                "requested_backend": "auto",
                "selected_backend": "fallback_backend",
                "fallback_reason": "test_auto_fallback",
                "release_gate": "warning",
                "warnings": ["test_auto_fallback"],
            },
            decision_id="test.runtime_branch",
            phase="load",
            component="test_sink",
            category="backend_selection",
            fallback_allowed=True,
        )
        return LoadResult(
            inserted_rows=len(payload.artifact._rows),
            updated_rows=0,
            total_rows=len(payload.artifact._rows),
            staging_rows=len(payload.artifact._rows),
        )


def _etl_load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False, "load_governance": {"audit": {"enabled": False}}},
    )
