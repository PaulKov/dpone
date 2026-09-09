from __future__ import annotations

from types import SimpleNamespace

from dpone.config.load_config import LoadConfig
from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.decision_audit import (
    CompositeDecisionPublisher,
    DecisionAuditPolicy,
    RuntimeDecision,
    RuntimeDecisionContext,
    RuntimeDecisionSummary,
)
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.sources.strategies.mssql.mssql_export_optimizer_runtime import resolve_mssql_export_provider


def test_persist_disabled_keeps_logs_and_summary_without_db_audit() -> None:
    audit = InMemoryLoadStepAuditStorage()
    logger = _DecisionLogger()
    summary = RuntimeDecisionSummary()
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(persist=False),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=_load_record(),
        logger=logger,
        summary=summary,
    )

    publisher.publish(
        RuntimeDecision(
            decision_id="test.persist_disabled",
            phase="load",
            component="unit",
            category="backend_selection",
            requested="auto",
            selected="file",
            fallback_allowed=True,
            fallback_reason="stream_not_available",
            release_gate="warning",
            warnings=("stream_not_available",),
        )
    )

    assert audit.records == []
    assert logger.warnings
    assert summary.to_jsonable()["decisions"][0]["selected"] == "file"


def test_mssql_export_optimizer_decision_is_published_from_probe_evidence() -> None:
    audit = InMemoryLoadStepAuditStorage()
    publisher = CompositeDecisionPublisher(
        policy=DecisionAuditPolicy(),
        governance_service=LoadGovernanceService(audit_storage=audit),
        load_record=_load_record(),
        logger=_DecisionLogger(),
        summary=RuntimeDecisionSummary(),
    )
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        options={
            "native_transfer": {
                "snapshot": {
                    "export_optimizer": {"mode": "auto", "min_speedup_pct": 15},
                    "export_optimizer_probes": [
                        {"provider_id": "mssql_bcp_native", "rows_per_second": 100_000},
                        {"provider_id": "mssql_odbc_array", "rows_per_second": 160_000},
                    ],
                }
            }
        },
    )

    with RuntimeDecisionContext.activate(publisher):
        selected = resolve_mssql_export_provider(
            connector=object(),
            load_config=load_config,
            query="select id from dbo.orders",
            schema=[("id", "int")],
            current_default="mssql_bcp_native",
        )

    assert selected == "mssql_odbc_array"
    records = [record for record in audit.records if record.step_id == "source_export.optimizer"]
    assert len(records) == 1
    details = records[0].details
    assert details["requested"] == "auto"
    assert details["selected"] == "mssql_odbc_array"
    assert details["details"]["current_default"] == "mssql_bcp_native"


class _DecisionLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, message: str, *_args: object, **_kwargs: object) -> None:
        self.infos.append(message)

    def warning(self, message: str, *_args: object, **_kwargs: object) -> None:
        self.warnings.append(message)


def _load_record() -> SimpleNamespace:
    return SimpleNamespace(run_id="run-1", load_id="load-1")
