"""Production-runner CLI/Python parity contracts for the documented external route."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.contracts.clickhouse_external_replication import ExternalPublicationError
from dpone.ports.clickhouse_external_replication import ExternalReplicationReceipt
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sinks.load_result import AtomicCommitOutcome

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTED_MANIFEST = ROOT / "examples/batch/clickhouse-external-replication-full-refresh.batch.yaml"
RECEIPT = ExternalReplicationReceipt(
    target_key="1" * 64,
    operation_id="8" * 64,
    generation_id="2" * 64,
    inventory_digest="3" * 64,
    plan_digest="4" * 64,
    artifact_sha256="5" * 64,
    member_ids=("6" * 64, "7" * 64),
    authority_version=3,
    phase="COMMITTED",
).to_dict()


class StubLogger:
    def log_etl_start(self, payload: Any) -> None:
        del payload

    def log_etl_progress(self, event: str, payload: Any) -> None:
        del event, payload

    def info(self, message: str) -> None:
        del message

    def warning(self, message: str) -> None:
        del message

    def log_etl_error(self, message: str, payload: Any) -> None:
        del message, payload

    def log_etl_end(self, payload: Any) -> None:
        del payload


class StubSource:
    def __init__(self, events: list[str]) -> None:
        self.connector = SimpleNamespace()
        self._events = events

    def get_incremental_state(self, load_config: LoadConfig) -> None:
        del load_config
        return None

    def extract(self, load_config: LoadConfig, state: Any) -> Any:
        del load_config, state
        return SimpleNamespace(
            artifact=SimpleNamespace(column_timezone=None, row_count=2),
            schema=[],
            state=None,
            force_full_refresh=False,
        )

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        del load_config, state
        self._events.append("source_checkpoint")


class ExternalSink:
    def __init__(self, events: list[str], *, fail: bool) -> None:
        self.connector = SimpleNamespace()
        self.events = events
        self._fail = fail

    def prepare_runtime_admission(self, load_config: LoadConfig, **kwargs: Any) -> LoadConfig:
        del kwargs
        cluster = load_config.options["physical_design"]["storage"]["clickhouse"]["cluster"]
        assert cluster["replication_mode"] == "external"
        assert cluster["ddl_scope"] == "cluster"
        self.events.append("runtime_admission")
        return load_config

    def load(self, load_config: LoadConfig, payload: Any) -> LoadResult:
        del load_config, payload
        if self._fail:
            self.events.append("publication_partial")
            raise ExternalPublicationError(
                "PUBLICATION_PARTIAL_TERMINAL",
                evidence={
                    "phase": "PUBLICATION_DISPATCHING",
                    "recovery": "reconcile the operation receipt before retry",
                },
            )
        self.events.append("publication_receipt_durable")
        return LoadResult(
            inserted_rows=2,
            updated_rows=0,
            total_rows=2,
            staging_rows=2,
            commit_receipt_id=str(RECEIPT["operation_id"]),
            commit_outcome=AtomicCommitOutcome.COMMITTED,
            reconciliation_metrics={"clickhouse_cluster_external_full_refresh": dict(RECEIPT)},
        )

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        del load_config, state
        self.events.append("source_checkpoint")


class ObservedIdentity(LoadIdentityService):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    def mark_committed(self, record: Any, load_result: LoadResult) -> Any:
        metrics = load_result.reconciliation_metrics or {}
        receipt = metrics["clickhouse_cluster_external_full_refresh"]
        assert all(receipt[field] == value for field, value in RECEIPT.items())
        self._events.append("target_commit_callback")
        return super().mark_committed(record, load_result)

    def mark_failed(self, record: Any, error: BaseException) -> Any:
        self._events.append("target_failed_callback")
        return super().mark_failed(record, error)


class Hydrator:
    def __init__(self, *, fail: bool) -> None:
        self._fail = fail
        self.events_by_run: list[list[str]] = []
        self.normalized_routes: list[dict[str, Any]] = []

    def build(self, *, config: dict[str, Any], load_config: LoadConfig) -> RuntimeBindings:
        del config
        cluster = load_config.options["physical_design"]["storage"]["clickhouse"]["cluster"]
        self.normalized_routes.append(
            {
                "source": load_config.options["source_type"],
                "sink": load_config.options["sink_type"],
                "strategy": load_config.load_strategy.value,
                "target": f"{load_config.target_schema}.{load_config.target_table}",
                "cluster": dict(cluster),
            }
        )
        events: list[str] = []
        self.events_by_run.append(events)
        return RuntimeBindings(
            source_obj=StubSource(events),
            sink_obj=ExternalSink(events, fail=self._fail),
            etl_logger=StubLogger(),
            load_identity_service=ObservedIdentity(events),
        )


class ObservedRouteFactory:
    def __init__(self) -> None:
        self._delegate = RouteCapabilityRuntimeFactory()
        self.routes: list[tuple[str, str, str]] = []

    def build(self, *, load_config: LoadConfig, source: Any, sink: Any, logger: Any) -> Any:
        self.routes.append(
            (
                load_config.options["source_type"],
                load_config.options["sink_type"],
                load_config.load_strategy.value,
            )
        )
        return self._delegate.build(load_config=load_config, source=source, sink=sink, logger=logger)


def test_documented_external_route_has_default_runner_cli_python_parity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone import api
    from dpone.commands import run_cmd

    hydrator, route_factory = _install_runtime(monkeypatch, fail=False)
    args = _run_args(run_id="external-cluster-full-refresh-parity")

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    python_payload = api.run(DOCUMENTED_MANIFEST, run_id=args.run_id).to_dict()

    for field in ("status", "inserted_rows", "updated_rows", "final_rows", "extracted_rows", "errors"):
        assert cli_payload["result"][field] == python_payload["result"][field]
    assert {
        field: python_payload["result"][field]
        for field in ("status", "inserted_rows", "updated_rows", "final_rows", "extracted_rows", "errors")
    } == {
        "status": "success",
        "inserted_rows": 2,
        "updated_rows": 0,
        "final_rows": 2,
        "extracted_rows": 2,
        "errors": [],
    }
    for payload in (cli_payload, python_payload):
        assert payload["run_id"] == "external-cluster-full-refresh-parity"
        assert payload["process"] == "external_cluster_full_refresh"
        assert payload["selector"] == "source_schema.source_table"
    assert hydrator.events_by_run == [
        ["runtime_admission", "publication_receipt_durable", "target_commit_callback"],
        ["runtime_admission", "publication_receipt_durable", "target_commit_callback"],
    ]
    assert hydrator.normalized_routes == [hydrator.normalized_routes[0], hydrator.normalized_routes[0]]
    assert hydrator.normalized_routes[0] == {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "full_refresh",
        "target": "analytics.target_table",
        "cluster": {
            "name": "analytics_cluster",
            "ddl_scope": "cluster",
            "replication_mode": "external",
            "external_content_row_budget": 100_000,
        },
    }
    assert route_factory.routes == [("mssql", "clickhouse", "full_refresh")] * 2
    for payload in (cli_payload, python_payload):
        metrics = payload["result"]["details"]["reconciliation_metrics"]
        receipt = metrics["clickhouse_cluster_external_full_refresh"]
        assert all(receipt[field] == value for field, value in RECEIPT.items())
        quality = metrics["quality_gates"]
        assert quality["passed"] is True
        assert quality["gate_contract"] == [
            {"gate_id": "source_target_rows", "type": "row_count_reconciliation", "severity": "error"}
        ]
        assert quality["results"][0]["status"] == "passed"
        assert quality["results"][0]["metrics"] == {
            "source_row_count": 2,
            "target_row_count": 2,
            "difference": 0,
            "allowed_difference": 0.0,
        }


def test_external_route_default_runner_failure_never_commits_or_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone import api
    from dpone.commands import run_cmd

    hydrator, route_factory = _install_runtime(monkeypatch, fail=True)
    args = _run_args(run_id="external-cluster-full-refresh-failure")

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 1
    cli_error = json.loads(capsys.readouterr().err)
    with pytest.raises(ExternalPublicationError) as raised:
        api.run(DOCUMENTED_MANIFEST, run_id=args.run_id)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL"
    assert cli_error["result"]["error_code"] == raised.value.code
    assert cli_error["result"]["evidence"] == raised.value.evidence
    assert route_factory.routes == [("mssql", "clickhouse", "full_refresh")] * 2
    assert hydrator.events_by_run == [
        ["runtime_admission", "publication_partial", "target_failed_callback"],
        ["runtime_admission", "publication_partial", "target_failed_callback"],
    ]
    assert all(
        "target_commit_callback" not in events and "source_checkpoint" not in events
        for events in hydrator.events_by_run
    )


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail: bool,
) -> tuple[Hydrator, ObservedRouteFactory]:
    from dpone.ports import process_runner, runtime_hydrator

    hydrator = Hydrator(fail=fail)
    route_factory = ObservedRouteFactory()
    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", hydrator)
    monkeypatch.setattr(
        process_runner,
        "_PROCESS_RUNNER",
        DefaultProcessRunner(route_capability_factory=route_factory),
    )
    return hydrator, route_factory


def _run_args(*, run_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        path=DOCUMENTED_MANIFEST,
        selector=None,
        run_id=run_id,
        dag_id=None,
        execution_date=None,
        interval_start=None,
        interval_end=None,
        retry_attempts=0,
        retry_backoff_seconds=0.0,
        format="json",
        registry=[],
        sample=None,
        target=None,
        select=[],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=10,
    )
