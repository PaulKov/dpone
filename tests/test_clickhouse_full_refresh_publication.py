from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.sinks.clickhouse_full_refresh_catalog import ClickHousePublicationTable
from dpone.runtime.sinks.clickhouse_full_refresh_contract import (
    ClickHouseFullRefreshPublicationError,
    publication_invocation_id,
    publication_marker_name,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import (
    SCHEDULER_IDENTITY_OPTION,
    ClickHouseFullRefreshOutcomeUnknown,
    ClickHouseFullRefreshPublicationService,
)

_OLD = "11111111-1111-1111-1111-111111111111"
_NEW = "22222222-2222-2222-2222-222222222222"


class _Catalog:
    def __init__(self, *, target: bool = True) -> None:
        self.engine = "Atomic"
        self.records: dict[str, ClickHousePublicationTable] = {
            "candidate": self._table("candidate", _NEW),
        }
        if target:
            self.records["target"] = self._table("target", _OLD)
        self.counts = {"target": 9}
        self.exchange_calls = 0
        self.rename_calls = 0
        self.fail_exchange: str | None = None
        self.fail_rename: str | None = None
        self.fail_drop_table: str | None = None
        self.fail_quiescence = False
        self.active_query_ids: set[str] = set()
        self.last_query_id: str | None = None

    @staticmethod
    def _table(name: str, uuid: str, *, engine: str = "MergeTree", comment: str = "") -> ClickHousePublicationTable:
        return ClickHousePublicationTable(name=name, uuid=uuid, engine=engine, comment=comment)

    def database_engine(self, database: str) -> str | None:
        assert database == "analytics"
        return self.engine

    def tables(self, database: str, names: tuple[str, ...]) -> dict[str, ClickHousePublicationTable]:
        assert database == "analytics"
        return {name: self.records[name] for name in names if name in self.records}

    def create_marker(self, database: str, marker: str, comment: str) -> None:
        assert database == "analytics"
        if marker in self.records:
            raise RuntimeError("TABLE_ALREADY_EXISTS")
        self.records[marker] = self._table(
            marker, "33333333-3333-3333-3333-333333333333", engine="TinyLog", comment=comment
        )

    def exchange(self, database: str, target: str, candidate: str, *, query_id: str) -> None:
        assert database == "analytics"
        self.exchange_calls += 1
        self.last_query_id = query_id
        if self.fail_exchange == "inflight":
            self.active_query_ids.add(query_id)
            raise RuntimeError("reply lost while server query remains active")
        if self.fail_exchange == "before":
            raise RuntimeError("reply lost before mutation")
        old, new = self.records[target], self.records[candidate]
        self.records[target] = replace(new, name=target)
        self.records[candidate] = replace(old, name=candidate)
        if self.fail_exchange == "after":
            raise RuntimeError("reply lost after mutation")

    def rename(self, database: str, candidate: str, target: str, *, query_id: str) -> None:
        assert database == "analytics"
        self.rename_calls += 1
        self.last_query_id = query_id
        if self.fail_rename == "before":
            raise RuntimeError("reply lost before mutation")
        self.records[target] = replace(self.records.pop(candidate), name=target)
        if self.fail_rename == "after":
            raise RuntimeError("reply lost after mutation")

    def drop(self, database: str, table: str) -> None:
        assert database == "analytics"
        if self.fail_drop_table == table:
            self.fail_drop_table = None
            raise RuntimeError("injected drop failure")
        del self.records[table]

    def publication_query_active(self, query_id: str) -> bool:
        if self.fail_quiescence:
            raise RuntimeError("system.processes unavailable")
        return query_id in self.active_query_ids

    def complete_inflight_exchange(self) -> None:
        assert self.last_query_id is not None
        old, new = self.records["target"], self.records["candidate"]
        self.records["target"] = replace(new, name="target")
        self.records["candidate"] = replace(old, name="candidate")
        self.active_query_ids.remove(self.last_query_id)

    def count(self, database: str, table: str) -> int:
        assert database == "analytics"
        return self.counts[table]


def _config(*, run_id: str = "scheduled__2026-01-01", cluster: bool = False) -> LoadConfig:
    options: dict[str, Any] = {
        "run_id": run_id,
        SOURCE_BYTE_BUDGET_OPTION: 1024,
        SCHEDULER_IDENTITY_OPTION: publication_invocation_id(
            scheduler_run_id="scheduled__stable",
            process_id="workflow",
        ),
    }
    if cluster:
        options["physical_design"] = {
            "storage": {"clickhouse": {"cluster": {"name": "analytics_cluster", "on_cluster": True}}}
        }
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_model",
        target_schema="analytics",
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def _candidate(config: LoadConfig) -> LoadConfig:
    return replace(config, target_table="candidate")


def test_existing_target_is_exchanged_once_and_cleaned_by_exact_uuid() -> None:
    catalog = _Catalog()
    service = ClickHouseFullRefreshPublicationService(catalog)

    receipt = service.publish(_config(), _candidate(_config()), staged_rows=9)

    marker_table = publication_marker_name("target")
    assert catalog.exchange_calls == 1
    assert catalog.records["target"].uuid == _NEW
    assert catalog.records["candidate"].uuid == _OLD
    assert marker_table in catalog.records

    service.cleanup(receipt)

    assert "candidate" not in catalog.records
    assert marker_table not in catalog.records


def test_lost_exchange_reply_is_reconciled_without_second_exchange() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "after"
    service = ClickHouseFullRefreshPublicationService(catalog)

    receipt = service.publish(_config(), _candidate(_config()), staged_rows=9)

    assert receipt.recovered_after_error is True
    assert catalog.exchange_calls == 1
    assert catalog.records["target"].uuid == _NEW


def test_uncommitted_exchange_error_is_not_retried_in_same_attempt() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "before"
    service = ClickHouseFullRefreshPublicationService(catalog)

    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown, match="completion is not proven"):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    assert catalog.exchange_calls == 1
    assert catalog.records["target"].uuid == _OLD
    assert publication_marker_name("target") in catalog.records


def test_same_run_retry_resumes_pending_marker_before_source_io() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "before"
    service = ClickHouseFullRefreshPublicationService(catalog)
    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    catalog.fail_exchange = None
    admitted = service.prepare_admission(_config())
    replay = service.replay_result(admitted)

    assert replay is not None
    assert replay.inserted_rows == 9
    assert replay.total_rows == 9
    assert replay.commit_outcome is not None
    assert catalog.exchange_calls == 2
    assert set(catalog.records) == {"target"}


def test_retry_waits_for_ambiguous_server_query_then_reconciles_without_second_exchange() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "inflight"
    service = ClickHouseFullRefreshPublicationService(catalog)
    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown, match="completion is not proven"):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown, match="still active"):
        service.prepare_admission(_config())
    assert catalog.exchange_calls == 1

    catalog.complete_inflight_exchange()
    admitted = service.prepare_admission(_config())

    assert service.replay_result(admitted) is not None
    assert catalog.exchange_calls == 1
    assert set(catalog.records) == {"target"}


def test_retry_fails_closed_when_server_quiescence_cannot_be_observed() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "before"
    service = ClickHouseFullRefreshPublicationService(catalog)
    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown):
        service.publish(_config(), _candidate(_config()), staged_rows=9)
    catalog.fail_quiescence = True

    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown, match="cannot prove"):
        service.prepare_admission(_config())

    assert catalog.exchange_calls == 1


def test_different_run_cannot_take_over_unresolved_marker() -> None:
    catalog = _Catalog()
    catalog.fail_exchange = "before"
    service = ClickHouseFullRefreshPublicationService(catalog)
    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    different = replace(
        _config(run_id="manual__different"),
        options={
            **_config().options,
            SCHEDULER_IDENTITY_OPTION: publication_invocation_id(
                scheduler_run_id="manual__different",
                process_id="workflow",
            ),
        },
    )
    with pytest.raises(ClickHouseFullRefreshPublicationError, match="another run owns"):
        service.prepare_admission(different)

    assert catalog.exchange_calls == 1


def test_absent_target_lost_rename_reply_is_reconciled() -> None:
    catalog = _Catalog(target=False)
    catalog.fail_rename = "after"
    service = ClickHouseFullRefreshPublicationService(catalog)

    receipt = service.publish(_config(), _candidate(_config()), staged_rows=9)

    assert receipt.recovered_after_error is True
    assert catalog.rename_calls == 1
    assert catalog.records["target"].uuid == _NEW
    service.cleanup(receipt)
    assert set(catalog.records) == {"target"}


@pytest.mark.parametrize("engine", ["Ordinary", "Replicated"])
def test_unsupported_database_engine_blocks_before_marker(engine: str) -> None:
    catalog = _Catalog()
    catalog.engine = engine
    service = ClickHouseFullRefreshPublicationService(catalog)

    with pytest.raises(ClickHouseFullRefreshPublicationError, match="ENGINE_UNSUPPORTED"):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    assert catalog.exchange_calls == 0
    assert publication_marker_name("target") not in catalog.records


def test_cluster_topology_blocks_before_catalog_mutation() -> None:
    catalog = _Catalog()
    service = ClickHouseFullRefreshPublicationService(catalog)

    with pytest.raises(ClickHouseFullRefreshPublicationError, match="TOPOLOGY_UNSUPPORTED"):
        service.publish(_config(cluster=True), _candidate(_config(cluster=True)), staged_rows=9)

    assert catalog.exchange_calls == 0


@pytest.mark.parametrize("engine", ["Distributed", "ReplicatedMergeTree"])
def test_unsupported_table_engine_blocks_before_marker(engine: str) -> None:
    catalog = _Catalog()
    catalog.records["target"] = catalog._table("target", _OLD, engine=engine)
    service = ClickHouseFullRefreshPublicationService(catalog)

    with pytest.raises(ClickHouseFullRefreshPublicationError, match="TOPOLOGY_UNSUPPORTED"):
        service.publish(_config(), _candidate(_config()), staged_rows=9)

    assert catalog.exchange_calls == 0


def test_cleanup_rejects_replaced_predecessor() -> None:
    catalog = _Catalog()
    service = ClickHouseFullRefreshPublicationService(catalog)
    receipt = service.publish(_config(), _candidate(_config()), staged_rows=9)
    catalog.records["candidate"] = catalog._table("candidate", "44444444-4444-4444-4444-444444444444")

    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown, match="cleanup requires committed"):
        service.cleanup(receipt)

    assert publication_marker_name("target") in catalog.records


def test_cleanup_retry_removes_marker_after_predecessor_was_already_dropped() -> None:
    catalog = _Catalog()
    service = ClickHouseFullRefreshPublicationService(catalog)
    receipt = service.publish(_config(), _candidate(_config()), staged_rows=9)
    marker_table = publication_marker_name("target")
    catalog.fail_drop_table = marker_table

    with pytest.raises(RuntimeError, match="injected drop failure"):
        service.cleanup(receipt)

    assert "candidate" not in catalog.records
    assert marker_table in catalog.records
    service.cleanup(receipt)
    assert set(catalog.records) == {"target"}


def test_marker_name_is_stable_and_bounded() -> None:
    assert publication_marker_name("target") == publication_marker_name("target")
    assert len(publication_marker_name("x" * 500)) <= 255


def test_non_full_refresh_admission_is_inert() -> None:
    catalog = _Catalog()
    service = ClickHouseFullRefreshPublicationService(catalog)
    config = replace(_config(), load_strategy=LoadStrategy.INCREMENTAL_APPEND)

    assert service.prepare_admission(config) is config
    assert catalog.exchange_calls == 0


def test_runtime_hook_delegates_prepare_and_replay() -> None:
    from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices

    replay = SimpleNamespace(total_rows=9)
    sink = SimpleNamespace(
        prepare_runtime_admission=lambda config, **_: replace(config, options={**config.options, "prepared": True}),
        replay_result=lambda config: replay if config.options.get("prepared") else None,
    )
    mssql = SimpleNamespace(prepare=lambda config, **_: config, replay_result=lambda config: None)
    runtime = ProcessorRuntimeServices(
        source=object(),
        sink=sink,
        source_state_service=object(),
        payload_load_service=object(),
        mssql_transaction_admission_service=mssql,
    )

    prepared = runtime.prepare_admission(_config(), run_context=object(), load_record=object(), dag_id="workflow")

    assert prepared.options["prepared"] is True
    assert runtime.replay_result(prepared) is replay


def test_runtime_identity_survives_fresh_audit_records_and_resumes_before_source_io() -> None:
    from dpone.contracts.run_context import RunContext
    from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink

    catalog = _Catalog()
    catalog.fail_exchange = "before"
    service = ClickHouseFullRefreshPublicationService(catalog)
    sink = SimpleNamespace(_full_refresh_publication=service)
    context = RunContext(run_id="scheduled__stable")

    first = ClickHouseSink.prepare_runtime_admission(
        sink,
        _config(run_id="audit-attempt-1"),
        run_context=context,
        load_record=SimpleNamespace(run_id="audit-attempt-1"),
        dag_id="workflow",
    )
    with pytest.raises(ClickHouseFullRefreshOutcomeUnknown):
        service.publish(first, _candidate(first), staged_rows=9)

    catalog.fail_exchange = None
    second = ClickHouseSink.prepare_runtime_admission(
        sink,
        _config(run_id="audit-attempt-2"),
        run_context=context,
        load_record=SimpleNamespace(run_id="audit-attempt-2"),
        dag_id="workflow",
    )

    assert service.replay_result(second) is not None
    assert catalog.exchange_calls == 2
    assert set(catalog.records) == {"target"}
