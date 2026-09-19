"""Pinned live acceptance for external-replication publication adapters."""

from __future__ import annotations

import os
import secrets

import pytest

from tests.integration.clickhouse_cluster.evidence import record_external_scenario
from tests.integration.clickhouse_cluster.external_replication_live_support import (
    CLUSTER as _CLUSTER,
)
from tests.integration.clickhouse_cluster.external_replication_live_support import (
    docker_member_endpoint as _docker_member_endpoint,
)
from tests.integration.clickhouse_cluster.external_replication_live_support import execute as _execute
from tests.integration.clickhouse_cluster.external_replication_live_support import publication_case as _publication_case

pytestmark = pytest.mark.integration_live


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_stages_each_member_and_fresh_service_cleans_exact_predecessors() -> None:
    from dpone.config.load_config import LoadConfig
    from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
    from dpone.runtime.artifacts import InMemoryRowsArtifact
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
    from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
    from dpone.runtime.sinks.load_payload import LoadPayload

    database = f"external_publication_{secrets.token_hex(4)}"
    _execute(18123, f"CREATE DATABASE {database} ON CLUSTER `{_CLUSTER}` ENGINE=Atomic")
    for port in (18123, 28123):
        _execute(port, f"CREATE TABLE {database}.target (id Int64) ENGINE=MergeTree ORDER BY id")
        _execute(port, f"INSERT INTO {database}.target VALUES (1)")
    connector = ClickHouseConnector(
        host="127.0.0.1",
        port=18123,
        database=database,
        user="default",
        password="",
        driver="http",
        external_member_endpoint_resolver=_docker_member_endpoint,
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="source_table",
        target_schema=database,
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
        staging_schema=database,
        options={
            SCHEDULER_IDENTITY_OPTION: "docker-external-publication",
            SOURCE_BYTE_BUDGET_OPTION: 1024 * 1024,
            "external_artifact_store_path": "/tmp/dpone-external-artifacts-docker",
            "lineage": False,
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "MergeTree",
                        "order_by": ["id"],
                        "cluster": {
                            "name": _CLUSTER,
                            "ddl_scope": "cluster",
                            "replication_mode": "external",
                            "external_content_row_budget": 100,
                        },
                    }
                }
            },
        },
    )
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 10}, {"id": 20}]),
        schema=(("id", "bigint"),),
    )
    sink = ClickHouseSink(connector)
    admitted = sink._full_refresh_publication.prepare_admission(config)
    sink.preflight_before_extract(load_config=admitted)
    result = sink.load(admitted, payload)
    receipt = (result.reconciliation_metrics or {})["clickhouse_cluster_external_full_refresh"]

    assert result.total_rows == 2
    assert receipt["phase"] == "COMMITTED"
    assert receipt["evidence_scope"] == "runtime"
    assert receipt["evidence_status"] == "UNVERIFIED"
    assert len(receipt["member_ids"]) == 2
    for port in (18123, 28123):
        assert _execute(port, f"SELECT groupArray(id) FROM {database}.target") == [("[10,20]",)]
        assert _execute(
            port,
            f"SELECT count() FROM system.tables WHERE database='{database}' AND name LIKE 'target__dpone_ext_%'",
        ) == [("0",)]
    record_external_scenario(
        "external_replication_fresh_cleanup",
        "PASS",
        server_version=_execute(18123, "SELECT version()")[0][0],
        details={
            "operation_id": result.commit_receipt_id,
            "member_count": len(receipt["member_ids"]),
            "production_composition": True,
        },
    )


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_publishes_verified_empty_generation() -> None:
    sink, config, payload, database = _publication_case("empty", rows=[])

    admitted = sink._full_refresh_publication.prepare_admission(config)
    sink.preflight_before_extract(load_config=admitted)
    result = sink.load(admitted, payload)
    receipt = (result.reconciliation_metrics or {})["clickhouse_cluster_external_full_refresh"]

    assert result.total_rows == 0
    assert result.staging_rows == 0
    assert result.commit_receipt_id
    assert receipt["phase"] == "COMMITTED"
    assert receipt["evidence_scope"] == "runtime"
    assert receipt["evidence_status"] == "UNVERIFIED"
    for port in (18123, 28123):
        assert _execute(port, f"SELECT count() FROM {database}.target") == [("0",)]
        assert _execute(
            port,
            f"SELECT count() FROM system.tables WHERE database='{database}' AND name LIKE 'target__dpone_ext_%'",
        ) == [("0",)]
    record_external_scenario(
        "external_replication_empty_generation",
        "PASS",
        server_version=_execute(18123, "SELECT version()")[0][0],
        details={"operation_id": result.commit_receipt_id, "row_count": 0},
    )


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_reconciles_lost_responses_and_staging_restart() -> None:
    sink, config, payload, database = _publication_case("faults")
    facade = sink._full_refresh_publication._external
    original_factory = facade._service_factory
    injected = {"stage": False, "publication": False, "cleanup": False}

    def faulting_factory(*args: object, **kwargs: object) -> object:
        adapter = original_factory(*args, **kwargs)
        original_stage = adapter.stage_member_once
        original_publication = adapter.dispatch_publication_once
        original_cleanup = adapter.dispatch_cleanup_once

        def lost_stage(*call_args: object, **call_kwargs: object) -> object:
            result = original_stage(*call_args, **call_kwargs)
            if not injected["stage"]:
                injected["stage"] = True
                raise RuntimeError("injected lost member-load response")
            return result

        def lost_publication(*call_args: object, **call_kwargs: object) -> None:
            original_publication(*call_args, **call_kwargs)
            if not injected["publication"]:
                injected["publication"] = True
                raise RuntimeError("injected lost publication response")

        def lost_cleanup(*call_args: object, **call_kwargs: object) -> None:
            original_cleanup(*call_args, **call_kwargs)
            if not injected["cleanup"]:
                injected["cleanup"] = True
                raise RuntimeError("injected lost cleanup response")

        adapter.stage_member_once = lost_stage
        adapter.dispatch_publication_once = lost_publication
        adapter.dispatch_cleanup_once = lost_cleanup
        return adapter

    facade._service_factory = faulting_factory
    admitted = sink._full_refresh_publication.prepare_admission(config)
    result = sink.load(admitted, payload)

    assert injected == {"stage": True, "publication": True, "cleanup": True}
    for port in (18123, 28123):
        assert _execute(port, f"SELECT groupArray(id) FROM {database}.target") == [("[10,20]",)]
    server_version = _execute(18123, "SELECT version()")[0][0]
    details = {"operation_id": result.commit_receipt_id, "production_composition": True}
    for scenario in (
        "external_replication_lost_load_response",
        "external_replication_lost_publication_response",
        "external_replication_lost_cleanup_response",
    ):
        record_external_scenario(scenario, "PASS", server_version=server_version, details=details)

    interrupted_sink, interrupted_config, interrupted_payload, interrupted_database = _publication_case("restart")
    interrupted_facade = interrupted_sink._full_refresh_publication._external
    normal_factory = interrupted_facade._service_factory
    interrupted = False
    staged_before_restart: list[tuple[str, int]] = []
    stage_attempts = 0

    def interrupted_factory(*args: object, **kwargs: object) -> object:
        nonlocal interrupted, stage_attempts
        adapter = normal_factory(*args, **kwargs)
        original_stage = adapter.stage_member_once

        def stop_before_second_load(*call_args: object, **call_kwargs: object) -> object:
            nonlocal interrupted, stage_attempts
            stage_attempts += 1
            if stage_attempts == 2 and not interrupted:
                interrupted = True
                raise RuntimeError("injected worker interruption before second member load")
            result = original_stage(*call_args, **call_kwargs)
            staged_before_restart.append((str(call_args[0]), int(result["row_count"])))
            return result

        adapter.stage_member_once = stop_before_second_load
        return adapter

    interrupted_facade._service_factory = interrupted_factory
    interrupted_admitted = interrupted_sink._full_refresh_publication.prepare_admission(interrupted_config)
    with pytest.raises(Exception, match="STAGING_INCOMPLETE"):
        interrupted_facade.stage(interrupted_admitted, interrupted_payload)

    fresh_sink, _, _, _ = _publication_case("restart", database=interrupted_database, initialize=False)
    fresh_facade = fresh_sink._full_refresh_publication._external
    fresh_factory = fresh_facade._service_factory
    staged_after_restart: list[str] = []

    def observed_recovery_factory(*args: object, **kwargs: object) -> object:
        adapter = fresh_factory(*args, **kwargs)
        original_stage = adapter.stage_member_once

        def record_recovery_stage(*call_args: object, **call_kwargs: object) -> object:
            staged_after_restart.append(str(call_args[0]))
            return original_stage(*call_args, **call_kwargs)

        adapter.stage_member_once = record_recovery_stage
        return adapter

    fresh_facade._service_factory = observed_recovery_factory
    recovered = fresh_sink._full_refresh_publication.prepare_admission(interrupted_config)
    replay = fresh_sink._full_refresh_publication.replay_result(recovered)
    assert interrupted is True
    assert len(staged_before_restart) == 1 and staged_before_restart[0][1] == 2
    assert len(staged_after_restart) == 1
    assert staged_after_restart[0] != staged_before_restart[0][0]
    assert replay is not None and replay.total_rows == 2
    for port in (18123, 28123):
        assert _execute(port, f"SELECT groupArray(id) FROM {interrupted_database}.target") == [("[10,20]",)]
    record_external_scenario(
        "external_replication_staging_fresh_service_recovery",
        "PASS",
        server_version=server_version,
        details={
            "operation_id": replay.commit_receipt_id,
            "production_composition": True,
            "members_ready_before_restart": len(staged_before_restart),
            "members_staged_after_restart": len(staged_after_restart),
        },
    )


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_terminal_partial_is_retained_without_redispatch() -> None:
    from dpone.contracts.clickhouse_external_replication import ExternalPublicationError

    sink, config, payload, database = _publication_case("terminal_partial")
    facade = sink._full_refresh_publication._external
    original_factory = facade._service_factory
    dispatches = 0

    def faulting_factory(*args: object, **kwargs: object) -> object:
        adapter = original_factory(*args, **kwargs)
        original_publication = adapter.dispatch_publication_once

        def terminal_partial(*call_args: object, **call_kwargs: object) -> None:
            nonlocal dispatches
            dispatches += 1
            original_publication(*call_args, **call_kwargs)
            candidate = str(call_kwargs["candidate_name"])
            _execute(18123, f"EXCHANGE TABLES {database}.target AND {database}.{candidate}")
            raise RuntimeError("injected lost response after one-member rollback")

        adapter.dispatch_publication_once = terminal_partial
        return adapter

    facade._service_factory = faulting_factory
    admitted = sink._full_refresh_publication.prepare_admission(config)

    with pytest.raises(ExternalPublicationError, match="PUBLICATION_PARTIAL_TERMINAL"):
        sink.load(admitted, payload)
    with pytest.raises(ExternalPublicationError, match="PUBLICATION_PARTIAL_TERMINAL"):
        sink._full_refresh_publication.prepare_admission(config)

    assert dispatches == 1
    assert _execute(18123, f"SELECT groupArray(id) FROM {database}.target") == [("[1]",)]
    assert _execute(28123, f"SELECT groupArray(id) FROM {database}.target") == [("[10,20]",)]
    assert (
        int(
            _execute(
                18123,
                "SELECT uniqExact(entry) FROM system.distributed_ddl_queue "
                f"WHERE cluster='{_CLUSTER}' AND position(query, '{database}') > 0 "
                "AND position(query, 'EXCHANGE TABLES') > 0",
            )[0][0]
        )
        == 1
    )
    record_external_scenario(
        "external_replication_terminal_partial_no_redispatch",
        "PASS",
        server_version=_execute(18123, "SELECT version()")[0][0],
        details={"dispatch_count": dispatches, "retained_mixed_generation": True},
    )
