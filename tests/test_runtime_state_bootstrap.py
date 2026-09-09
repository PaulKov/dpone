from __future__ import annotations

from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.bootstrap_state import RuntimeStateBootstrap
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import (
    ResolvedConnectorFactory,
)
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.state.factory import StateFactory


def test_disabled_state_does_not_create_durable_state_connectors(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_connector(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("disabled state must not create durable state connectors")

    monkeypatch.setattr(StateFactory, "create_bigquery_connector", forbidden_connector)
    monkeypatch.setattr(StateFactory, "create_mssql_connector", forbidden_connector)
    monkeypatch.setattr(StateFactory, "create_postgres_connector", forbidden_connector)

    bindings = RuntimeStateBootstrap().build(
        config={},
        sink_cfg={"type": "clickhouse", "connection_id": "ClickHouse"},
        state_cfg={"type": "disabled"},
        load_config=LoadConfig(
            source_conn_id="mssql_example",
            target_conn_id="ClickHouse",
            source_schema="pln",
            source_table="Sale_plan_type1",
            target_schema="DWH_OLAP",
            target_table="dpone_smoke_mssql_sale_plan_type1",
        ),
    )

    assert bindings.state_type == "disabled"
    assert bindings.xmin_state_storage is None
    assert bindings.kafka_offset_state_storage is None
    assert bindings.partition_checkpoint_store is None
    assert bindings.shared_bq_connector is None
    assert bindings.shared_mssql_state_connector is None
    assert bindings.shared_postgres_state_connector is None


def test_missing_state_defaults_to_disabled_for_stateless_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_connector(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("missing state on stateless loads must not create durable state connectors")

    monkeypatch.setattr(StateFactory, "create_bigquery_connector", forbidden_connector)
    monkeypatch.setattr(StateFactory, "create_mssql_connector", forbidden_connector)
    monkeypatch.setattr(StateFactory, "create_postgres_connector", forbidden_connector)

    bindings = RuntimeStateBootstrap().build(
        config={},
        sink_cfg={"type": "clickhouse", "connection_id": "ClickHouse"},
        state_cfg={},
        state_configured=False,
        load_config=LoadConfig(
            source_conn_id="ClickHouse",
            target_conn_id="ClickHouse",
            source_schema="landing",
            source_table="orders",
            target_schema="landing",
            target_table="orders_copy",
            load_strategy=LoadStrategy.FULL_REFRESH,
        ),
    )

    assert bindings.state_type == "disabled"
    assert bindings.xmin_state_storage is None
    assert bindings.shared_bq_connector is None


def test_missing_state_fails_closed_for_stateful_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_connector(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("stateful loads must not invent implicit state defaults")

    monkeypatch.setattr(StateFactory, "create_bigquery_connector", forbidden_connector)

    with pytest.raises(RuntimeConfigurationError, match="state.type must be set explicitly"):
        RuntimeStateBootstrap().build(
            config={},
            sink_cfg={"type": "clickhouse", "connection_id": "ClickHouse"},
            state_cfg={},
            state_configured=False,
            load_config=LoadConfig(
                source_conn_id="ClickHouse",
                target_conn_id="ClickHouse",
                source_schema="landing",
                source_table="orders",
                target_schema="landing",
                target_table="orders_copy",
                load_strategy=LoadStrategy.INCREMENTAL_APPEND,
            ),
        )


def test_stateful_bigquery_state_requires_explicit_vault_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    bigquery_connector = object()
    xmin_storage = object()
    kafka_storage = object()

    def create_bigquery_connector(**kwargs: Any) -> object:
        calls.append(f"bigquery:{kwargs['connection_id']}:{kwargs['mount_point']}:{kwargs['path']}")
        return bigquery_connector

    monkeypatch.setattr(StateFactory, "create_bigquery_connector", create_bigquery_connector)
    monkeypatch.setattr(
        StateFactory,
        "create_xmin_state_storage",
        lambda **kwargs: xmin_storage,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_bigquery_kafka_offset_state_storage",
        lambda **kwargs: kafka_storage,
    )

    bindings = RuntimeStateBootstrap().build(
        config={},
        sink_cfg={"type": "clickhouse", "connection_id": "ClickHouse"},
        state_cfg={
            "type": "bigquery",
            "credentials_source": "vault",
            "vault_mount_point": "prod",
            "vault_path": "gcp/example-dp-prod/bq/example-etl-service",
        },
        load_config=LoadConfig(
            source_conn_id="ClickHouse",
            target_conn_id="ClickHouse",
            source_schema="landing",
            source_table="orders",
            target_schema="landing",
            target_table="orders_copy",
            load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        ),
    )

    assert calls == ["bigquery:ClickHouse:prod:gcp/example-dp-prod/bq/example-etl-service"]
    assert bindings.state_type == "bigquery"
    assert bindings.xmin_state_storage is xmin_storage
    assert bindings.kafka_offset_state_storage is kafka_storage
    assert bindings.shared_bq_connector is bigquery_connector


def test_disabled_state_rejects_stateful_load_strategies() -> None:
    with pytest.raises(RuntimeConfigurationError, match="state.type='disabled'"):
        RuntimeStateBootstrap().build(
            config={},
            sink_cfg={"type": "clickhouse", "connection_id": "ClickHouse"},
            state_cfg={"type": "disabled"},
            load_config=LoadConfig(
                source_conn_id="mssql_example",
                target_conn_id="ClickHouse",
                source_schema="pln",
                source_table="Sale_plan_type1",
                target_schema="DWH_OLAP",
                target_table="dpone_smoke_mssql_sale_plan_type1",
                load_strategy=LoadStrategy.INCREMENTAL_APPEND,
            ),
        )


def test_resolved_state_uses_injected_snapshot_without_legacy_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = object()
    xmin_storage = object()
    kafka_storage = object()

    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda connection, **kwargs: connector,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_postgres_connector",
        lambda **kwargs: pytest.fail("legacy credential discovery must not run"),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_postgres_xmin_state_storage",
        lambda **kwargs: xmin_storage,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_postgres_kafka_offset_state_storage",
        lambda **kwargs: kafka_storage,
    )

    bindings = RuntimeStateBootstrap().build_resolved(
        state_cfg={"type": "postgres"},
        state_connection=ResolvedBindingConnection(
            credentials=CredentialsConfig(),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="postgres",
                properties={},
            ),
        ),
        load_config=LoadConfig(
            source_conn_id="source-main",
            target_conn_id="sink-main",
            source_schema="raw",
            source_table="orders",
            target_schema="analytics",
            target_table="orders",
            load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        ),
    )

    assert bindings.shared_postgres_state_connector is connector
    assert bindings.xmin_state_storage is xmin_storage
    assert bindings.kafka_offset_state_storage is kafka_storage


def test_resolved_mssql_state_inherits_registry_schema_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda connection, **kwargs: object(),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_xmin_state_storage",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_kafka_offset_state_storage",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_state.build_mssql_load_audit_storage",
        lambda *args, **kwargs: object(),
    )

    RuntimeStateBootstrap().build_resolved(
        state_cfg={
            "type": "mssql",
            "table": {"name": "dpone_source_state"},
        },
        state_connection=ResolvedBindingConnection(
            credentials=CredentialsConfig(database="Example_System", schema="dbo"),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="mssql",
                properties={"database": "Example_System", "schema": "dbo"},
            ),
        ),
        load_config=LoadConfig(
            source_conn_id="source-main",
            target_conn_id="sink-main",
            source_schema="public",
            source_table="metrics_config",
            target_schema="sample_metrics",
            target_table="metrics_config",
            load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        ),
    )

    assert captured["state_table"] == "dpone_source_state"
    assert captured["schema"] == "dbo"


def test_resolved_mssql_state_keeps_authored_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda connection, **kwargs: object(),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_xmin_state_storage",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_kafka_offset_state_storage",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_state.build_mssql_load_audit_storage",
        lambda *args, **kwargs: object(),
    )

    RuntimeStateBootstrap().build_resolved(
        state_cfg={
            "type": "mssql",
            "table": {"name": "dpone_source_state", "schema": "already_set"},
        },
        state_connection=ResolvedBindingConnection(
            credentials=CredentialsConfig(database="Example_System", schema="dbo"),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="mssql",
                properties={"schema": "dbo"},
            ),
        ),
        load_config=LoadConfig(
            source_conn_id="source-main",
            target_conn_id="sink-main",
            source_schema="public",
            source_table="metrics_config",
            target_schema="sample_metrics",
            target_table="metrics_config",
            load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        ),
    )

    assert captured["schema"] == "already_set"
