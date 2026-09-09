from __future__ import annotations

import json
import re
from datetime import timedelta
from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink


class _FakeConnection:
    def __init__(self, connector: _FakeClickHouseConnector) -> None:
        self.connector = connector

    def execute(self, query: str, rows: list[tuple[object, ...]], **kwargs: object) -> None:
        self.connector.queries.append((query, rows, kwargs))


class _FakeClickHouseConnector:
    host = "127.0.0.1"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    application_name = "test"
    secure = False
    compression = True
    connect_timeout = 10
    send_receive_timeout = 3600
    settings: dict[str, object] = {}

    def __init__(self, *, table_exists: bool = False) -> None:
        self.queries: list[tuple[str, object | None, object | None]] = []
        self.connection = _FakeConnection(self)
        self.table_exists = table_exists

    def execute_query(self, query: str, params: object | None = None) -> int:
        self.queries.append((query, params, None))
        return 0

    def get_records(self, query: str):
        self.queries.append((query, None, None))
        if query.startswith("EXISTS TABLE"):
            return [(1 if self.table_exists else 0,)]
        if query.startswith("SELECT count()"):
            return [(1,)]
        if "system.columns" in query:
            return [("id", "Int64")]
        return [(0,)]


def test_clickhouse_uses_target_schema_for_artifacts_when_staging_schema_is_default() -> None:
    connector = _FakeClickHouseConnector()

    ClickHouseSink(connector).load(_load_config(), _payload())

    rendered = _rendered(connector)
    assert "`Example_Datamarts`.`orders__dpone_staging_" in rendered
    assert "`DWH_Tech`.`orders__dpone_staging_" not in rendered


def test_clickhouse_uses_explicit_staging_schema_for_staging_and_cleanup() -> None:
    connector = _FakeClickHouseConnector()
    config = _load_config(staging_schema="DWH_Tech")

    ClickHouseSink(connector).load(config, _payload())

    rendered = _rendered(connector)
    assert "CREATE DATABASE IF NOT EXISTS `DWH_Tech`" in rendered
    assert "CREATE TABLE `DWH_Tech`.`orders__dpone_staging_" in rendered
    assert "INSERT INTO `DWH_Tech`.`orders__dpone_staging_" in rendered
    assert re.search(r"DROP TABLE IF EXISTS `DWH_Tech`.`orders__dpone_staging_[0-9a-f]{8}`", rendered)
    assert "`Example_Datamarts`.`orders__dpone_staging_" not in rendered


def test_clickhouse_projects_lineage_into_explicit_staging_schema() -> None:
    connector = _FakeClickHouseConnector()
    config = _load_config(staging_schema="DWH_Tech")
    config.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    sink = ClickHouseSink(connector)
    handle = sink.stage_payload(config, _payload())
    assert handle.metadata["operation_schema"] == "DWH_Tech"
    assert handle.metadata["operation_tables"]["staging"].startswith("DWH_Tech.orders__dpone_staging_")

    projected = sink.lineage_projector.project(
        load_config=config,
        handle=handle,
        lineage_options=type("LineageOptions", (), {"enabled": True, "has_feature": lambda self, _: False})(),
        load_record=type("Record", (), {"run_id": "run", "load_id": "load"})(),
    )

    rendered = _rendered(connector)
    assert projected.projected is True
    assert "CREATE TABLE `DWH_Tech`.`orders__dpone_projected_" in rendered
    assert "INSERT INTO `DWH_Tech`.`orders__dpone_projected_" in rendered


def test_clickhouse_load_governance_audit_can_use_dwh_tech_schema() -> None:
    from dpone.runtime.lineage.audit import LoadIdentityService
    from dpone.runtime.state.clickhouse import ClickHouseLoadAuditStorage, ClickHouseLoadStepAuditStorage

    connector = _FakeClickHouseConnector()
    identity = LoadIdentityService(
        audit_storage=ClickHouseLoadAuditStorage(connector, schema="DWH_Tech", table="__dpone__loads")
    )
    record = identity.start(_load_config(staging_schema="DWH_Tech"), process_name="orders")
    ClickHouseLoadStepAuditStorage(connector, schema="DWH_Tech", table="__dpone__load_steps").record_step(
        type(
            "Step",
            (),
            {
                "run_id": record.run_id,
                "load_id": record.load_id,
                "step_id": "staging_loaded",
                "phase": "load",
                "kind": "runtime",
                "status": "succeeded",
                "started_at": record.started_at,
                "finished_at": record.started_at + timedelta(seconds=4),
                "error_message": None,
                "details": {"operation_schema": "DWH_Tech", "staged_rows": 40},
            },
        )()
    )

    rendered = _rendered(connector)
    assert "CREATE TABLE IF NOT EXISTS `DWH_Tech`.`__dpone__loads`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `DWH_Tech`.`__dpone__load_steps`" in rendered
    details_json = connector.queries[-1][1][0][-1]  # type: ignore[index]
    assert '"throughput": {"byte_count"' not in details_json
    assert '"schema_version": "dpone.runtime.throughput.v1"' in details_json
    assert '"rows_per_second": 10.0' in details_json


def test_clickhouse_staging_like_target_ensures_explicit_operation_schema_database() -> None:
    connector = _FakeClickHouseConnector(table_exists=True)
    config = _load_config(staging_schema="DWH_Tech")
    config.load_strategy = LoadStrategy.PARTITION_REPLACE

    ClickHouseSink(connector).stage_payload(config, _payload())

    rendered = _rendered(connector)
    assert "CREATE DATABASE IF NOT EXISTS `DWH_Tech`" in rendered
    assert "CREATE TABLE `DWH_Tech`.`orders__dpone_staging_" in rendered
    assert " AS `Example_Datamarts`.`orders`" in rendered


def test_manifest_schemas_expose_managed_artifact_staging_schema_contract() -> None:
    for path in (
        Path("src/dpone/schema/etl-config.schema.json"),
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
    ):
        schema = json.loads(path.read_text(encoding="utf-8"))
        sink = (
            schema["properties"]["sink"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]
        )
        staging = sink["staging"]["properties"]

        assert staging["schema"]["default"] == "staging"
        assert "DWH_Tech" in staging["schema"]["markdownDescription"]
        assert staging["database"]["type"] == "string"


def test_clickhouse_managed_artifact_cleanup_and_swap_use_on_cluster() -> None:
    connector = _FakeClickHouseConnector()
    config = _load_config(staging_schema="DWH_Tech")
    config.options["physical_design"] = {"storage": {"clickhouse": {"cluster": "dwh"}}}

    ClickHouseSink(connector).load(config, _payload())

    rendered = _rendered(connector)
    assert re.search(
        r"RENAME TABLE `DWH_Tech`.`orders__dpone_staging_[0-9a-f]{8}` "
        r"TO `Example_Datamarts`.`orders` ON CLUSTER `dwh`",
        rendered,
    )
    assert re.search(
        r"DROP TABLE IF EXISTS `DWH_Tech`.`orders__dpone_staging_[0-9a-f]{8}` ON CLUSTER `dwh`",
        rendered,
    )


def _load_config(*, staging_schema: str = "staging") -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="Example_Datamarts",
        target_table="orders",
        staging_schema=staging_schema,
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False},
    )


def _payload() -> LoadPayload:
    return LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "int")])


def _rendered(connector: _FakeClickHouseConnector) -> str:
    return "\n".join(str(query) for query, *_ in connector.queries)
