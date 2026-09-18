from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.contracts.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalMember,
    ExternalMemberRecord,
    PhysicalGeneration,
    derive_generation_id,
)
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.clickhouse_external_replication_member_driver import (
    ClickHouseExternalReplicationMemberDriver,
    canonical_rows_digest,
    canonical_schema_digest,
)
from dpone.runtime.sinks.load_payload import LoadPayload


def _digest(character: str) -> str:
    return character * 64


def _record() -> ExternalAuthorityRecord:
    artifact = ArtifactIdentity(
        sha256=_digest("a"),
        byte_size=128,
        row_count=2,
        schema_digest=_digest("b"),
        wire_digest=_digest("c"),
    )
    return ExternalAuthorityRecord(
        target_key=_digest("1"),
        operation_id=_digest("2"),
        fence_token="fence-token",
        phase=ExternalAuthorityPhase.STAGING,
        dispatch_epoch=0,
        inventory_digest=_digest("3"),
        plan_digest=_digest("4"),
        database="analytics",
        target="target_table",
        candidate="candidate_table",
        members=tuple(
            ExternalMemberRecord(
                member_id=ExternalMember.create(
                    shard_num=1,
                    replica_num=replica,
                    internal_replication=False,
                ).member_id
            )
            for replica in (1, 2)
        ),
        artifact=artifact,
        artifact_binding_id="artifact-v1",
        generation_id=derive_generation_id(
            operation_id=_digest("2"),
            artifact_sha256=artifact.sha256,
            schema_digest=artifact.schema_digest,
            row_count=artifact.row_count,
        ),
    )


class _Connector:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, Any]] = {
            "target_table": {
                "uuid": "target-uuid",
                "engine": "MergeTree ORDER BY tuple()",
                "columns": [("id", "Int64", "", "", 1), ("label", "String", "", "", 2)],
                "rows": [(2, "beta"), (1, "alpha")],
            }
        }
        self.queries: list[tuple[str, Any]] = []
        self.mutations: list[str] = []

    def get_records(self, query: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.queries.append((query, params))
        table_name = str((params or {}).get("table", ""))
        table = self.tables.get(table_name)
        if "FROM system.tables" in query:
            return [] if table is None else [(table["uuid"], table["engine"])]
        if "FROM system.columns" in query:
            return [] if table is None else list(table["columns"])
        if "SELECT count()" in query:
            assert table is not None
            return [(len(table["rows"]),)]
        if query.startswith("SELECT "):
            assert table is not None
            limit = int(query.rsplit(" LIMIT ", 1)[1])
            return sorted(table["rows"])[:limit]
        raise AssertionError(f"unexpected query shape: {query.split()[0]}")

    def execute_query(self, query: str, params: Any = None) -> int:
        self.mutations.append(query)
        if query.startswith("DROP TABLE"):
            self.tables.pop("candidate_table", None)
        return 0


class _Sink:
    def __init__(self, connector: _Connector) -> None:
        self.connector = connector
        self.created: list[tuple[Any, Any, bool]] = []
        self.loaded: list[tuple[Any, LoadPayload]] = []

    def _create_table(self, config: Any, schema: Any, *, if_not_exists: bool) -> None:
        self.created.append((config, schema, if_not_exists))
        self.connector.tables[config.target_table] = {
            "uuid": "candidate-uuid",
            "engine": "MergeTree ORDER BY tuple()",
            "columns": [("id", "Int64", "", "", 1), ("label", "String", "", "", 2)],
            "rows": [],
        }

    def _insert_payload(self, config: Any, payload: LoadPayload) -> int:
        self.loaded.append((config, payload))
        rows = [(1, "alpha"), (2, "beta")]
        self.connector.tables[config.target_table]["rows"] = rows
        return len(rows)


@pytest.fixture
def load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source_schema",
        source_table="source_table",
        target_schema="analytics",
        target_table="target_table",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "MergeTree",
                        "cluster": {
                            "name": "analytics_cluster",
                            "ddl_scope": "cluster",
                            "replication_mode": "external",
                        },
                        "access_table": {"name": "target_table_all"},
                    }
                }
            }
        },
    )


def _driver(load_config: LoadConfig, connector: _Connector) -> tuple[ClickHouseExternalReplicationMemberDriver, _Sink]:
    sink = _Sink(connector)
    driver = ClickHouseExternalReplicationMemberDriver(
        load_config=load_config,
        payload_schema=(("id", "bigint"), ("label", "varchar")),
        sink_factory=lambda direct: sink if direct is connector else None,
        member_identity=lambda direct: _record().members[0].member_id if direct is connector else "",
        max_content_rows=3,
    )
    return driver, sink


def test_observe_returns_local_physical_id_and_bounded_canonical_digests(load_config: LoadConfig) -> None:
    connector = _Connector()
    driver, _ = _driver(load_config, connector)
    record = _record()

    observed = driver.observe(connector, record)

    assert observed.member_id == record.members[0].member_id
    assert observed.target == PhysicalGeneration(
        uuid="target-uuid",
        engine_full="MergeTree ORDER BY tuple()",
        schema_digest=canonical_schema_digest(connector.tables["target_table"]["columns"]),
        content_digest=canonical_rows_digest([(1, "alpha"), (2, "beta")]),
        row_count=2,
    )
    assert observed.candidate is None
    assert "alpha" not in repr(observed)


def test_create_uses_deterministic_candidate_and_forces_local_ddl(load_config: LoadConfig) -> None:
    connector = _Connector()
    driver, sink = _driver(load_config, connector)

    created = driver.create_candidate(connector, _record())

    config, schema, if_not_exists = sink.created[0]
    clickhouse = config.options["physical_design"]["storage"]["clickhouse"]
    assert (config.target_schema, config.target_table) == ("analytics", "candidate_table")
    assert schema == (("id", "bigint"), ("label", "varchar"))
    assert if_not_exists is False
    assert clickhouse["cluster"] == {"ddl_scope": "local", "replication_mode": "external"}
    assert "access_table" not in clickhouse
    assert created.uuid == "candidate-uuid"


def test_load_accepts_only_sealed_load_payload_and_calls_sink_once(load_config: LoadConfig) -> None:
    connector = _Connector()
    driver, sink = _driver(load_config, connector)
    record = _record()
    created = driver.create_candidate(connector, record)
    record = replace(record, members=(replace(record.members[0], candidate=created), record.members[1]))
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]),
        schema=(("id", "bigint"), ("label", "varchar")),
    )

    driver.load_candidate(connector, record, payload)

    assert sink.loaded == [(sink.created[0][0], payload)]
    assert driver.observe(connector, record).candidate is not None

    with pytest.raises(ValueError, match="ARTIFACT_UNSUPPORTED"):
        driver.load_candidate(connector, record, object())
    assert len(sink.loaded) == 1

    mismatched = payload.rebind(schema=(("other", "bigint"),))
    with pytest.raises(ValueError, match="ARTIFACT_UNSUPPORTED"):
        driver.load_candidate(connector, record, mismatched)
    assert len(sink.loaded) == 1


def test_drop_mutates_only_the_exact_expected_candidate_uuid(load_config: LoadConfig) -> None:
    connector = _Connector()
    driver, _ = _driver(load_config, connector)
    record = _record()
    expected = driver.create_candidate(connector, record)

    driver.drop_candidate(connector, record, expected)

    assert connector.mutations == ["DROP TABLE `analytics`.`candidate_table`"]
    assert driver.observe(connector, record).candidate is None


def test_drop_rejects_foreign_uuid_without_mutation(load_config: LoadConfig) -> None:
    connector = _Connector()
    driver, _ = _driver(load_config, connector)
    record = _record()
    expected = driver.create_candidate(connector, record)

    with pytest.raises(ValueError, match="GENERATION_DIVERGED"):
        driver.drop_candidate(connector, record, replace(expected, uuid="foreign-uuid"))

    assert connector.mutations == []
    assert "candidate_table" in connector.tables


def test_observe_rejects_replicated_engine_and_unbounded_content(load_config: LoadConfig) -> None:
    connector = _Connector()
    connector.tables["target_table"]["engine"] = "ReplicatedMergeTree('/path', 'replica') ORDER BY tuple()"
    driver, _ = _driver(load_config, connector)

    with pytest.raises(ValueError, match="ENGINE_UNSUPPORTED"):
        driver.observe(connector, _record())

    connector.tables["target_table"]["engine"] = "MergeTree ORDER BY tuple()"
    connector.tables["target_table"]["rows"] = [(index, str(index)) for index in range(4)]
    with pytest.raises(ValueError, match="CONTENT_BUDGET_EXCEEDED"):
        driver.observe(connector, _record())
