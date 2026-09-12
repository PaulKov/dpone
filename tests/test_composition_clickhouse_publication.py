"""Offline snapshot publication helpers; not live ClickHouse proof."""

from __future__ import annotations

import json
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_http import ClickHouseHttpError, ClickHouseHttpObservation
from dpone.app.composition_clickhouse_catalog import ClickHouseHttpSnapshotCatalog
from dpone.app.composition_clickhouse_publication import (
    ClickHouseDispatchBudgetPolicy,
    ClickHouseHttpSnapshotExecutor,
    clickhouse_plan_write,
    snapshot_target_for_write,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import InsertGenerationDispatch
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
from tests.composition_snapshot_helpers import intent


def _json_compact(columns: tuple[tuple[str, str], ...], rows: list[list[object]]) -> bytes:
    return json.dumps(
        {
            "meta": [{"name": name, "type": kind} for name, kind in columns],
            "data": rows,
            "rows": len(rows),
        },
        separators=(",", ":"),
    ).encode()


class _ScriptedCatalogHttp:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies = bodies
        self.payloads: list[bytes] = []
        self.paths: list[str] = []
        self.query_ids: list[str] = []

    def request(self, *, path: str, payload: bytes, query_id: str) -> ClickHouseHttpObservation:
        self.paths.append(path)
        self.query_ids.append(query_id)
        self.payloads.append(payload)
        text = payload.decode()
        for marker, body in self.bodies.items():
            if marker in text:
                return ClickHouseHttpObservation(len(payload), body, "identity")
        raise ClickHouseHttpError(len(payload))


def _catalog_bodies(
    *,
    tables: list[list[object]] | None = None,
    ddl: list[list[object]] | None = None,
    effects: list[object] | None = None,
    topology: list[object] | None = None,
) -> dict[str, bytes]:
    value = intent()
    target = value.target
    return {
        "system.databases": _json_compact((("engine", "String"),), [["Atomic"]]),
        "total_bytes": _json_compact(
            (
                ("name", "String"),
                ("toString(uuid)", "String"),
                ("engine", "String"),
                ("total_bytes", "Nullable(UInt64)"),
                ("total_rows", "Nullable(UInt64)"),
            ),
            tables
            if tables is not None
            else [
                [target.target_table, value.generation.old_target_uuid, "MergeTree", 40, 3],
                [target.generation_table, value.generation.new_generation_uuid, "MergeTree", 50, 4],
            ],
        ),
        "create_table_query": _json_compact(
            (("name", "String"), ("create_table_query", "String")),
            ddl
            if ddl is not None
            else [
                [target.target_table, "CREATE TABLE t (id Int32) ENGINE = MergeTree ORDER BY id"],
                [target.generation_table, "CREATE TABLE g (id Int32) ENGINE = MergeTree ORDER BY id"],
            ],
        ),
        "uniqExact(host_name)": _json_compact(
            (("node_count", "UInt64"), ("replica_count", "UInt64")),
            [topology if topology is not None else [1, 1]],
        ),
        "system.mutations": _json_compact(
            (
                ("mutations", "UInt64"),
                ("row_policies", "UInt64"),
                ("computed_columns", "UInt64"),
                ("skipping_indices", "UInt64"),
                ("distributed", "UInt64"),
            ),
            [effects if effects is not None else [0, 0, 0, 0, 0]],
        ),
    }


def _write() -> DbtRelationWrite:
    return DbtRelationWrite(
        project_path="standalone",
        workflow_id="a_native",
        resource_id="a_native",
        kind="transfer",
        connector="clickhouse",
        connection_ref="ch-sink",
        database="default",
        schema="default",
        relation="orders",
    )


def test_clickhouse_plan_write_requires_exact_sink_match() -> None:
    write = _write()
    plan = SimpleNamespace(writes=(write,))
    manifest = {"sink": {"table": {"name": "orders"}}}
    assert clickhouse_plan_write(plan, manifest) is write
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_payload"):
        clickhouse_plan_write(SimpleNamespace(writes=()), manifest)


def test_snapshot_target_uses_enrollment_and_reserved_generation() -> None:
    from tests.composition_snapshot_helpers import DATABASE, SERVICE, digest

    enrollment = SimpleNamespace(
        body={
            "service_id": SERVICE,
            "database_uuid": DATABASE,
            "target_enrollment_sha256": digest("enrollment"),
        }
    )
    write = _write()
    target = snapshot_target_for_write(enrollment, write)
    assert target.target_table == "orders"
    assert target.generation_table == "orders__dpone_gen"
    assert target.write_subject_sha256 == dbt_relation_write_subject(write)
    assert target.enrollment_sha256 == digest("enrollment")


def test_dispatch_budget_policy_rejects_oversize_insert() -> None:
    from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
    from dpone.contracts.composition_snapshot import SnapshotLimits
    from tests.composition_snapshot_helpers import digest

    value = intent()
    policy = ClickHouseDispatchBudgetPolicy(SnapshotLimits(1, 10, 10, 10, 10, 20))
    dispatch = InsertGenerationDispatch(
        value.attempt,
        value.target,
        value.generation.new_generation_uuid,
        (ClickHouseDispatchColumn("id", "Int32"),),
        digest("payload"),
        11,
        0,
    )
    with pytest.raises(CompositionAdmissionError, match="clickhouse_dispatch_payload_budget"):
        policy.require_dispatch(object(), dispatch)


def test_snapshot_executor_requires_attached_transport() -> None:
    executor = ClickHouseHttpSnapshotExecutor()
    with pytest.raises(CompositionAdmissionError, match="clickhouse_transport_binding"):
        executor.exchange_once(intent())


def test_http_catalog_observes_tables_without_copying_generation_hashes() -> None:
    value = intent()
    bodies = _catalog_bodies()
    catalog = ClickHouseHttpSnapshotCatalog(_ScriptedCatalogHttp(bodies))
    observation = catalog.inspect(value)
    assert observation.target == value.target
    assert observation.target_uuid == value.generation.old_target_uuid
    assert observation.generation_uuid == value.generation.new_generation_uuid
    assert observation.database_engine == "Atomic"
    assert observation.table_engines == ("MergeTree", "MergeTree")
    assert (observation.node_count, observation.replica_count) == (1, 1)
    assert observation.schema_sha256 == (None, None)
    assert observation.physical_sha256 == (None, None)
    assert observation.unsupported_features == ()
    assert observation.generation_content_sha256 is None
    assert observation.generation_rows == 4
    assert observation.generation_bytes == 50
    assert observation.old_target_bytes == 40
    assert observation.retained_bytes == 40
    assert observation.catalog_evidence_sha256 != value.generation.content_sha256
    assert observation.catalog_evidence_sha256.startswith("sha256:")
    digest = sha256()
    for body in (
        bodies["system.databases"],
        bodies["total_bytes"],
        bodies["create_table_query"],
        bodies["uniqExact(host_name)"],
        bodies["system.mutations"],
    ):
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    assert observation.catalog_evidence_sha256 == "sha256:" + digest.hexdigest()


def test_http_catalog_keeps_null_bytes_unknown() -> None:
    value = intent()
    target = value.target
    catalog = ClickHouseHttpSnapshotCatalog(
        _ScriptedCatalogHttp(
            _catalog_bodies(
                tables=[
                    [target.target_table, value.generation.old_target_uuid, "MergeTree", None, None],
                    [target.generation_table, value.generation.new_generation_uuid, "MergeTree", None, 0],
                ]
            )
        )
    )
    observation = catalog.inspect(value)
    assert observation.generation_rows == 0
    assert observation.generation_bytes is None
    assert observation.old_target_bytes is None
    assert observation.retained_bytes is None
    assert observation.generation_content_sha256 is None


def test_http_catalog_records_unsupported_features_from_http() -> None:
    value = intent()
    catalog = ClickHouseHttpSnapshotCatalog(
        _ScriptedCatalogHttp(
            _catalog_bodies(
                ddl=[
                    [value.target.target_table, "CREATE TABLE t (id Int32) ENGINE = MergeTree TTL now()"],
                    [value.target.generation_table, "CREATE TABLE g (id Int32) ENGINE = MergeTree ORDER BY id"],
                ],
                effects=[1, 0, 0, 0, 0],
            )
        )
    )
    observation = catalog.inspect(value)
    assert observation.unsupported_features == ("mutations", "ttl")


def test_http_catalog_fail_closes_when_http_is_missing() -> None:
    catalog = ClickHouseHttpSnapshotCatalog(_ScriptedCatalogHttp({}))
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog"):
        catalog.inspect(intent())


def test_http_catalog_cannot_classify_publication_without_typed_hashes() -> None:
    catalog = ClickHouseHttpSnapshotCatalog(_ScriptedCatalogHttp(_catalog_bodies()))
    assert catalog.can_classify_publication() is False


def test_http_catalog_binds_query_id_to_attempt() -> None:
    http = _ScriptedCatalogHttp(_catalog_bodies())
    catalog = ClickHouseHttpSnapshotCatalog(http)
    value = intent()
    catalog.inspect(value)
    prefix = "dpone-catalog-" + value.attempt.attempt_sha256.removeprefix("sha256:")[:32]
    assert http.query_ids
    assert all(query_id.startswith(prefix) for query_id in http.query_ids)
    assert all(prefix in path for path in http.paths)
