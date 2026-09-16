"""Durable transport identity preserves exact master BCP names and bindings."""

from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.runtime.native_wire_models import stable_hash
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter
from dpone.runtime.sinks.mssql_native_prepared_owner import planned_stage

# Generated from pure definitions at master 86a58e3, using synthetic inputs only.
GOLDENS = [
    {
        "case": "ascii",
        "values": [
            "run-synthetic",
            "target-synthetic",
            "query-synthetic",
            "window-synthetic",
            "schema-synthetic",
            "wire-synthetic",
        ],
        "attempt": "000001:0",
        "projection": {
            "run_id": "run-synthetic",
            "target_id": "target-synthetic",
            "source_query_id": "query-synthetic",
            "window_fingerprint": "window-synthetic",
            "schema_fingerprint": "schema-synthetic",
            "wire_fingerprint": "wire-synthetic",
        },
        "projection_repr_utf8_hex": "7b2772756e5f6964273a202772756e2d73796e746865746963272c20277461726765745f6964273a20277461726765742d73796e746865746963272c2027736f757263655f71756572795f6964273a202771756572792d73796e746865746963272c202777696e646f775f66696e6765727072696e74273a202777696e646f772d73796e746865746963272c2027736368656d615f66696e6765727072696e74273a2027736368656d612d73796e746865746963272c2027776972655f66696e6765727072696e74273a2027776972652d73796e746865746963277d",
        "table_name": "dpone_native_192d75c2a546b5879420fce3894c710e4d1d8a62",
        "ownership": {
            "database": "synthetic_db",
            "schema": "synthetic_schema",
            "table": "dpone_native_192d75c2a546b5879420fce3894c710e4d1d8a62",
            "binding": "192d75c2a546b5879420fce3894c710e4d1d8a6216bc42fbf527ecd18cd15bd2",
        },
        "native_plan_binding": "sha256:4229fad4c385f6d7f8c7c25ed07a5491f071bbe7a370c41efbf9dc9a3b44f8d5",
        "prepared": {
            "database": "synthetic_db",
            "schema": "synthetic_schema",
            "table": "dpone_native_prepared_4229fad4c385f6d7f8c7c25ed07a5491f071bbe7",
            "binding": "4229fad4c385f6d7f8c7c25ed07a5491f071bbe7a370c41efbf9dc9a3b44f8d5",
        },
    },
    {
        "case": "unicode",
        "values": ["run-Пример-😀", "target-'quoted'", "query-\\-\n", "window-Ω", "schema-é", "wire-空"],
        "attempt": "000002:1",
        "projection": {
            "run_id": "run-Пример-😀",
            "target_id": "target-'quoted'",
            "source_query_id": "query-\\-\n",
            "window_fingerprint": "window-Ω",
            "schema_fingerprint": "schema-é",
            "wire_fingerprint": "wire-空",
        },
        "projection_repr_utf8_hex": "7b2772756e5f6964273a202772756e2dd09fd180d0b8d0bcd0b5d1802df09f9880272c20277461726765745f6964273a20227461726765742d2771756f74656427222c2027736f757263655f71756572795f6964273a202771756572792d5c5c2d5c6e272c202777696e646f775f66696e6765727072696e74273a202777696e646f772dcea9272c2027736368656d615f66696e6765727072696e74273a2027736368656d612dc3a9272c2027776972655f66696e6765727072696e74273a2027776972652de7a9ba277d",
        "table_name": "dpone_native_852fb7c95abadc064e9159f197343fbcbe8a55f5",
        "ownership": {
            "database": "synthetic_db",
            "schema": "synthetic_schema",
            "table": "dpone_native_852fb7c95abadc064e9159f197343fbcbe8a55f5",
            "binding": "852fb7c95abadc064e9159f197343fbcbe8a55f5509b8e9c463f6cc936c9c9ea",
        },
        "native_plan_binding": "sha256:49a4c4e90111c8c6305a55daeedb2a1565fcc9c464982e77cafef0d2633d10fd",
        "prepared": {
            "database": "synthetic_db",
            "schema": "synthetic_schema",
            "table": "dpone_native_prepared_49a4c4e90111c8c6305a55daeedb2a1565fcc9c4",
            "binding": "49a4c4e90111c8c6305a55daeedb2a1565fcc9c464982e77cafef0d2633d10fd",
        },
    },
]


@pytest.mark.parametrize("golden", GOLDENS, ids=lambda value: value["case"])
def test_omitted_transport_preserves_master_identity(golden):
    plan = NativeChunkPlan(*golden["values"])
    projection = plan.to_dict()
    assert projection == golden["projection"]
    assert repr(projection).encode().hex() == golden["projection_repr_utf8_hex"]
    assert stable_hash(projection) == golden["native_plan_binding"]
    importer = object.__new__(MssqlNativeChunkImporter)
    importer.database, importer.schema = "synthetic_db", "synthetic_schema"
    assert importer.table_name(plan, golden["attempt"]) == golden["table_name"]
    assert importer._ownership(plan, golden["attempt"]) == golden["ownership"]
    config = SimpleNamespace(
        staging_database=None, target_database="synthetic_db", staging_schema=None, target_schema="synthetic_schema"
    )
    assert planned_stage(SimpleNamespace(plan=plan), config) == golden["prepared"]


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
def test_explicit_transport_binds_resolved_policy_and_changes_names(backend):
    from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy

    policy = NativeBulkTransportPolicy(backend, "rows", 8 << 30)
    values = GOLDENS[0]["values"]
    plan = NativeChunkPlan(*values, transport=policy)
    assert plan.to_dict() == dict(NativeChunkPlan(*values).to_dict(), transport=policy.to_dict())
    importer = object.__new__(MssqlNativeChunkImporter)
    assert importer.table_name(plan, GOLDENS[0]["attempt"]) != GOLDENS[0]["table_name"]
    projection = plan.to_dict()
    projection["transport"]["batch_rows"] = 1
    assert plan.to_dict()["transport"]["batch_rows"] == 65536


def test_removed_raw_activation_is_not_restored():
    with pytest.raises(TypeError):
        NativeChunkPlan(*GOLDENS[0]["values"], source_read_mode="raw_single_query")
