"""Production capture wiring must bind real observers and immutable source authority."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials
from dpone.app.composition_clickhouse_capture_factory import build_clickhouse_capture_components
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot_capture import attempt_snapshot_target
from tests.composition_snapshot_helpers import digest, intent


def arguments(tmp_path):
    value = intent()
    pin = {
        "database_id": 7,
        "database_guid": "22222222-2222-2222-2222-222222222222",
        "create_token": "2026-01-01T00:00:00",
    }
    resolved = SimpleNamespace(
        descriptor=SimpleNamespace(
            connection_type="mssql",
            properties={
                "database": "source",
                "composition_service_id": "11111111-1111-1111-1111-111111111111",
                "database_authorities": {"source": pin, "control": {**pin, "database_id": 8}},
            },
        ),
        credentials=SimpleNamespace(database="source"),
    )

    def no_connection():
        pytest.fail("factory construction or foreign attempt opened SQL")

    control = SimpleNamespace(
        connection_factory=no_connection,
        expected_service_id=resolved.descriptor.properties["composition_service_id"],
        control_database="control",
        control_schema="custom",
    )
    return dict(
        parent={
            "control": control,
            "resolver": SimpleNamespace(resolve=lambda ref: resolved),
            "context": SimpleNamespace(runtime_context_sha256=digest("verified runtime")),
        },
        manifest={
            "source": {
                "connection_ref": "source-ref",
                "table": {"database": "source", "schema": "dbo", "name": "orders"},
            }
        },
        plan=SimpleNamespace(sources=SimpleNamespace(subject_sha256=value.attempt.plan_sha256)),
        attempt=value.attempt,
        target=attempt_snapshot_target(value.target, value.attempt),
        limits=value.limits,
        root=tmp_path / "snapshots",
        endpoint="http://127.0.0.1:8123",
        credentials=ClickHouseTransportCredentials("observer", "test-credential"),
        ca_file=None,
    )


def test_construction_defers_rows_but_installs_capture_and_visibility(tmp_path):
    args = arguments(tmp_path)
    components = build_clickhouse_capture_components(**args)
    assert components.source.table_identity == ("source", "dbo", "orders")
    assert components.source.limits == args["limits"]
    assert components.catalog.can_observe_capture()
    assert not components.catalog.can_classify_publication()
    assert not args["root"].exists()
    with pytest.raises(CompositionAdmissionError, match="source_binding"):
        components.capture._store.claim_once(replace(args["attempt"], try_number=args["attempt"].try_number + 1))


def test_source_observation_uses_the_supplied_business_connection(tmp_path, monkeypatch):
    from dpone.app import composition_clickhouse_capture_factory as module

    observed = []

    def verify(connection, **kwargs):
        observed.append((connection, kwargs))
        return b"canonical observed pins"

    monkeypatch.setattr(module, "require_mssql_connection_identity", verify)
    components = build_clickhouse_capture_components(**arguments(tmp_path))
    connection = object()
    assert components.source._source_identity(connection) == b"canonical observed pins"
    assert observed[0][0] is connection
    assert [pin.database_name for pin in observed[0][1]["pins"]] == ["source", "control"]
    assert observed[0][1]["control_schema"] == "custom"


def test_foreign_source_service_is_rejected_before_sql(tmp_path):
    args = arguments(tmp_path)
    args["parent"]["resolver"].resolve("source-ref").descriptor.properties["composition_service_id"] = (
        "33333333-3333-3333-3333-333333333333"
    )
    with pytest.raises(CompositionAdmissionError, match="source_service"):
        build_clickhouse_capture_components(**args)


@pytest.mark.parametrize("missing", ["source", "control"])
def test_missing_source_or_control_pin_is_admission_failure_without_sql(tmp_path, missing):
    args = arguments(tmp_path)
    del args["parent"]["resolver"].resolve("source-ref").descriptor.properties["database_authorities"][missing]
    with pytest.raises(CompositionAdmissionError, match="source_authority"):
        build_clickhouse_capture_components(**args)
