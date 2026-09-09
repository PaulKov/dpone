"""Real observer orchestration with scripted SQL rows; not live certification."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
from dpone.contracts.dbt_workspace_observation import (
    MssqlWorkspaceObservationRequest,
    WorkspaceObservationError,
    WorkspaceObservationLimits,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.dbt_workspace_mssql_observer import MssqlWorkspaceCatalogObserver

PIN = MssqlDatabaseAuthorityPin("warehouse", 7, "2026-01-01T00:00:00.000", UUID("00000000-0000-0000-0000-000000000007"))


def _write(name="Orders", *, database="warehouse", kind="model"):
    return DbtRelationWrite(
        "dbt/demo", "orders", f"model.demo.{name}", kind, "mssql", "warehouse", database, "mart", name
    )


def _request(*writes, **overrides):
    values = dict(
        release_id="sha256:" + "a" * 64,
        runtime_context_sha256="sha256:" + "b" * 64,
        macro_authority_sha256=DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
        pin=PIN,
        default_database="warehouse",
        invocation_databases=("warehouse",),
        writes=writes or (_write(),),
    )
    values.update(overrides)
    return MssqlWorkspaceObservationRequest(**values)


def _connection():
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host="fixture", database="warehouse", username="fixture", password="never-report"
        ),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor("mssql", {}),
    )


def test_input_macro_drift_is_rejected_before_connecting():
    with pytest.raises(WorkspaceObservationError, match="macro_authority"):
        replace(_request(), macro_authority_sha256="sha256:" + "c" * 64)


@pytest.mark.parametrize("name", ["", "x" * 129, "😀" * 65, "bad\x00name", "bad'name", "bad]name"])
def test_unrepresentable_dbt_names_are_rejected(name):
    with pytest.raises((ValueError, DbtPublishingError)):
        _request(_write(name))


@pytest.mark.parametrize("location", ["pin", "default", "invocation", "write"])
def test_database_name_rewritten_by_pinned_use_macro_is_rejected(location):
    if location == "pin":
        pin = replace(PIN, database_name='ware"house')
        with pytest.raises(WorkspaceObservationError, match="database_identifier"):
            _request(pin=pin)
    elif location == "default":
        with pytest.raises(WorkspaceObservationError, match="database_identifier"):
            _request(default_database='ware"house')
    elif location == "invocation":
        with pytest.raises(WorkspaceObservationError, match="database_identifier"):
            _request(invocation_databases=('ware"house',))
    else:
        with pytest.raises(WorkspaceObservationError, match="database_identifier"):
            _request(_write(database='ware"house'))


def test_large_project_reserves_helpers_without_a_toy_slot_limit():
    request = _request(*(_write(f"relation_{index}") for index in range(197 * 5)))
    assert len(request.writes) == 985


@pytest.mark.parametrize(
    "databases", [(), ["warehouse"], ("warehouse", "warehouse"), tuple(f"db{i}" for i in range(65))]
)
def test_invocation_database_set_is_nonempty_unique_tuple_with_a_bounded_size(databases):
    with pytest.raises(WorkspaceObservationError, match="invocation_databases"):
        _request(invocation_databases=databases)


@pytest.mark.parametrize(
    "field", ["max_slots", "max_nodes", "max_edges", "max_depth", "max_json_bytes", "timeout_seconds"]
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_observation_budgets_are_positive_bounded_integers(field, value):
    with pytest.raises(WorkspaceObservationError, match="limits"):
        replace(WorkspaceObservationLimits(), **{field: value})


def _header():
    return dict(
        database_id=7,
        database_name="warehouse",
        database_reference_mismatch_count=0,
        containment=0,
        compatibility_level=160,
        engine_edition=3,
        engine_version="16.0.1",
        server_name="fixture",
        machine_name="fixture",
        instance_name=None,
        physical_name="fixture",
        server_collation="Latin1_General_100_CI_AS",
        database_collation="Latin1_General_100_CS_AS",
        catalog_collation="Latin1_General_100_CS_AS",
        can_view_definition=1,
        can_select_dependencies=1,
        original_sid=b"login",
        effective_sid=b"login",
        database_sid=b"principal",
    )


def _slot(slot_id, **overrides):
    row = dict(
        slot_id=slot_id,
        equivalence_class=slot_id + 1,
        database_id=7,
        schema_id=3,
        schema_name="mart",
        object_id=None,
        object_name=None,
        object_type=None,
        is_ms_shipped=None,
        create_token=None,
        modify_token=None,
        literal_roundtrip=1,
    )
    return row | overrides


def _edge(frontier_id, *, child="Downstream", parent="Orders", **overrides):
    row = dict(
        frontier_id=frontier_id,
        object_id=100 + frontier_id,
        schema_name="mart",
        object_name=child,
        create_token="2026-01-01T00:00:00.000",
        modify_token="2026-01-01T00:00:00.000",
        referenced_server=None,
        referenced_database="warehouse",
        referenced_schema="mart",
        referenced_entity=parent,
        referenced_id=None,
        referencing_minor_id=0,
        referenced_minor_id=0,
        literal_roundtrip=1,
    )
    return row | overrides


class ScriptedCatalog:
    """Script only database responses; execute the real observer and pin verifier."""

    def __init__(self, *, header=None, slots=None, incoming=None, fail=None):
        self.header = _header() if header is None else header
        self.slots = slots
        self.incoming = incoming or (lambda frontier: [])
        self.fail = fail
        self.events = []
        self.parameters = []

    def _event(self, event):
        self.events.append(event)
        if self.fail == event:
            raise RuntimeError("driver secret=never-report")

    def begin(self):
        self._event("begin")

    def rollback(self):
        self._event("rollback")

    def close(self):
        self._event("close")

    @contextmanager
    def bounded_query_timeout(self, seconds):
        assert 1 <= seconds <= 10
        yield

    def get_records(self, sql, params=None, *, as_dict=False):
        assert as_dict
        self.parameters.append((sql, params))
        if "workspace:header" in sql:
            self._event("header")
            return [self.header.copy()]
        if "workspace:slots" in sql:
            self._event("slots")
            payload = json.loads(params[-1])
            return self.slots if self.slots is not None else [_slot(i) for i in range(len(payload))]
        if "workspace:incoming" in sql:
            self._event("incoming")
            return self.incoming(json.loads(params[-1]))
        if "VIEW ANY DATABASE" in sql:
            return [dict(permitted=1, is_sysadmin=0)]
        if "database_recovery_status" in sql:
            return [
                dict(
                    database_id=7,
                    database_name="warehouse",
                    state_desc="ONLINE",
                    user_access_desc="MULTI_USER",
                    has_dbaccess=1,
                    create_token=PIN.create_token,
                    database_guid=str(PIN.database_guid),
                )
            ]
        raise AssertionError(f"unexpected query: {sql}")


def _observe(catalog, request=None, *, clock=lambda: 0):
    def connect(connection, database):
        assert database == "warehouse"
        assert 1 <= connection.credentials.connect_timeout <= 10
        catalog._event("connect")
        return catalog

    return MssqlWorkspaceCatalogObserver(connector_factory=connect, clock=clock).observe(
        request or _request(), _connection()
    )


def test_complete_read_observation_keeps_subjects_and_closes_without_commit():
    catalog = ScriptedCatalog()
    request = _request(_write("Orders"), _write("orders"))
    result = _observe(catalog, request)
    assert result.request == request
    assert result.subject_sha256 == request.subject_sha256
    assert [slot.equivalence_class for slot in result.slots] == [1, 2]
    assert result.dependencies == ()
    assert result.header.database_id == 7
    assert result.header.original_login_sid_sha256.startswith("sha256:")
    assert "never-report" not in repr(result)
    assert catalog.events[-2:] == ["rollback", "close"]


def test_header_verifies_every_invocation_database_through_sql_server():
    catalog = ScriptedCatalog()
    _observe(catalog, _request(invocation_databases=("warehouse", "WAREHOUSE")))
    header_parameters = [params for sql, params in catalog.parameters if "workspace:header" in sql]
    assert len(header_parameters) == 2
    assert json.loads(header_parameters[0][0]) == [{"database": "warehouse"}, {"database": "WAREHOUSE"}]
    assert header_parameters[1] == header_parameters[0]


def test_request_default_database_must_match_resolved_connection_exactly():
    catalog = ScriptedCatalog()
    with pytest.raises(WorkspaceObservationError, match="database_context"):
        _observe(catalog, _request(default_database="WAREHOUSE"))
    assert catalog.events == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("database_id", 8),
        ("database_reference_mismatch_count", 1),
        ("database_name", "other"),
        ("containment", 1),
        ("compatibility_level", 120),
        ("engine_edition", 5),
        ("can_view_definition", 0),
        ("can_select_dependencies", None),
        ("original_sid", b""),
        ("effective_sid", b"other"),
        ("database_sid", None),
        ("catalog_collation", None),
    ],
)
def test_unavailable_or_unsupported_header_never_returns_observation(field, value):
    catalog = ScriptedCatalog(header=_header() | {field: value})
    with pytest.raises(WorkspaceObservationError):
        _observe(catalog)
    assert catalog.events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize("fail", ["begin", "header", "slots", "incoming", "rollback", "close"])
def test_driver_and_cleanup_failures_are_detached_and_never_success(fail):
    catalog = ScriptedCatalog(fail=fail)
    with pytest.raises(WorkspaceObservationError) as caught:
        _observe(catalog)
    assert "never-report" not in str(caught.value)
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert catalog.events[-1] == "close"


def test_incoming_diamond_preserves_all_raw_rows_but_reads_each_view_once():
    calls = []

    def incoming(frontier):
        calls.append(frontier)
        relations = [item["relation"] for item in frontier]
        if relations == ["Orders"]:
            return [_edge(0, child="Left"), _edge(0, child="Right", object_id=102)]
        if relations == ["Left", "Right"]:
            return [
                _edge(0, child="Bottom", parent="Left", object_id=103),
                _edge(1, child="Bottom", parent="Right", object_id=103),
            ]
        assert relations == ["Bottom"]
        return []

    result = _observe(ScriptedCatalog(incoming=incoming))
    assert len(result.dependencies) == 4
    assert [item["relation"] for call in calls for item in call] == ["Orders", "Left", "Right", "Bottom"]


def test_dependency_cycle_fails_closed_and_cleans_up():
    def incoming(frontier):
        relation = frontier[0]["relation"]
        return [_edge(0, child="Orders", parent="Loop")] if relation == "Loop" else [_edge(0, child="Loop")]

    catalog = ScriptedCatalog(incoming=incoming)
    with pytest.raises(WorkspaceObservationError, match="dependency_cycle"):
        _observe(catalog)
    assert catalog.events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize(
    "budget,reason", [("max_edges", "edge_budget"), ("max_nodes", "node_budget"), ("max_depth", "depth_budget")]
)
def test_dependency_budgets_fail_instead_of_truncating(budget, reason):
    limits = replace(WorkspaceObservationLimits(), **{budget: 1})
    request = _request(limits=limits)

    def incoming(frontier):
        relation = frontier[0]["relation"]
        if relation == "Orders":
            return (
                [_edge(0, child="A")]
                if budget == "max_depth"
                else [_edge(0, child="A"), _edge(0, child="B", object_id=102)]
            )
        return [_edge(0, child="B", parent="A", object_id=102)] if budget == "max_depth" else []

    with pytest.raises(WorkspaceObservationError, match=reason):
        _observe(ScriptedCatalog(incoming=incoming), request)


def test_every_named_or_default_write_database_must_resolve_to_pinned_database():
    request = _request(_write("Orders"), _write("Other", database="different"))
    rows = [_slot(0), _slot(1, database_id=8)]
    with pytest.raises(WorkspaceObservationError, match="database_context"):
        _observe(ScriptedCatalog(slots=rows), request)


def test_transfer_skips_dbt_literal_policy_but_not_physical_database_identity():
    transfer = _write("Transferred", kind="transfer")
    result = _observe(ScriptedCatalog(slots=[_slot(0, literal_roundtrip=0)]), _request(transfer))
    assert result.slots[0].slot_id == 0


def test_duplicate_or_missing_slot_rows_never_look_complete():
    request = _request(_write("Orders"), _write("Other"))
    with pytest.raises(WorkspaceObservationError, match="slot_completeness"):
        _observe(ScriptedCatalog(slots=[_slot(0), _slot(0)]), request)


def test_existing_object_type_outside_table_or_view_is_unsupported():
    row = _slot(
        0,
        object_id=42,
        object_name="Orders",
        object_type="P",
        is_ms_shipped=0,
        create_token="2026-01-01T00:00:00.000",
        modify_token="2026-01-01T00:00:00.000",
    )
    with pytest.raises(WorkspaceObservationError, match="object_capability"):
        _observe(ScriptedCatalog(slots=[row]))


def test_json_budget_accepts_exact_bytes_and_rejects_one_byte_less_before_connect():
    baseline = ScriptedCatalog()
    request = _request()
    _observe(baseline, request)
    json_payloads = [
        params[-1] for sql, params in baseline.parameters if "workspace:slots" in sql or "workspace:incoming" in sql
    ]
    exact = max(len(payload.encode("utf-8")) for payload in json_payloads)
    _observe(ScriptedCatalog(), replace(request, limits=replace(request.limits, max_json_bytes=exact)))
    overflow = ScriptedCatalog()
    with pytest.raises(WorkspaceObservationError, match="json_budget"):
        _observe(overflow, replace(request, limits=replace(request.limits, max_json_bytes=exact - 1)))
    assert overflow.events[-2:] == ["rollback", "close"]
    slot_size = len(json_payloads[0].encode("utf-8"))
    never_connected = ScriptedCatalog()
    with pytest.raises(WorkspaceObservationError, match="json_budget"):
        _observe(never_connected, replace(request, limits=replace(request.limits, max_json_bytes=slot_size - 1)))
    assert never_connected.events == []


def test_elapsed_deadline_discards_complete_rows_and_still_cleans_up():
    class Clock:
        value = 0.0

        def __call__(self):
            self.value += 0.12
            return self.value

    catalog = ScriptedCatalog()
    request = _request(limits=replace(WorkspaceObservationLimits(), timeout_seconds=1))
    with pytest.raises(WorkspaceObservationError, match="deadline"):
        _observe(catalog, request, clock=Clock())
    assert catalog.events[-2:] == ["rollback", "close"]
