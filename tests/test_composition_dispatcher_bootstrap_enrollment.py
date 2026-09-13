"""Bootstrap SQL-only boundaries: distinct services, exact originals and cleanup."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_bootstrap_enrollment as module
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_dispatcher_binding import CompositionDispatcherBinding
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.dbt_relation_writes import transfer_relation_write
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_custody_enrollment import v2_body
from tests.test_composition_clickhouse_supervisor_enrollment import enrolled
from tests.test_composition_dispatcher_service_config import DIGEST, IDENTIFIER, decode, document


def harness(monkeypatch, *, authorities=1, targets=1):
    body = v2_body()
    body["facts"]["linux"]["capture_custody"] = {
        "identity_maps": {"uid_map": [[0, 0, 4294967295]], "gid_map": [[0, 0, 4294967295]]},
        "root_identity": {"device": 1, "inode": 8, "uid": 101, "gid": 101, "mode": 448},
    }
    enrollment = enrolled(body)
    value = document()
    value.update(
        dispatcher_uid=101,
        dispatcher_gid=101,
        supervisor_enrollment_sha256=enrollment.enrollment_sha256,
        capture_root="/capture",
        capture_root_identity=body["facts"]["linux"]["capture_custody"]["root_identity"],
    )
    if authorities == 2:
        value["authorities"]["sha256:" + "b" * 64] = deepcopy(value["authorities"][DIGEST])
    config = decode(value, bootstrap_uid=101, bootstrap_gid=101)
    events, contexts = [], {}
    manifest = {
        "name": "copy",
        "source": {"type": "mssql", "connection_ref": "source", "table": {"schema": "dbo", "name": "orders"}},
        "sink": {
            "type": "clickhouse",
            "connection_ref": "target",
            "table": {"schema": "analytics", "name": "orders"},
            "strategy": {"mode": "full_refresh"},
        },
    }
    write = transfer_relation_write(project_path=".", workflow_id="flow", workload_id="copy", manifest=manifest)
    plan = SimpleNamespace(
        writes=(write,), sources=SimpleNamespace(transfer_manifests=(("copy", canonical_json_bytes(manifest)),))
    )
    entries = {
        "control": {"type": "mssql"},
        "source": {"type": "mssql"},
        "target": {
            "type": "clickhouse",
            "connection": {
                "composition_service_id": body["service_id"],
                "database": "analytics",
                "database_authorities": {"analytics": {"database_uuid": body["database_uuid"]}},
            },
        },
    }
    bindings = {name: {"connection_ref": name} for name in entries}
    control = SimpleNamespace(
        descriptor=SimpleNamespace(connection_type="mssql", properties={"composition_service_id": IDENTIFIER})
    )

    def resolve(reference):
        events.append(("resolve", reference))
        assert reference == "control", "resolved business credentials"
        return control

    for authority in config.authorities:
        occurrence = CompositionOccurrenceContext(IDENTIFIER, "prod", DIGEST, DIGEST, None, authority)
        runtime = SimpleNamespace(
            binding_set={"bindings": bindings},
            connection_registry={"connections": entries},
            resolver=SimpleNamespace(resolve=resolve),
            authority_subject_sha256=authority,
        )
        context = SimpleNamespace(
            occurrence=occurrence,
            runtime=runtime,
            plan=plan,
            target_binding_ref="target",
            binding=CompositionDispatcherBinding(config.dispatcher_id, "api", config.binding_identity_sha256),
        )
        contexts[authority] = tuple(context for _ in range(targets))

    class Cursor:
        def execute(self, sql, *args):
            events.append(("query", sql))

        def fetchall(self):
            domain = module.clickhouse_domain_for_write(entries["target"]["connection"], write)
            return ((domain.connector, domain.service_id, domain.physical_subject_sha256),)

        def close(self):
            events.append("cursor-close")

    class Connection:
        autocommit = False

        def cursor(self):
            return cursor

        def rollback(self):
            events.append("rollback")

        def close(self):
            events.append("connection-close")

    cursor, connection = Cursor(), Connection()
    returned_service = [IDENTIFIER]

    def connections(**kwargs):
        def opened(context):
            kwargs["inputs"].resolve_connection(context, "control")
            events.append("open")
            return connection, returned_service[0]

        return SimpleNamespace(control_connection_with_service=opened)

    class Ledger:
        def __init__(self, cursor, schema):
            self.cursor, self.schema = cursor, schema

        def begin(self, service):
            events.append(("begin", service))
            return 7

        def require_transaction(self, transaction=None):
            assert transaction in (None, 7)
            return 7

        def table(self, name):
            return "[dpone_control].[composition_" + name + "]"

    monkeypatch.setattr(module, "CompositionAuthorityConnections", connections)
    monkeypatch.setattr(module, "CompositionMssqlLedger", Ledger)
    monkeypatch.setattr(
        module, "read_service_enrollment", lambda ledger, service: events.append(("enrollment", service)) or enrollment
    )
    monkeypatch.setattr(module.time, "monotonic", lambda: 100.0)
    loader = SimpleNamespace(load_bootstrap=lambda authority: events.append(("load", authority)) or contexts[authority])
    return SimpleNamespace(
        config=config,
        loader=loader,
        contexts=contexts,
        entries=entries,
        bindings=bindings,
        enrollment=enrollment,
        events=events,
        connection=connection,
        cursor=cursor,
        returned_service=returned_service,
    )


def test_all_authorities_and_contexts_use_control_only_and_distinct_service_identity(monkeypatch):
    case = harness(monkeypatch, authorities=2, targets=2)
    observed = module.read_bootstrap_enrollment(case.config, case.loader, 130.0)
    assert observed == case.enrollment
    assert case.enrollment.body["service_id"] != IDENTIFIER
    assert case.events.count("connection-close") == 4
    assert case.events.count("rollback") == 4
    assert all(event[1] == IDENTIFIER for event in case.events if isinstance(event, tuple) and event[0] == "begin")
    assert len([event for event in case.events if isinstance(event, tuple) and event[0] == "load"]) == 2


@pytest.mark.parametrize("alias", ["source", "target"])
def test_control_registry_alias_collision_rejects_before_resolution(monkeypatch, alias):
    case = harness(monkeypatch)
    case.bindings["control"]["connection_ref"] = alias
    with pytest.raises(CompositionAdmissionError):
        module.read_bootstrap_enrollment(case.config, case.loader, 130.0)
    assert not any(event[0] == "resolve" for event in case.events if isinstance(event, tuple))


@pytest.mark.parametrize("failure", ["service", "database", "domain", "enrollment", "changed", "query", "cleanup"])
def test_failed_sql_observation_closes_every_acquired_handle(monkeypatch, failure):
    case = harness(monkeypatch)
    if failure == "service":
        case.returned_service[0] = case.enrollment.body["service_id"]
    elif failure == "database":
        case.entries["target"]["connection"]["database_authorities"]["analytics"]["database_uuid"] = (
            "22222222-2222-4222-8222-222222222222"
        )
    elif failure == "domain":
        case.cursor.fetchall = lambda: ()
    elif failure in {"enrollment", "changed"}:
        body = case.enrollment.body
        body["facts"]["linux"]["capture_custody"]["root_identity"]["inode"] += 1
        foreign = enrolled(body)
        values = iter([foreign] if failure == "enrollment" else [case.enrollment, foreign])
        monkeypatch.setattr(module, "read_service_enrollment", lambda *args: next(values))
    elif failure == "query":
        case.cursor.execute = lambda *args: (_ for _ in ()).throw(RuntimeError("secret"))
    else:

        def failed_close():
            case.events.append("cursor-close")
            raise RuntimeError("secret")

        case.cursor.close = failed_close
    with pytest.raises(CompositionAdmissionError) as error:
        module.read_bootstrap_enrollment(case.config, case.loader, 130.0)
    assert "secret" not in str(error.value)
    assert case.events[-1] == "connection-close"
    assert "rollback" in case.events


@pytest.mark.parametrize("deadline", [100.0, float("nan"), float("inf"), True])
def test_expired_or_invalid_deadline_cannot_open_control(monkeypatch, deadline):
    case = harness(monkeypatch)
    with pytest.raises(CompositionAdmissionError):
        module.read_bootstrap_enrollment(case.config, case.loader, deadline)
    assert case.events == []


def test_empty_bootstrap_contexts_cannot_pass(monkeypatch):
    case = harness(monkeypatch)
    case.contexts[DIGEST] = ()
    with pytest.raises(CompositionAdmissionError):
        module.read_bootstrap_enrollment(case.config, case.loader, 130.0)


@pytest.mark.parametrize(
    "field,value", [("dispatcher_uid", 102), ("capture_custody", "foreign"), ("capture_root", "/foreign")]
)
def test_shared_enrollment_comparison_preserves_exact_custody_configuration(monkeypatch, field, value):
    from dataclasses import replace

    from dpone.app.composition_dispatcher_enrollment_validation import require_dispatcher_enrollment

    case = harness(monkeypatch)
    with pytest.raises(CompositionAdmissionError):
        require_dispatcher_enrollment(
            replace(case.config, **{field: value}), case.enrollment, case.enrollment.body["service_id"]
        )
    assert case.events == []


def test_deadline_expiring_during_sql_read_rejects_after_cleanup(monkeypatch):
    case = harness(monkeypatch)
    reads = []

    def read(ledger, service):
        reads.append(service)
        if len(reads) == 2:
            monkeypatch.setattr(module.time, "monotonic", lambda: 131.0)
        return case.enrollment

    monkeypatch.setattr(module, "read_service_enrollment", read)
    with pytest.raises(CompositionAdmissionError):
        module.read_bootstrap_enrollment(case.config, case.loader, 130.0)
    assert reads == [case.enrollment.body["service_id"]] * 2
    assert case.events[-3:] == ["rollback", "cursor-close", "connection-close"]
