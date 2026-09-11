"""Offline ClickHouse gate state/transaction failures; no live certification."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.adapters import composition_clickhouse_dispatch_queries as dispatch_queries
from dpone.adapters import composition_clickhouse_gate_queries as gate_queries
from dpone.adapters.composition_clickhouse_dispatch_queries import document_sha256
from dpone.adapters.composition_clickhouse_gate import MssqlClickHouseGate
from dpone.adapters.composition_clickhouse_gate_queries import (
    ClickHouseLocalSupervisorObservation,
    ClickHouseSupervisorObservation,
)
from dpone.adapters.composition_clickhouse_principal import IssuedClickHouseCredentials
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchObservation
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn, CreateGenerationDispatch
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
from tests.composition_mssql_store_fault_model import Connection, Cursor, FaultDatabase
from tests.composition_snapshot_helpers import intent, occurrence


def test_supervisor_rejects_digest_without_original():
    with pytest.raises(CompositionAdmissionError):
        ClickHouseSupervisorObservation(
            *(["11111111-1111-1111-1111-111111111111"] * 4), "127.0.0.1", "sha256:" + "a" * 64, b"{}"
        )


def test_gate_requires_real_dependencies():
    with pytest.raises(TypeError):
        MssqlClickHouseGate(lambda: None)


# The existing SQL transaction/history model is reused; only this additive
# catalog and external ClickHouse/supervisor observations are offline doubles.


SQL_SERVICE = "10000000-0000-4000-8000-000000000002"


class GateCursor(Cursor):
    def _select_or_mutate(self, sql, p):
        data = self.connection.data
        if "composition_issued_authorities]" in sql and "connector='clickhouse'" in sql:
            return [
                (key,)
                for key, rows in data["issued_authorities"].items()
                for connector, service, principal in rows
                if (connector, service, principal) == ("clickhouse", p[0], p[1])
            ][:2]
        if sql.startswith("INSERT") and "composition_issued_authorities]" in sql:
            values = data["issued_authorities"].get(p[0], ())
            assert p[1:] not in values
            data["issued_authorities"][p[0]] = (*values, p[1:])
            return []
        if sql.startswith("INSERT") and "composition_proofs]" in sql:
            assert p[:3] not in data["proofs"]
            data["proofs"][p[:3]] = p[3:]
            return []
        if "composition_ch_" not in sql:
            return super()._select_or_mutate(sql, p)
        assert self.connection.transaction_id == self.connection.database.lock_owner
        for table in ("ch_gates", "ch_gate_bindings", "ch_gate_events"):
            if f"composition_{table}]" not in sql:
                continue
            key = p[:2] if table == "ch_gate_events" else p[0]
            if sql.startswith("INSERT"):
                assert key not in data[table]
                data[table][key] = p
                return []
            found = data[table].get(key)
            if found is None:
                return []
            if "SELECT TOP (2) operation_key,login_name" in sql:
                return [found[1:3]]
            if "SELECT TOP (2) LOWER(CONVERT" in sql:
                return [(found[1],)]
            return [found[-2:]]
        if sql.startswith("INSERT"):
            if "composition_ch_dispatches]" in sql:
                assert p[0] not in data["dispatches"] and not any(
                    r[1] == p[1] or r[4] == p[4] for r in data["dispatches"].values()
                )
                assert not any(k[0] == p[3] for k in data["closures"])
                data["dispatches"][p[0]] = p
            elif "composition_ch_dispatch_terminals]" in sql:
                assert p[0] in data["dispatches"] and p[0] not in data["terminals"]
                data["terminals"][p[0]] = p[1:]
            else:
                assert p[:2] not in data["closures"]
                data["closures"][p[:2]] = p[2:]
            return []
        if "composition_ch_dispatch_closures]" in sql:
            if len(p) == 1:
                return [(phase,) for gate, phase in data["closures"] if gate == p[0]][:3]
            found = data["closures"].get(p)
            return [] if found is None else [found]
        if "composition_ch_dispatch_terminals]" in sql:
            found = data["terminals"].get(p[0])
            return [] if found is None else [found]
        rows = sorted(data["dispatches"].values())
        if "WHERE claim_key" in sql:
            return [r for r in rows if r[1] == p[0] or r[0] == p[1] or r[4] == p[2]][:4]
        if "WHERE gate_id" in sql:
            return [r for r in rows if r[3] == p[0] and (len(p) == 1 or r[0] > p[1])][:1]
        return [r for r in rows if r[0] == p[0]][:2]


class GateConnection(Connection):
    def cursor(self):
        return GateCursor(self)


class GateDatabase(FaultDatabase):
    def connect(self):
        connection = GateConnection(self)
        self.connections.append(connection)
        return connection


class Supervisor:
    def __init__(self, value):
        fields = dict(
            service_id=value.target.service_id,
            database_uuid=value.target.database_id,
            boot_id=str(UUID(int=701)),
            isolation_id=str(UUID(int=702)),
            supervisor_ip="10.1.0.2",
        )
        document = canonical_json_bytes(
            {
                "schema": "dpone.composition-clickhouse-supervisor.v1",
                **fields,
                "facts": {"offline_fixture": "never live authority"},
            }
        )
        self.value = ClickHouseSupervisorObservation(
            **fields, evidence_sha256=document_sha256(document), evidence_document=document
        )
        self.change = None

    def observe(self, context, **_):
        if self.change:
            self.change(context)
        return self.value


class Admin:
    def __init__(self):
        self.users, self.events, self.fail = {}, [], None
        self.callback = None

    def _event(self, phase):
        self.events.append(phase)
        if self.callback:
            self.callback(phase)
        if self.fail == phase:
            raise CompositionAdmissionError("offline_admin_unknown")

    def require_target(self, target):
        target.__post_init__()

    def create_disabled(self, name, password):
        self._event("create")
        assert name not in self.users
        self.users[name] = [str(UUID(int=801 + len(self.users))), None, False]
        return self.users[name][0]

    def grant_disabled(self, name, user, target, purpose):
        self._event("grant")
        assert self.users[name][:2] == [user, None]

    def enable(self, name, user, host):
        self.users[name][1] = host
        self._event("enable")

    def enable_local(self, name, user):
        self.enable(name, user, "LOCAL")

    def observe_local(self, name, user, target, purpose):
        return self.observe(name, user, target, purpose, host="LOCAL", revoked=False)

    def observe(self, name, user, target, purpose, *, host, revoked):
        self._event("observe")
        assert self.users[name] == [user, host, revoked]
        return {"username": name, "user_id": user, "host": host, "revoked": revoked}

    def revoke(self, name, user):
        self._event("revoke")
        assert self.users[name][0] == user
        self.users[name][1:] = [None, True]

    def require_quiescence(self, name):
        self._event("quiescence")


class Policy:
    def __init__(self):
        self.change = None

    def require_dispatch(self, context, dispatch):
        if self.change:
            self.change(context)


@pytest.fixture
def active(monkeypatch):
    install_offline_catalog_references(monkeypatch)
    monkeypatch.setattr(gate_queries, "require_clickhouse_gate_schema", lambda *_: None)
    monkeypatch.setattr(dispatch_queries, "require_clickhouse_dispatch_schema", lambda *_: None)
    value = intent()
    db = GateDatabase(occurrence().request, SQL_SERVICE)
    db.data.update(ch_gates={}, ch_gate_bindings={}, ch_gate_events={}, dispatches={}, terminals={}, closures={})
    activation = MssqlCompositionActivationStore(db.connect, expected_service_id=SQL_SERVICE)
    activation.prepare(db.request)
    activation.activate(db.request)
    MssqlCompositionAttemptStore(db.connect, expected_service_id=SQL_SERVICE).admit_once(value.attempt)
    admin, supervisor, policy = Admin(), Supervisor(value), Policy()
    gate = MssqlClickHouseGate(
        db.connect,
        expected_service_id=SQL_SERVICE,
        target=value.target,
        purpose="ingest",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
    )
    return db, value, gate, admin, supervisor, policy


def completed(dispatch):
    return ClickHouseDispatchObservation(
        dispatch.dispatch_sha256, dispatch.claim_key, dispatch.query_id, 0, 0, document_sha256(b""), "content-length"
    )


def make_dispatch(value):
    return CreateGenerationDispatch(
        value.attempt, value.target, value.generation.new_generation_uuid, (ClickHouseDispatchColumn("id", "Int32"),)
    )


def test_real_sql_lifecycle_retains_exact_uuid_and_proof_originals(active):
    db, value, gate, admin, _, _ = active
    credentials = gate.issue_once(value.attempt)
    assert isinstance(credentials, IssuedClickHouseCredentials)
    assert credentials.password not in repr(db.data)
    assert db.data["issued_authorities"][value.attempt.attempt_sha256] == (
        ("clickhouse", value.target.service_id, "clickhouse-user:" + credentials.user_id),
    )
    journal, dispatch = gate.journal(value.attempt, credentials.user_id), make_dispatch(value)
    journal.claim_once(dispatch)
    journal.record_completed(dispatch, completed(dispatch))
    closed, quiet = gate.close(value.attempt), gate.prove_quiescence(value.attempt)
    assert closed.kind == "CLOSED_GATES" and quiet.kind == "QUIESCENCE"
    assert closed.authorities == quiet.authorities
    assert len(db.data["proofs"]) == 2 and len(db.data["closures"]) == 2
    assert gate.close(value.attempt) == closed
    assert admin.users[credentials.username][1:] == [None, True]
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    with pytest.raises(CompositionAdmissionError):
        journal.claim_once(dispatch)


@pytest.mark.parametrize("phase", ["create", "grant", "enable", "observe"])
def test_unknown_issuance_never_redelivers_or_certifies_closure(active, phase):
    db, value, gate, admin, _, _ = active
    admin.fail = phase
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    admin.fail = None
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    with pytest.raises(CompositionAdmissionError):
        gate.close(value.attempt)
    assert not db.data["proofs"]
    assert admin.events.count("create") == 1


def test_lost_intent_commit_ack_never_creates_or_replays(active):
    db, value, gate, admin, _, _ = active
    db.fail_commit = True
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    db.fail_commit = False
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    assert db.data["ch_gates"] and not admin.events


def test_paused_presend_request_blocks_revocation_and_closed(active):
    db, value, gate, admin, _, _ = active
    credentials = gate.issue_once(value.attempt)
    dispatch, journal = make_dispatch(value), gate.journal(value.attempt, credentials.user_id)
    journal.claim_once(dispatch)
    with pytest.raises(CompositionAdmissionError, match="unresolved"):
        gate.close(value.attempt)
    assert "revoke" not in admin.events and not db.data["proofs"]
    assert not any(phase == "CLOSED" for _, phase in db.data["closures"])
    journal.record_completed(dispatch, completed(dispatch))
    assert gate.close(value.attempt).kind == "CLOSED_GATES"


@pytest.mark.parametrize("stage", ["issue", "dispatch", "close"])
def test_supervisor_cannot_switch_actual_sql_transaction(active, stage):
    db, value, gate, admin, supervisor, policy = active
    if stage != "issue":
        credentials = gate.issue_once(value.attempt)

    def change(context):
        context.cursor.connection.commit()
        context.begin(SQL_SERVICE)

    supervisor.change = change
    with pytest.raises(CompositionAdmissionError, match="transaction_identity"):
        if stage == "issue":
            gate.issue_once(value.attempt)
        elif stage == "dispatch":
            gate.journal(value.attempt, credentials.user_id).claim_once(make_dispatch(value))
        else:
            gate.close(value.attempt)
    assert not db.data["proofs"]


def test_budget_callback_cannot_replace_transaction(active):
    db, value, gate, _, _, policy = active
    credentials = gate.issue_once(value.attempt)

    def change(context):
        context.cursor.connection.commit()
        context.begin(SQL_SERVICE)

    policy.change = change
    with pytest.raises(CompositionAdmissionError, match="transaction_identity"):
        gate.journal(value.attempt, credentials.user_id).claim_once(make_dispatch(value))
    assert not db.data["dispatches"]


def test_changed_supervisor_original_blocks_closure(active):
    db, value, gate, _, supervisor, _ = active
    gate.issue_once(value.attempt)
    other = Supervisor(value).value
    from dpone.contracts.strict_json import strict_json_object

    body = strict_json_object(other.evidence_document)
    body["boot_id"] = str(UUID(int=999))
    document = canonical_json_bytes(body)
    supervisor.value = replace(
        other, boot_id=body["boot_id"], evidence_document=document, evidence_sha256=document_sha256(document)
    )
    with pytest.raises(CompositionAdmissionError, match="subject_changed"):
        gate.close(value.attempt)
    assert not db.data["proofs"]


def test_two_purposes_retain_both_issued_principals_and_proofs(active):
    db, value, gate, admin, supervisor, policy = active
    ingest = gate.issue_once(value.attempt)
    publisher_gate = MssqlClickHouseGate(
        db.connect,
        expected_service_id=SQL_SERVICE,
        target=value.target,
        purpose="publisher",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
    )
    publisher = publisher_gate.issue_once(value.attempt)
    assert ingest.user_id != publisher.user_id
    gate.close(value.attempt)
    publisher_gate.close(value.attempt)
    assert len(db.data["issued_authorities"][value.attempt.attempt_sha256]) == 2
    assert len(db.data["proofs"]) == 4


def test_deleted_proof_original_is_not_recreated_by_readback(active):
    db, value, gate, _, _, _ = active
    gate.issue_once(value.attempt)
    gate.close(value.attempt)
    db.data["proofs"].clear()
    with pytest.raises(CompositionAdmissionError):
        gate.prove_quiescence(value.attempt)


@pytest.mark.parametrize("phase", ["create", "grant", "enable"])
def test_lost_later_sql_commit_ack_never_returns_credentials(active, phase):
    db, value, gate, admin, _, _ = active

    def lose_ack(observed):
        if observed == phase:
            db.fail_commit = True

    admin.callback = lose_ack
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    db.fail_commit, admin.callback = False, None
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    assert admin.events.count("create") == 1
    if phase != "enable":
        with pytest.raises(CompositionAdmissionError):
            gate.close(value.attempt)
        assert not db.data["proofs"]
    else:
        # The administrator enable response completed before the lost READY SQL
        # ACK, so durable READY can be reconciled for closure, never issuance.
        assert gate.close(value.attempt).kind == "CLOSED_GATES"


@pytest.mark.parametrize("change", ["issued", "binding", "gate_hash", "ready_hash"])
def test_changed_durable_original_rejects_closure(active, change):
    db, value, gate, _, _, _ = active
    gate.issue_once(value.attempt)
    if change == "issued":
        db.data["issued_authorities"].clear()
    elif change == "binding":
        key, row = next(iter(db.data["ch_gate_bindings"].items()))
        db.data["ch_gate_bindings"][key] = (row[0], str(UUID(int=999)), *row[2:])
    else:
        table = "ch_gates" if change == "gate_hash" else "ch_gate_events"
        key, row = next((k, v) for k, v in db.data[table].items() if table == "ch_gates" or k[1] == "READY")
        db.data[table][key] = (*row[:-2], "sha256:" + "f" * 64, row[-1])
    with pytest.raises(CompositionAdmissionError):
        gate.close(value.attempt)
    assert not db.data["proofs"]


def test_closure_racing_enable_never_allows_ready_or_false_proof(active):
    db, value, gate, admin, _, _ = active

    def close_during_enable(phase):
        if phase == "enable":
            with pytest.raises(CompositionAdmissionError, match="enable_unresolved"):
                gate.close(value.attempt)

    admin.callback = close_during_enable
    with pytest.raises(CompositionAdmissionError, match="enable_closed"):
        gate.issue_once(value.attempt)
    assert not db.data["proofs"]
    assert not any(phase == "READY" for _, phase in db.data["ch_gate_events"])


@pytest.mark.parametrize("table", ["ch_gates", "ch_gate_bindings", "ch_gate_events"])
@pytest.mark.parametrize(
    "part", ["table", "columns", "keys", "foreign_keys", "checks", "objects", "triggers", "events", "row_security"]
)
def test_additive_gate_catalog_rejects_every_unexpected_projection(table, part):
    from dpone.adapters.composition_clickhouse_gate_schema import (
        GATE_TABLES,
        gate_trigger_sql,
        require_clickhouse_gate_schema,
    )
    from dpone.adapters.composition_mssql_catalog_types import CompositionTrigger
    from tests.composition_mssql_catalog_helpers import Cursor as CatalogCursor
    from tests.composition_mssql_catalog_helpers import expected_rows

    def trigger(schema, name):
        return CompositionTrigger(
            "composition_" + name + "_invariant", gate_trigger_sql(schema, name), ("DELETE", "INSERT", "UPDATE")
        )

    cursor = CatalogCursor(expected_rows(tables=GATE_TABLES, definitions={}, metadata={}, trigger_for=trigger))
    require_clickhouse_gate_schema(cursor, "control")
    cursor.rows[f"[control].[composition_{table}]", part] += (("unexpected",),)
    with pytest.raises(CompositionAdmissionError):
        require_clickhouse_gate_schema(cursor, "control")


def test_gate_schema_is_external_append_only_with_fenced_transition_rules():
    from dpone.adapters.composition_clickhouse_gate_schema import render_clickhouse_gate_schema

    ddl = render_clickhouse_gate_schema()
    assert ddl.count("CREATE TABLE") == 3 and ddl.count("CREATE TRIGGER") == 3
    assert "DPONE_CH_GATE_IMMUTABLE" in ddl and "DPONE_CH_GATE_PHASE" in ddl
    assert "dpone:composition-control:v1" in ddl and "ENABLING" in ddl
    assert "CREATE USER" not in ddl and "GRANT" not in ddl


@pytest.mark.parametrize("host", ["127.0.0.1", "::1"])
def test_loopback_ip_is_rejected_before_gate_intent(host):
    observation = Supervisor(intent()).value
    with pytest.raises(CompositionAdmissionError, match="supervisor_ip"):
        replace(observation, supervisor_ip=host)


def local_observation(value):
    fields = dict(
        service_id=value.service_id,
        database_uuid=value.database_uuid,
        boot_id=value.boot_id,
        isolation_id=value.isolation_id,
        enrollment_sha256="sha256:" + "a" * 64,
        network_namespace_id="4026532900",
    )
    document = canonical_json_bytes(
        {
            "schema": "dpone.composition-clickhouse-local-supervisor.v1",
            **fields,
            "facts": {"offline_fixture": "never live authority"},
        }
    )
    return ClickHouseLocalSupervisorObservation(
        **fields, evidence_sha256=document_sha256(document), evidence_document=document
    )


def test_explicit_local_gate_issues_and_closes_without_ip_fallback(active):
    db, value, gate, admin, supervisor, _ = active
    supervisor.value = local_observation(supervisor.value)
    issued = gate.issue_once(value.attempt)
    assert admin.users[issued.username][1] == "LOCAL"
    assert gate.close(value.attempt).kind == "CLOSED_GATES"
    assert gate.prove_quiescence(value.attempt).kind == "QUIESCENCE"
    assert admin.users[issued.username] == [issued.user_id, None, True]


def test_host_policy_switch_cannot_issue_another_principal(active):
    db, value, gate, admin, supervisor, _ = active
    gate.issue_once(value.attempt)
    supervisor.value = local_observation(supervisor.value)
    with pytest.raises(CompositionAdmissionError):
        gate.issue_once(value.attempt)
    with pytest.raises(CompositionAdmissionError, match="gate_subject_changed"):
        gate.close(value.attempt)
    assert len(admin.users) == 1


@pytest.mark.parametrize("namespace", ["", "0", "01", "net:[1]", "-1", "18446744073709551616"])
def test_local_observation_rejects_invalid_namespace(namespace):
    value = local_observation(Supervisor(intent()).value)
    with pytest.raises(CompositionAdmissionError):
        replace(value, network_namespace_id=namespace)
