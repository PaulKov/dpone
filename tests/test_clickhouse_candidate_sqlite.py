"""Real v2 journals: positive acknowledgements, retained names and no replay."""

from __future__ import annotations

import copy
import importlib
import json
import pickle
import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.adapters.clickhouse_observation_profile import ProtectedObservationProfile
from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError, AuthoritySubject, TransportState
from dpone.contracts.clickhouse_candidate import ProtectedPublicationRequest, VerifiedEnrollment
from dpone.contracts.clickhouse_observation import CandidateColumn, CandidateDesign, MultisetState, ObservationLimits


def request(operation="deployment:one", target="target", candidate="candidate"):
    return ProtectedPublicationRequest(
        operation,
        AuthoritySubject("deployment", "server", "db", target),
        candidate,
        CandidateDesign((CandidateColumn("id", "Int32"),), ("id",), ("id",), ()),
        ObservationLimits(4, 10, 1024, 256, 8, 1000, 4096, 5.0),
    )


def enrollment(value):
    return VerifiedEnrollment._issue(
        value, "inventory-1", "a" * 64, ProtectedObservationProfile(value.design, value.limits).profile_digest
    )


def store(tmp_path):
    module = importlib.import_module("dpone.adapters.clickhouse_candidate_sqlite")
    path = tmp_path / "authority.db"
    module.SQLiteCandidateAuthority.provision(path, "deployment")
    return module.SQLiteCandidateAuthority(path, "deployment")


def journal(authority):
    return importlib.import_module("dpone.adapters.clickhouse_candidate_requests").CandidateRequestJournal(
        authority._storage
    )


def mutation(invocation, kind="create", sequence=0):
    api = importlib.import_module("dpone.contracts.clickhouse_candidate")
    value = request()
    profile = ProtectedObservationProfile(value.design, value.limits)
    batch = profile.validate_batch(()) if kind == "create" else profile.validate_batch(((7,), (7,)))
    return api.CandidateMutationRequest(
        invocation.binding, kind, sequence, profile.design_digest, "b" * 64, batch.payload_digest, batch.evidence
    )


def completion(value):
    api = importlib.import_module("dpone.contracts.clickhouse_candidate")
    return api.CandidateCompletion(
        value.binding.operation_id,
        value.query_id,
        value.kind,
        value.statement_digest,
        value.payload_digest,
        "server",
        (24, 8, 14),
        54468,
        "0.2.10",
    )


def complete_create(authority, invocation):
    writes = journal(authority)
    value = mutation(invocation)
    grant = writes.register(invocation, value)
    writes.begin_send(grant)
    writes.record_terminal(grant, completion(value))
    return writes


def test_explicit_v2_does_not_change_v1_default_or_history(tmp_path):
    authority = store(tmp_path)
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    private = tmp_path / "old"
    private.mkdir(mode=0o700)
    old_path = private / "old.db"
    SQLitePublicationAuthority.provision(old_path, "deployment")
    old = SQLitePublicationAuthority(old_path, "deployment")
    original = old.acquire("deployment:legacy", request().subject, "legacy_candidate")
    before = old.diagnostics(original.operation_id)
    with pytest.raises(AuthorityError):
        type(authority)(old_path, "deployment")
    assert old.diagnostics(original.operation_id) == before
    with sqlite3.connect(old_path) as db:
        assert (
            db.execute("SELECT schema_version FROM authority_metadata").fetchone()[0] == "dpone.clickhouse.authority.v1"
        )
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='candidate_operations'").fetchall()


def test_candidate_name_cannot_be_another_target(tmp_path):
    authority = store(tmp_path)
    first, second = request(), request("deployment:two", "candidate", "new_candidate")
    with authority.enroll(first, enrollment(first)):
        with pytest.raises(AuthorityConflict):
            authority.enroll(second, enrollment(second))
    assert authority.inspect(first.operation_id).binding.candidate == first.candidate
    with authority._storage.connection() as db:
        assert db.execute("SELECT count(*) FROM name_reservations").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM subjects").fetchone()[0] == 1


@pytest.mark.parametrize(("target", "candidate"), [("other", "target"), ("other", "candidate"), ("target", "other")])
def test_every_cross_role_collision_preserves_original(tmp_path, target, candidate):
    authority = store(tmp_path)
    first = request()
    with authority.enroll(first, enrollment(first)):
        second = request("deployment:two", target, candidate)
        with pytest.raises(AuthorityConflict):
            authority.enroll(second, enrollment(second))
    assert authority.inspect(first.operation_id).lifecycle == "registered"
    with pytest.raises(AuthorityError):
        authority.inspect(second.operation_id)


def test_same_operation_never_recreates_invocation(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        for changed in (value, replace(value, candidate="new_candidate")):
            with pytest.raises(AuthorityConflict):
                authority.enroll(changed, enrollment(changed))
        for copier in (copy.copy, copy.deepcopy, pickle.dumps):
            with pytest.raises(TypeError):
                copier(invocation)
    with pytest.raises(AuthorityError):
        invocation.assert_current()
    assert authority.inspect(value.operation_id).binding == invocation.binding
    assert not hasattr(authority, "acquire")
    assert not hasattr(authority, "prepare")


def test_foreign_readiness_is_rejected_before_reservation(tmp_path):
    authority = store(tmp_path)
    value = request()
    with pytest.raises(AuthorityError):
        authority.enroll(value, enrollment(replace(value, candidate="other")))
    with pytest.raises(AuthorityError):
        authority.inspect(value.operation_id)


@pytest.mark.parametrize("phase", ["enroll", "register", "begin_send", "record_terminal", "close_admission"])
def test_lost_commit_ack_does_not_return_new_permission(tmp_path, monkeypatch, phase):
    authority = store(tmp_path)
    value = request()
    invocation = None if phase == "enroll" else authority.enroll(value, enrollment(value))
    writes = journal(authority)
    mutation_value = None if invocation is None else mutation(invocation)
    grant = writes.register(invocation, mutation_value) if phase in {"begin_send", "record_terminal"} else None
    if phase == "record_terminal":
        writes.begin_send(grant)
    original = authority._storage.transaction

    @contextmanager
    def lost_ack():
        with original() as db:
            yield db
        raise AuthorityError("injected commit acknowledgement loss")

    with monkeypatch.context() as scoped:
        scoped.setattr(authority._storage, "transaction", lost_ack)
        with pytest.raises(AuthorityError):
            if phase == "enroll":
                authority.enroll(value, enrollment(value))
            elif phase == "register":
                writes.register(invocation, mutation_value)
            elif phase == "begin_send":
                writes.begin_send(grant)
            elif phase == "record_terminal":
                writes.record_terminal(grant, completion(mutation_value))
            else:
                authority.close_admission(value.operation_id)
    reopened = type(authority)(tmp_path / "authority.db", "deployment")
    status = reopened.inspect(value.operation_id)
    assert status.binding.operation_id == value.operation_id
    with pytest.raises(AuthorityConflict):
        reopened.enroll(value, enrollment(value))
    if phase == "begin_send":
        assert journal(reopened).requests(value.operation_id)[0].state == TransportState.MAY_HAVE_SENT
        with pytest.raises(AuthorityConflict):
            writes.begin_send(grant)
    elif phase == "record_terminal":
        assert journal(reopened).requests(value.operation_id)[0].state == TransportState.CLOSED_TERMINAL
    elif phase == "close_admission":
        assert status.admission_closed


def test_request_completion_adds_multiset_once_and_closure_is_irreversible(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        insert = mutation(invocation, "insert", 1)
        grant = writes.register(invocation, insert)
        authority.close_admission(value.operation_id)
        writes.begin_send(grant)  # Accepted-before-close work remains entitled to finish.
        receipt = completion(insert)
        writes.record_terminal(grant, receipt)
        writes.record_terminal(grant, receipt)
        assert authority.inspect(value.operation_id).expected == insert.evidence
        with pytest.raises(AuthorityConflict):
            writes.register(invocation, replace(insert, sequence=2))
        authority.source_exhausted(invocation)
        assert authority.inspect(value.operation_id).source_exhausted
        assert authority.inspect(value.operation_id).seal is None


def test_insert_requires_successful_create_and_single_inflight_request(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = journal(authority)
        with pytest.raises(AuthorityConflict):
            writes.register(invocation, mutation(invocation, "insert", 1))
        create = writes.register(invocation, mutation(invocation))
        with pytest.raises(AuthorityConflict):
            writes.register(invocation, mutation(invocation))
        writes.begin_send(create)
        with pytest.raises(AuthorityConflict):
            writes.register(invocation, mutation(invocation, "insert", 1))


def test_modified_receipt_or_request_cannot_close_or_add_rows(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        insert = mutation(invocation, "insert", 1)
        with pytest.raises(AuthorityError):
            writes.register(invocation, replace(insert, design_digest="c" * 64))
        grant = writes.register(invocation, insert)
        writes.begin_send(grant)
        with pytest.raises(AuthorityError):
            writes.record_terminal(grant, replace(completion(insert), payload_digest="d" * 64))
        assert authority.inspect(value.operation_id).expected == MultisetState(0, 0, 0, (0,))
        with pytest.raises(AuthorityConflict):
            writes.close_unsent(value.operation_id)


def test_unsent_cancellation_retains_load_and_names(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        grant = writes.register(invocation, mutation(invocation, "insert", 1))
        writes.close_unsent(value.operation_id)
        with pytest.raises(AuthorityError):
            writes.begin_send(grant)
        status = authority.inspect(value.operation_id)
        assert status.lifecycle == "retained"
        assert status.admission_closed
        assert status.expected.count == 0


@pytest.mark.parametrize("damage", ["version", "symlink", "permissions", "replacement"])
def test_v2_storage_guards_preserve_originals(tmp_path, damage):
    authority = store(tmp_path)
    path = tmp_path / "authority.db"
    if damage == "version":
        with sqlite3.connect(path) as db:
            db.execute("UPDATE authority_metadata SET schema_version='future'")
    elif damage == "permissions":
        path.chmod(0o644)
    else:
        original = tmp_path / "original.db"
        path.rename(original)
        if damage == "symlink":
            path.symlink_to(original)
        else:
            type(authority).provision(path, "deployment")
    with pytest.raises(AuthorityError):
        authority.execution_identity()


def test_history_reservations_and_request_identity_are_immutable(tmp_path):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = complete_create(authority, invocation)
        assert writes.requests(value.operation_id)[0].request.query_id.startswith("dpone-candidate-")
        for sql in [
            "DELETE FROM name_reservations",
            "UPDATE candidate_requests SET request='{}'",
            "DELETE FROM candidate_history",
        ]:
            with pytest.raises(sqlite3.IntegrityError), sqlite3.connect(tmp_path / "authority.db") as db:
                db.execute(sql)
        with authority._storage.connection() as db:
            encoded = db.execute("SELECT expected FROM candidate_operations").fetchone()[0]
        assert isinstance(encoded, str)
        assert json.loads(encoded)["total"] == 0


@pytest.mark.parametrize("version", ["dpone.clickhouse.authority.v2", "future", None])
def test_unknown_or_untyped_version_cannot_create_a_file(tmp_path, version):
    from dpone.adapters.clickhouse_authority_storage import AuthorityStorage, AuthorityStorageError

    path = tmp_path / "never.db"
    with pytest.raises(AuthorityStorageError):
        AuthorityStorage.provision(path, "deployment", version=version)
    assert not path.exists()


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", "-journal"])
def test_provision_never_replaces_original_or_adopts_sidecars(tmp_path, suffix):
    from dpone.adapters.clickhouse_candidate_sqlite import SQLiteCandidateAuthority

    original = tmp_path / ("authority.db" + suffix)
    original.write_bytes(b"preserve original bytes")
    with pytest.raises(AuthorityError):
        SQLiteCandidateAuthority.provision(tmp_path / "authority.db", "deployment")
    assert original.read_bytes() == b"preserve original bytes"
    if suffix:
        assert not (tmp_path / "authority.db").exists()


def test_rollback_leaves_no_owner_or_partial_reservation(tmp_path, monkeypatch):
    authority = store(tmp_path)
    transaction = authority._storage.transaction

    @contextmanager
    def fail_before_commit():
        with transaction() as db:
            yield db
            raise AuthorityError("injected transaction rollback")

    value = request()
    with monkeypatch.context() as scoped:
        scoped.setattr(authority._storage, "transaction", fail_before_commit)
        with pytest.raises(AuthorityError):
            authority.enroll(value, enrollment(value))
    with authority._storage.connection() as db:
        for table in (
            "subjects",
            "operations",
            "history",
            "name_reservations",
            "candidate_operations",
            "candidate_history",
        ):
            assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    with authority.enroll(value, enrollment(value)):
        assert authority.inspect(value.operation_id).lifecycle == "registered"


@pytest.mark.parametrize("damage", ["expired", "invocation_closed", "secret", "revision", "request", "store"])
def test_expired_or_changed_grants_never_enter_send(tmp_path, damage):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = journal(authority)
        grant = writes.register(invocation, mutation(invocation))
        if damage == "expired":
            grant.close()
        elif damage == "invocation_closed":
            invocation.close()
        elif damage == "secret":
            grant._secret = "foreign"
        elif damage == "revision":
            grant._revision = 1
        elif damage == "request":
            grant._request = replace(grant.request, statement_digest="c" * 64)
        else:
            directory = tmp_path / "foreign"
            directory.mkdir(mode=0o700)
            writes = journal(store(directory))
        with pytest.raises(AuthorityError):
            writes.begin_send(grant)
        assert journal(authority).requests(value.operation_id)[0].state == TransportState.NOT_STARTED


@pytest.mark.parametrize("server_version", [[24, 8, 14], (24.0, 8, 14), (24, 8, 15)])
def test_completion_requires_exact_immutable_server_profile(tmp_path, server_version):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        writes = journal(authority)
        grant = writes.register(invocation, mutation(invocation))
        writes.begin_send(grant)
        with pytest.raises(AuthorityError):
            writes.record_terminal(grant, replace(completion(grant.request), server_version=server_version))
        assert writes.requests(value.operation_id)[0].state == TransportState.MAY_HAVE_SENT


@pytest.mark.parametrize(
    ("state", "revision", "digest"),
    [
        (TransportState.NOT_STARTED, 1, None),
        (TransportState.MAY_HAVE_SENT, 0, None),
        (TransportState.CLOSED_TERMINAL, 2, None),
        (TransportState.CLOSED_TERMINAL, 2, "not-a-digest"),
        (TransportState.CLOSED_WITHOUT_SEND, 1, "a" * 64),
    ],
)
def test_request_status_cannot_claim_impossible_completion(tmp_path, state, revision, digest):
    from dpone.contracts.clickhouse_candidate import CandidateRequestStatus

    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        with pytest.raises(AuthorityError):
            CandidateRequestStatus(mutation(invocation), state, revision, digest)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE candidate_requests SET revision=1 WHERE state='closed_terminal'",
        "UPDATE candidate_requests SET state='closed_terminal',revision=2,completion_digest=NULL",
        "UPDATE candidate_operations SET revision=0",
    ],
)
def test_storage_rejects_invalid_completion_or_revision_regression(tmp_path, sql):
    authority = store(tmp_path)
    value = request()
    with authority.enroll(value, enrollment(value)) as invocation:
        complete_create(authority, invocation)
        with pytest.raises(sqlite3.IntegrityError), sqlite3.connect(tmp_path / "authority.db") as db:
            db.execute(sql)
