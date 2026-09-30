"""Exact-envelope and ACK tests; server concurrency requires the live suite."""

import hashlib
from dataclasses import replace
from importlib import import_module
from uuid import UUID

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationStatus as Status,
)
from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityPhase as Phase,
)
from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityRecord,
    GenerationIdentity,
    VersionedAuthorityRecord,
    canonical_json,
)
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding, publication_binding_digest
from tests.test_mssql_publication_admission import Catalog

BINDING = PublicationAuthorityBinding("mssql", "metadata", "Example_System", "dbo", "example", "test")
WRITE_ID = UUID("11111111-2222-4333-8444-555555555555")
BINDING_DIGEST = publication_binding_digest(BINDING, endpoint_identity="a" * 64)


def api():
    return import_module("dpone.runtime.state.mssql_publication_authority")


def record():
    return AuthorityRecord(
        target_key="b" * 64,
        operation_id="operation",
        fence_token="fence",
        phase=Phase.PREPARED,
        dispatch_epoch=0,
        inventory_digest="c" * 64,
        plan_digest="d" * 64,
        database="analytics",
        target="example",
        candidate="example_candidate",
        desired=GenerationIdentity("new", "ReplicatedMergeTree('/new','r')", "e" * 64, "default", "/new"),
        predecessor=GenerationIdentity("old", "ReplicatedMergeTree('/old','r')", "f" * 64, "default", "/old"),
        staged_rows=2,
    )


def receipt(value, base_revision, *, won=1, **changes):
    payload = value.payload.encode()
    provenance = canonical_json(
        {"contract": "dpone.publication-origin.v1", "origin": "native", "binding_digest": BINDING_DIGEST}
    ).encode()
    fields = dict(
        won=won,
        revision=base_revision,
        payload=payload,
        payload_hash=hashlib.sha256(payload).digest(),
        write_id=str(UUID(hex=value.authority_write_id or WRITE_ID.hex)),
        binding=bytes.fromhex(BINDING_DIGEST),
        event_matches=1,
        chain_matches=1,
        origin="native",
        provenance=provenance,
        provenance_hash=hashlib.sha256(provenance).digest(),
        operation=value.operation_id,
        phase=value.phase.value,
    )
    fields.update(changes)
    return tuple(fields.values())


class Transport:
    """Inject a server response, including commit/error ordering, without SQL execution."""

    def __init__(self, rows, failure=None):
        self.rows = rows
        self.failure = failure
        self.calls = []
        self.autocommit = True
        self.description = (("receipt",),)

    def cursor(self):
        return self

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchall(self):
        return self.rows

    def nextset(self):
        if self.failure == "after_output":
            raise RuntimeError("rolled back")
        return False

    def commit(self):
        if self.failure == "commit":
            raise TimeoutError("lost ACK")

    def close(self):
        pass

    def rollback(self):
        pass


def authority(transport):
    # Real catalog validation uses synthetic metadata; the wire transport is
    # injected independently. Neither injection certifies a deployed server.
    return api().MssqlPublicationAuthority(
        catalog_connector=Catalog(),
        session_factory=lambda: transport,
        binding=BINDING,
        endpoint_identity="a" * 64,
        write_id_factory=lambda: WRITE_ID,
    )


def test_create_prepared_returns_exact_committed_envelope_without_permit():
    desired = replace(record(), authority_write_id=WRITE_ID.hex)
    transport = Transport([receipt(desired, 1)])
    result = authority(transport).create_if_absent(record())
    assert result.status is Status.VERIFIED
    assert result.observed == VersionedAuthorityRecord(desired, 1)
    assert result.permit is None
    assert len(transport.calls) == 1


def test_only_acknowledged_winning_dispatch_transition_receives_permit():
    before = VersionedAuthorityRecord(record(), 1)
    desired = before.record.dispatching(token="intent-1", query_digest="1" * 64)
    written = replace(desired, authority_write_id=WRITE_ID.hex)
    result = authority(Transport([receipt(written, 2)])).compare_and_swap(before, desired)
    assert result.status is Status.VERIFIED
    assert result.permit is not None
    assert result.permit.dispatch_epoch == 1
    assert result.permit.operation_id == "operation"


@pytest.mark.parametrize("failure", ["after_output", "commit"])
def test_unknown_write_never_receives_permit_or_readback_retry(failure):
    desired = record().dispatching(token="intent-1", query_digest="1" * 64)
    transport = Transport([receipt(replace(desired, authority_write_id=WRITE_ID.hex), 2)], failure)
    result = authority(transport).compare_and_swap(VersionedAuthorityRecord(record(), 1), desired)
    assert result.status is Status.OUTCOME_UNKNOWN and result.permit is None
    assert len(transport.calls) == 1


def test_losing_identical_cas_does_not_impersonate_winner():
    desired = record().dispatching(token="intent-1", query_digest="1" * 64)
    transport = Transport([receipt(replace(desired, authority_write_id=WRITE_ID.hex), 2, won=0)])
    result = authority(transport).compare_and_swap(VersionedAuthorityRecord(record(), 1), desired)
    assert result.status is Status.CONFLICT and result.permit is None


@pytest.mark.parametrize(
    "change",
    [
        {"revision": 7},
        {"payload_hash": b"x" * 32},
        {"binding": b"x" * 32},
        {"event_matches": 0},
        {"chain_matches": 0},
        {"origin": "invented"},
        {"provenance_hash": b"x" * 32},
        {"operation": "another"},
        {"phase": "COMPLETED"},
        {"write_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"},
    ],
)
def test_corrupt_receipt_never_grants_a_permit(change):
    desired = record().dispatching(token="intent-1", query_digest="1" * 64)
    transport = Transport([receipt(replace(desired, authority_write_id=WRITE_ID.hex), 2, **change)])
    result = authority(transport).compare_and_swap(VersionedAuthorityRecord(record(), 1), desired)
    assert result.status is Status.OUTCOME_UNKNOWN and result.permit is None


@pytest.mark.parametrize("value", [Phase.DISPATCHING, Phase.COMMITTED, Phase.CLEANUP_DISPATCHING, Phase.COMPLETED])
def test_native_create_cannot_masquerade_as_imported_progress(value):
    transport = Transport([])
    with pytest.raises(ValueError):
        authority(transport).create_if_absent(replace(record(), phase=value))
    assert not transport.calls


@pytest.mark.parametrize(
    "change",
    [
        {"dispatch_epoch": 0},
        {"operation_id": "other"},
        {"target_key": "c" * 64},
        {"staged_rows": 0},
        {"ddl_correlation_token": None},
        {"ddl_query_digest": None},
    ],
)
def test_invalid_dispatch_transition_is_rejected_before_io(change):
    transport = Transport([])
    desired = replace(record().dispatching(token="intent-1", query_digest="1" * 64), **change)
    with pytest.raises(ValueError):
        authority(transport).compare_and_swap(VersionedAuthorityRecord(record(), 1), desired)
    assert not transport.calls


def test_phase_regression_is_rejected_before_io():
    transport = Transport([])
    with pytest.raises(ValueError):
        authority(transport).compare_and_swap(
            VersionedAuthorityRecord(replace(record(), phase=Phase.COMMITTED), 2), record()
        )
    assert not transport.calls


def test_same_dispatching_phase_cannot_issue_another_permit():
    transport = Transport([])
    dispatching = record().dispatching(token="intent-1", query_digest="1" * 64)
    with pytest.raises(ValueError):
        authority(transport).compare_and_swap(VersionedAuthorityRecord(dispatching, 2), dispatching)
    assert not transport.calls


def test_read_existing_dispatch_intent_only_returns_state():
    value = replace(record().dispatching(token="intent-1", query_digest="1" * 64), authority_write_id=WRITE_ID.hex)
    transport = Transport([receipt(value, 2, won=0)])
    assert authority(transport).read_versioned(value.target_key) == VersionedAuthorityRecord(value, 2)
    assert len(transport.calls) == 1


def test_absent_read_is_not_a_native_create():
    transport = Transport([])
    assert authority(transport).read_versioned("b" * 64) is None
    assert "INSERT" not in transport.calls[0][0]


def test_missing_catalog_prevents_any_mutation_session():
    catalog = Catalog()
    catalog.column_rows = []
    sessions = []
    with pytest.raises(ValueError, match="publication catalog"):
        api().MssqlPublicationAuthority(
            catalog_connector=catalog,
            session_factory=lambda: sessions.append(1),
            binding=BINDING,
            endpoint_identity="a" * 64,
        )
    assert not sessions


@pytest.mark.parametrize("change", [{"ddl_entry": None}, {"ddl_entry": ""}])
def test_committed_transition_requires_original_ddl_entry(change):
    transport = Transport([])
    before = record().dispatching(token="intent-1", query_digest="1" * 64)
    desired = replace(before, phase=Phase.COMMITTED, **change)
    with pytest.raises(ValueError):
        authority(transport).compare_and_swap(VersionedAuthorityRecord(before, 2), desired)
    assert not transport.calls


def test_cleanup_dispatch_has_distinct_acknowledged_permit():
    before = replace(
        record().dispatching(token="intent-1", query_digest="1" * 64), phase=Phase.COMMITTED, ddl_entry="query-1"
    )
    desired = replace(
        before,
        phase=Phase.CLEANUP_DISPATCHING,
        dispatch_epoch=2,
        cleanup_correlation_token="cleanup-2",
        cleanup_query_digest="2" * 64,
    )
    transport = Transport([receipt(replace(desired, authority_write_id=WRITE_ID.hex), 4)])
    result = authority(transport).compare_and_swap(VersionedAuthorityRecord(before, 3), desired)
    assert result.status is Status.VERIFIED and result.permit.dispatch_epoch == 2


@pytest.mark.parametrize("bad_payload", [b"{}", b"[]", b"\xff", b"null", b"x" * (1024 * 1024 + 1)])
def test_malformed_read_cannot_be_treated_as_absent(bad_payload):
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    transport = Transport([receipt(record(), 1, payload=bad_payload)])
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(transport).read_versioned(record().target_key)


def test_transaction_batch_guards_predecessor_and_native_provenance():
    written = replace(record(), authority_write_id=WRITE_ID.hex)
    transport = Transport([receipt(written, 1)])
    authority(transport).create_if_absent(record())
    sql, params = transport.calls[0]
    assert "UPDLOCK,HOLDLOCK" in sql
    assert "@expected_revision=@actual_revision" in sql
    assert "DATALENGTH(@expected_payload)=DATALENGTH(@actual_payload)" in sql
    assert "e.origin='native'" in sql
    assert "e.provenance=@provenance" in sql
    assert "COMMIT" not in sql  # DBAPI owns ACK, not an early server result row
    assert sql.count("?") == len(params)
