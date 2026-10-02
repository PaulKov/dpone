"""SQL retirement receipts, not live proof of SQL locking or writer exclusion."""

import hashlib
from dataclasses import replace
from importlib import import_module

import pytest

from tests.test_mssql_publication_admission import Catalog
from tests.test_mssql_publication_authority import BINDING, WRITE_ID, Transport, authority, receipt
from tests.test_publication_retirement import observation, plan


def api():
    return import_module("dpone.runtime.state.mssql_publication_retirement")


def retired_record(value):
    return api().retirement_record(value, write_id=WRITE_ID)


def retired_receipt(value, **changes):
    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase

    # Derive the wire fixture independently; a broken retirement_record builder
    # must not silently change both the adapter's expectation and this receipt.
    expected = replace(
        value.original.record,
        phase=AuthorityPhase.RETIRED_UNPUBLISHED,
        schema_version="dpone.clickhouse.cluster-full-refresh.v1",
        quality_evidence=None,
        quality_reader=None,
        authority_write_id=WRITE_ID.hex,
    )
    provenance = value.payload.encode()
    return receipt(
        expected,
        1,
        origin="legacy_retired",
        provenance=provenance,
        provenance_hash=hashlib.sha256(provenance).digest(),
        **changes,
    )


def store(transport, *, clock=lambda: 220):
    return api().MssqlPublicationRetirement(
        catalog_connector=Catalog(),
        session_factory=lambda: transport,
        binding=BINDING,
        endpoint_identity="a" * 64,
        clock=clock,
        write_id_factory=lambda: WRITE_ID,
    )


def test_retirement_is_distinct_from_native_create_and_dispatch():
    value = plan()
    transport = Transport([retired_receipt(value)])
    assert store(transport).retire_if_absent(value) == "acknowledged"
    sql, params = transport.calls[0]
    assert "UPDLOCK,HOLDLOCK" in sql
    assert "'legacy_retired'" in sql
    assert "@actual_revision IS NULL" in sql
    assert "publication retirement history exists" in sql
    assert "UPDATE" not in sql
    assert "COMMIT" not in sql
    assert sql.count("?") == len(params)
    assert value.payload.encode() in params
    assert value.original_payload.hex() in value.payload
    record = retired_record(value)
    assert record.phase.value == "RETIRED_UNPUBLISHED"
    assert record.quality_evidence is None
    with pytest.raises(ValueError):
        authority(Transport([])).create_if_absent(record)


def test_exact_retirement_readback_never_writes_or_returns_a_permit():
    value = plan()
    transport = Transport([retired_receipt(value, won=0)])
    assert store(transport).inspect(value) == "exact"
    assert "INSERT" not in transport.calls[0][0]
    assert "UPDATE" not in transport.calls[0][0]


def test_absent_retirement_readback_is_not_an_insert():
    transport = Transport([])
    assert store(transport).inspect(plan()) == "absent"
    assert "INSERT" not in transport.calls[0][0]


@pytest.mark.parametrize("failure", ["commit", "after_output"])
def test_ambiguous_retirement_commit_does_not_retry_or_claim_success(failure):
    value = plan()
    transport = Transport([retired_receipt(value)], failure=failure)
    assert store(transport).retire_if_absent(value) == "unknown"
    assert len(transport.calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"binding": b"x" * 32},
        {"event_matches": 0},
        {"chain_matches": 0},
        {"payload_hash": b"x" * 32},
        {"provenance_hash": b"x" * 32},
        {"origin": "native"},
        {"revision": 2},
        {"phase": "COMMITTED"},
        {"provenance": b"untrusted"},
        {"operation": "changed"},
    ],
)
def test_corrupt_or_conflicting_retirement_receipt_is_not_success(changes):
    value = plan()
    row = list(retired_receipt(value))
    fields = (
        "won",
        "revision",
        "payload",
        "payload_hash",
        "write_id",
        "binding",
        "event_matches",
        "chain_matches",
        "origin",
        "provenance",
        "provenance_hash",
        "operation",
        "phase",
    )
    for key, changed in changes.items():
        row[fields.index(key)] = changed
    assert store(Transport([tuple(row)])).inspect(value) != "exact"
    assert store(Transport([tuple(row)])).retire_if_absent(value) != "acknowledged"


def test_existing_different_slot_returns_conflict_without_update():
    from tests.test_mssql_publication_authority import record

    transport = Transport([receipt(replace(record(), authority_write_id=WRITE_ID.hex), 1, won=0)])
    assert store(transport).retire_if_absent(plan()) == "conflict"
    assert "UPDATE" not in transport.calls[0][0]


@pytest.mark.parametrize("changed", ["binding", "expired", "history"])
def test_invalid_plan_rejected_before_sql(changed):
    value = observation()
    if changed == "binding":
        other = "e" * 64
        value = replace(value, binding_digest=other, freeze=replace(value.freeze, binding_digest=other))
    elif changed == "history":
        value = replace(value, replicas=(replace(value.replicas[0], history_gaps=((101, 102),)), *value.replicas[1:]))
    transport = Transport([])
    policy = import_module("dpone.contracts.publication_retirement")
    with pytest.raises(ValueError):
        store(transport, clock=lambda: 301 if changed == "expired" else 220).retire_if_absent(
            policy.PublicationRetirementPlan(value)
        )
    assert not transport.calls


def test_missing_catalog_prevents_retirement_session():
    catalog = Catalog()
    catalog.column_rows = []
    calls = []
    with pytest.raises(ValueError):
        api().MssqlPublicationRetirement(
            catalog_connector=catalog,
            session_factory=lambda: calls.append(1),
            binding=BINDING,
            endpoint_identity="a" * 64,
            clock=lambda: 220,
        )
    assert not calls


def test_retired_authority_cannot_become_publication_cleanup_receipt():
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError, VersionedAuthorityRecord
    from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt

    value = retired_record(plan())
    with pytest.raises(ClusterPublicationError, match="RECEIPT_INVALID"):
        ClusterFullRefreshReceipt.from_authority(VersionedAuthorityRecord(value, 1), "example")


def test_retired_authority_cannot_be_deserialized_as_publication_receipt():
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
    from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt

    value = plan()
    mapping = ClusterFullRefreshReceipt.from_authority(value.original, "example").to_dict()
    mapping["authority"]["phase"] = "RETIRED_UNPUBLISHED"
    with pytest.raises(ClusterPublicationError, match="RECEIPT_INVALID"):
        ClusterFullRefreshReceipt.from_mapping(mapping)


def test_retirement_cannot_issue_even_a_process_local_dispatch_permit():
    from dpone.contracts.clickhouse_cluster_publication import DispatchPermit

    with pytest.raises(ValueError, match="retired"):
        DispatchPermit.for_record(retired_record(plan()))


def test_directly_constructed_retired_receipt_cannot_authorize_cleanup():
    from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase, ClusterPublicationError
    from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt

    current = plan().original
    normal = ClusterFullRefreshReceipt.from_authority(current, "example")
    supplied = replace(normal, authority=replace(normal.authority, phase=AuthorityPhase.RETIRED_UNPUBLISHED))
    with pytest.raises(ClusterPublicationError, match="RECEIPT_INVALID"):
        supplied.validate_for_authority(current)


def test_expired_retirement_plan_readback_uses_original_provenance_without_writing():
    value = plan()
    transport = Transport([retired_receipt(value, won=0)])
    assert store(transport, clock=lambda: 301).inspect(value) == "exact"
    assert "INSERT" not in transport.calls[0][0]


def test_native_authority_can_read_exact_retirement_but_cannot_replay_it():
    value = plan()
    transport = Transport([retired_receipt(value, won=0)])
    observed = authority(transport).read_versioned(value.original.record.target_key)
    assert observed.record == retired_record(value)
    assert observed.version == 1
    assert "INSERT" not in transport.calls[0][0]
    assert "UPDATE" not in transport.calls[0][0]
    with pytest.raises(ValueError):
        authority(transport).compare_and_swap(observed, observed.record)
    assert len(transport.calls) == 1


@pytest.mark.parametrize("variant", ["native_origin", "different_original", "revision"])
def test_retired_readback_rejects_forged_origin_or_provenance(variant):
    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    value = plan()
    row = list(retired_receipt(value, won=0))
    if variant == "native_origin":
        row = list(receipt(retired_record(value), 1, won=0))
    elif variant == "different_original":
        original = replace(value.original.record, operation_id="another-operation")
        other = replace(
            value.observation,
            replicas=tuple(
                replace(
                    item, authority=replace(item.authority, record=original), original_payload=original.payload.encode()
                )
                for item in value.observation.replicas
            ),
        )
        row[9] = (
            import_module("dpone.contracts.publication_retirement").PublicationRetirementPlan(other).payload.encode()
        )
        row[10] = hashlib.sha256(row[9]).digest()
    else:
        row[1] = 2
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(Transport([tuple(row)])).read_versioned(value.original.record.target_key)
