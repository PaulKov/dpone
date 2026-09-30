"""Strict durable records: reject corruption instead of silently normalizing it."""

import json
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_publication_codec import decode_record, encode_record
from dpone.contracts.clickhouse_authority import (
    AuthorityError,
    AuthoritySubject,
    DispatchGrant,
    JournalEntry,
    OperationBinding,
)
from dpone.contracts.clickhouse_publication import (
    PublicationObservation,
    PublicationRecord,
    PublicationState,
    PublicationTable,
    choose_publication,
)


def example_record(absent=False):
    old = PublicationTable("old", "a" * 64, "b" * 64, 1, ("all",))
    new = PublicationTable("new", "a" * 64, "c" * 64, 2, ("all",))
    observed = PublicationObservation(
        ("server", "db", "target", "candidate"), "Atomic", None if absent else old, new, True, True
    )
    return PublicationRecord(choose_publication("deployment:one", observed), PublicationState.PREPARED)


@pytest.mark.parametrize("absent", [False, True])
@pytest.mark.parametrize(
    "state,claimed",
    [
        (PublicationState.PREPARED, False),
        (PublicationState.CLAIMED, True),
        (PublicationState.UNKNOWN, False),
        (PublicationState.UNKNOWN, True),
        (PublicationState.COMMITTED, True),
        (PublicationState.NOT_PUBLISHED, False),
    ],
)
def test_codec_round_trip_preserves_full_intent_and_claim_history(absent, state, claimed):
    record = replace(example_record(absent), state=state, claim_granted=claimed)
    encoded = encode_record(record)
    assert decode_record(encoded) == record
    assert encode_record(decode_record(encoded)) == encoded
    assert "secret" not in encoded


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), "unknown"),
        (("claim_granted",), 1),
        (("claim_granted",), True),
        (("extra",), None),
        (("intent", "method"), "exchange"),
        (("intent", "reason"), "anything"),
        (("intent", "partition_id"), "elsewhere"),
        (("intent", "before", "target", "rows"), True),
        (("intent", "before", "target", "content_digest"), "B" * 64),
        (("intent", "before", "catalog_complete"), 1),
        (("intent", "before", "subject"), ["server", "db", "target", 3]),
        (("intent", "before", "target", "uuid"), 12),
    ],
)
def test_corrupt_record_rejected(path, value):
    payload = json.loads(encode_record(example_record()))
    node = payload
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(AuthorityError):
        decode_record(json.dumps(payload))


@pytest.mark.parametrize("payload", ['{"state":"prepared","state":"claimed"}', '{"x":NaN}', "[]", "null"])
def test_duplicate_keys_and_nonfinite_or_wrong_shape_rejected(payload):
    with pytest.raises(AuthorityError):
        decode_record(payload)


def test_subject_identity_excludes_endpoint_alias_and_table_uuid():
    first = AuthoritySubject("deployment", "server", "db", "target")
    assert first.key == AuthoritySubject("deployment", "server", "db", "target").key
    assert first.key != replace(first, deployment_id="other").key
    assert first.key != replace(first, target="other").key
    assert len(first.key) == 64


@pytest.mark.parametrize("epoch", [True, 0, -1, 1.0])
def test_binding_rejects_invalid_epoch(epoch):
    with pytest.raises(AuthorityError):
        OperationBinding("deployment:one", AuthoritySubject("deployment", "server", "db", "target"), "candidate", epoch)


def test_binding_rejects_reused_target_as_candidate():
    with pytest.raises(AuthorityError):
        OperationBinding("deployment:one", AuthoritySubject("deployment", "server", "db", "target"), "target", 1)


def test_grant_repr_redacts_secret_and_entry_requires_revision():
    assert "private-token" not in repr(DispatchGrant("deployment:one", 1, "private-token"))
    with pytest.raises(AuthorityError):
        JournalEntry(example_record(), True)
