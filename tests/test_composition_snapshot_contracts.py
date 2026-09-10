"""Exact-byte snapshot documents are structural records, never permission."""

import json
from dataclasses import fields, replace
from hashlib import sha256
from typing import cast

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_snapshot import (
    SnapshotCatalogObservation,
    SnapshotLimits,
    SnapshotPublicationIntent,
    SnapshotPublicationRecord,
    SnapshotPublisherClosure,
    classify_snapshot,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_snapshot_helpers import NEW, OLD, digest, intent, observation


def test_intent_roundtrip_hashes_exact_canonical_utf8_bytes():
    value = intent()
    document = value.to_bytes()
    assert value.intent_sha256 == "sha256:" + sha256(document).hexdigest()
    assert SnapshotPublicationIntent.from_bytes(document, value.intent_sha256) == value
    assert value.exchange_query_id == "dpone-snapshot-" + value.intent_sha256.removeprefix("sha256:")


def test_legacy_slash_normalization_does_not_collide_new_intents():
    value = intent()
    left = replace(value, attempt=replace(value.attempt, dag_run_id="run\\attempt"))
    right = replace(value, attempt=replace(value.attempt, dag_run_id="run/attempt"))
    assert left.to_bytes() != right.to_bytes()
    assert left.intent_sha256 != right.intent_sha256
    with pytest.raises(CompositionAdmissionError):
        SnapshotPublicationIntent.from_bytes(right.to_bytes(), left.intent_sha256)


def test_changed_generation_uuid_and_incomplete_guard_are_rejected():
    value = intent()
    with pytest.raises(CompositionAdmissionError):
        replace(value.generation, new_generation_uuid=value.generation.old_target_uuid)
    with pytest.raises(CompositionAdmissionError):
        replace(value, attempt=replace(value.attempt, guard_epochs=(("sha256:" + "0" * 64, 1),)))


@pytest.mark.parametrize(
    "section", [None, "attempt", "target", "generation", "limits", "ingest_principal", "publisher_principal"]
)
@pytest.mark.parametrize("kind", ["unknown", "missing", "wrong_type"])
def test_each_nested_shape_is_closed_even_with_recomputed_digest(section, kind):
    body = json.loads(intent().to_bytes())
    item = body if section is None else body[section]
    if kind == "unknown":
        item["caller_approved"] = True
    elif kind == "missing":
        item.pop(next(iter(item)))
    else:
        item[next(iter(item))] = []
    document = canonical_json_bytes(body)
    with pytest.raises(CompositionAdmissionError, match="snapshot_intent_document"):
        SnapshotPublicationIntent.from_bytes(document, "sha256:" + sha256(document).hexdigest())


@pytest.mark.parametrize(
    "document",
    [
        b'{"schema":"x","schema":"x"}',
        b'{"x":NaN}',
        b'{"x":1e9999}',
        b"[]",
        b"null",
        b"\xff",
        b"{",
        b"{}" * (4 * 1024 * 1024 + 1),
    ],
)
def test_ambiguous_malformed_or_unbounded_documents_reject(document):
    with pytest.raises(CompositionAdmissionError):
        SnapshotPublicationIntent.from_bytes(document, "sha256:" + sha256(document).hexdigest())


@pytest.mark.parametrize(
    "encode",
    [lambda doc: b" " + doc, lambda doc: doc + b"\n", lambda doc: json.dumps(json.loads(doc), indent=2).encode()],
)
def test_noncanonical_json_rejects_even_with_its_own_digest(encode):
    document = encode(intent().to_bytes())
    with pytest.raises(CompositionAdmissionError):
        SnapshotPublicationIntent.from_bytes(document, "sha256:" + sha256(document).hexdigest())


@pytest.mark.parametrize("field", [field.name for field in fields(SnapshotLimits)])
@pytest.mark.parametrize("value", [True, 0, -1, "1", 1.0, 2**63])
def test_limits_are_bounded_positive_integers(field, value):
    with pytest.raises(CompositionAdmissionError):
        replace(intent().limits, **{field: value})


@pytest.mark.parametrize(
    "field", ["rows", "source_bytes", "wire_bytes", "generation_bytes", "old_target_bytes", "retained_bytes"]
)
@pytest.mark.parametrize("value", [True, -1, "0", None, 2**63])
def test_generation_counters_are_exact_nonnegative_integers(field, value):
    with pytest.raises(CompositionAdmissionError):
        replace(intent().generation, **{field: value})


@pytest.mark.parametrize(
    "value",
    [
        "00000000-0000-0000-0000-000000000000",
        "alias",
        OLD.upper().replace("10000000", "ABCDEF00"),
        "{" + OLD + "}",
        None,
    ],
)
def test_physical_uuid_cannot_be_alias_nil_or_noncanonical(value):
    with pytest.raises(CompositionAdmissionError):
        replace(intent().target, database_id=value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("database", "synthetic.other"),
        ("database", "synthetic;DROP DATABASE target"),
        ("target_table", "target`"),
        ("generation_table", "target"),
        ("target_table", ""),
        ("target_table", "t" * 129),
        ("target_table", "line\nfeed"),
    ],
)
def test_sql_names_are_closed_bounded_and_distinct(field, value):
    with pytest.raises(CompositionAdmissionError):
        replace(intent().target, **{field: value})


def test_principals_must_be_distinct_on_exact_clickhouse_service():
    value = intent()
    with pytest.raises(CompositionAdmissionError, match="principal_reuse"):
        replace(value, publisher_principal=value.ingest_principal)
    with pytest.raises(CompositionAdmissionError, match="principal"):
        replace(value, publisher_principal=replace(value.publisher_principal, service_id=OLD))


@pytest.mark.parametrize(
    "changes",
    [
        {"max_rows": 1},
        {"max_source_bytes": 19},
        {"max_wire_bytes": 23},
        {"max_generation_bytes": 49},
        {"max_retained_bytes": 49},
        {"max_total_transient_bytes": 99},
    ],
)
def test_every_effective_ceiling_applies_including_newly_retained_A(changes):
    value = intent()
    with pytest.raises(CompositionAdmissionError):
        replace(value, limits=replace(value.limits, **changes))


def test_record_roundtrip_retains_complete_closure_and_observation():
    value = intent()
    closure = SnapshotPublisherClosure(value.intent_sha256, digest("closed"), digest("quiescence"))
    row = (
        SnapshotPublicationRecord(value)
        .transition("EXCHANGE_INTENT")
        .transition(
            "PUBLISHED",
            closure=closure,
            observation=observation(value, published=True),
        )
    )
    assert row.record_sha256 == "sha256:" + sha256(row.to_bytes()).hexdigest()
    assert SnapshotPublicationRecord.from_bytes(row.to_bytes(), row.record_sha256) == row
    with pytest.raises(CompositionAdmissionError):
        replace(row, closure=replace(closure, intent_sha256=digest("other intent")))
    for state in ("PREPARED", "EXCHANGE_INTENT", "COMMIT_UNKNOWN", "PUBLISHED", "NOT_PUBLISHED"):
        with pytest.raises(CompositionAdmissionError, match="transition"):
            row.transition(state)


def test_classifier_cannot_create_resolved_record_without_closure_subject():
    value = intent()
    seen = observation(value, published=True)
    assert classify_snapshot(value, seen) == "PUBLISHED"
    with pytest.raises(CompositionAdmissionError, match="outcome_evidence"):
        SnapshotPublicationRecord(value).transition("EXCHANGE_INTENT").transition("PUBLISHED", observation=seen)


@pytest.mark.parametrize("state,revision", [("EXCHANGE_INTENT", 3), ("PUBLISHED", 2), ("COMMIT_UNKNOWN", 2)])
@pytest.mark.parametrize("boundary", ["constructor", "canonical_decode"])
def test_unreachable_state_revisions_reject_even_with_recomputed_digest(state, revision, boundary):
    value = intent()
    row = SnapshotPublicationRecord(value).transition("EXCHANGE_INTENT")
    if state != "EXCHANGE_INTENT":
        row = row.transition(
            state,
            closure=SnapshotPublisherClosure(value.intent_sha256, digest("closed"), digest("quiescence")),
            observation=observation(value, published=True),
        )
    if boundary == "constructor":
        with pytest.raises(CompositionAdmissionError, match="snapshot_revision"):
            replace(row, revision=revision)
    else:
        body = json.loads(row.to_bytes())
        body["revision"] = revision
        document = canonical_json_bytes(body)
        with pytest.raises(CompositionAdmissionError, match="snapshot_record_document"):
            SnapshotPublicationRecord.from_bytes(document, "sha256:" + sha256(document).hexdigest())


@pytest.mark.parametrize(
    "states",
    [
        (),
        ("EXCHANGE_INTENT",),
        ("EXCHANGE_INTENT", "COMMIT_UNKNOWN"),
        ("EXCHANGE_INTENT", "COMMIT_UNKNOWN", "COMMIT_UNKNOWN"),
        ("EXCHANGE_INTENT", "PUBLISHED"),
        ("NOT_PUBLISHED",),
        ("EXCHANGE_INTENT", "NOT_PUBLISHED"),
        ("EXCHANGE_INTENT", "COMMIT_UNKNOWN", "NOT_PUBLISHED"),
        ("EXCHANGE_INTENT", "COMMIT_UNKNOWN", "PUBLISHED"),
    ],
)
def test_reachable_revisions_preserve_cancellation_and_repeated_recovery(states):
    value = intent()
    row = SnapshotPublicationRecord(value)
    for state in states:
        terminal = state in {"PUBLISHED", "NOT_PUBLISHED"}
        row = row.transition(
            state,
            closure=SnapshotPublisherClosure(value.intent_sha256, digest("closed"), digest("quiescence"))
            if terminal
            else None,
            observation=observation(value, published=state == "PUBLISHED") if terminal else None,
        )
    assert row.revision == 1 + len(states)
    assert SnapshotPublicationRecord.from_bytes(row.to_bytes(), row.record_sha256) == row


@pytest.mark.parametrize(
    "pair,expected",
    [
        ((OLD, NEW), "NOT_PUBLISHED"),
        ((NEW, OLD), "PUBLISHED"),
        ((OLD, OLD), "COMMIT_UNKNOWN"),
        ((NEW, NEW), "COMMIT_UNKNOWN"),
        ((None, NEW), "COMMIT_UNKNOWN"),
        ((OLD, None), "COMMIT_UNKNOWN"),
        ((None, None), "COMMIT_UNKNOWN"),
        ((OLD, "10000000-0000-4000-8000-000000000099"), "COMMIT_UNKNOWN"),
    ],
)
def test_uuid_pair_is_classified_exactly(pair, expected):
    value = intent()
    seen = replace(observation(value), target_uuid=pair[0], generation_uuid=pair[1])
    assert classify_snapshot(value, seen) == expected


@pytest.mark.parametrize(
    "changes",
    [
        {"database_engine": "Replicated"},
        {"table_engines": ("MergeTree", "ReplacingMergeTree")},
        {"node_count": 2},
        {"replica_count": 2},
        {"generation_content_sha256": digest("changed")},
        {"schema_sha256": (digest("other"), digest("schema"))},
        {"physical_sha256": (digest("design"), digest("other"))},
        {"generation_rows": 1},
        {"generation_rows": None},
        {"generation_bytes": None},
        {"old_target_bytes": None},
        {"retained_bytes": None},
        {"generation_bytes": 201},
        {"retained_bytes": 101},
        {"old_target_bytes": 501},
        {"target": replace(intent().target, database_id=NEW)},
    ],
)
def test_changed_or_unavailable_physical_content_and_bytes_are_unknown(changes):
    value = intent()
    assert classify_snapshot(value, replace(observation(value), **changes)) == "COMMIT_UNKNOWN"


@pytest.mark.parametrize(
    "feature",
    [
        "incoming_materialized_view",
        "outgoing_materialized_view",
        "cross_database_writer",
        "ttl",
        "projection",
        "default_column",
        "alias_column",
        "materialized_column",
        "row_policy",
        "active_mutation",
        "codec",
        "index",
        "constraint",
        "ambient_writer",
        "remote_storage",
        "on_cluster",
        "unknown_catalog_visibility",
    ],
)
def test_any_unsupported_effect_blocks_pair_certainty(feature):
    value = intent()
    assert classify_snapshot(value, replace(observation(value), unsupported_features=(feature,))) == "COMMIT_UNKNOWN"


def test_method_presence_cannot_substitute_for_typed_catalog_observation():
    value = intent()

    class PretendObservation:
        def __getattr__(self, name):
            return getattr(observation(value, published=True), name)

    with pytest.raises(CompositionAdmissionError, match="classification_shape"):
        classify_snapshot(value, cast(SnapshotCatalogObservation, PretendObservation()))
