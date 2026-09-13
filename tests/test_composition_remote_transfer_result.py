"""Real typed originals cross-bind remote results; fixtures grant no SQL authority."""

from dataclasses import asdict, replace

import pytest

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, composition_attempt_epoch_subject
from dpone.contracts.composition_remote_transfer_result import decode_result, decode_status, evidence_digest
from dpone.contracts.composition_snapshot import SnapshotPublicationRecord, SnapshotPublisherClosure
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    attempt_snapshot_target,
    snapshot_generation_from_observation,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.composition_snapshot_helpers import digest, intent, observation


def ref(document):
    return {"document": strict_json_object(document), "sha256": evidence_digest(document)}


def result_body(rows=2):
    value = intent(rows=rows)
    attempt = value.attempt
    value = replace(value, target=attempt_snapshot_target(value.target, attempt))
    subject = SnapshotCaptureSubject(
        attempt, value.target, digest("source binding"), ("db", "dbo", "orders"), value.limits
    )
    captured = SnapshotCaptureRecord(
        subject.subject_sha256,
        value.generation.source_snapshot_sha256,
        digest("payload"),
        value.generation.content_sha256,
        value.generation.schema_sha256,
        value.generation.physical_sha256,
        digest("baseline"),
        value.generation.old_target_uuid,
        (("id", "Int32"),),
        rows,
        value.generation.source_bytes,
        value.generation.wire_bytes,
        40,
        10,
    )
    observed = replace(observation(value), generation_uuid=subject.generation_uuid)
    generation = snapshot_generation_from_observation(subject, captured, observed)
    principals = (value.ingest_principal, value.publisher_principal)

    def proof(kind, authorities, evidence, outcome=None):
        raw = canonical_json_bytes(evidence)
        p = CompositionAttemptProof(
            kind,
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            authorities,
            evidence_digest(raw),
            outcome,
        )
        return {**ref(canonical_json_bytes(p.to_dict())), "evidence_document": evidence}

    closures = {}
    for kind, field in (("CLOSED_GATES", "closed_gates"), ("QUIESCENCE", "quiescence")):
        entries = {}
        for purpose, principal in zip(("ingest", "publisher"), principals, strict=True):
            entries[purpose] = proof(
                kind,
                (principal,),
                {
                    "gate_key": evidence_digest(
                        canonical_json_bytes(
                            {
                                "schema": "dpone.composition-clickhouse-gate.v1",
                                "attempt": asdict(attempt),
                                "target": asdict(value.target),
                                "purpose": purpose,
                            }
                        )
                    ),
                    "gate_id": principal.principal_id.removeprefix("clickhouse-user:"),
                    "phase": "CLOSED",
                    "supervisor": {},
                    "principal": {},
                    "dispatch_terminals": [],
                    "quiescence": "complete-dispatch-barrier-and-empty-server-work",
                },
            )
        entries["terminal"] = proof(
            kind,
            tuple(sorted(principals)),
            {
                "schema": "dpone.composition-clickhouse-terminal-closure.v1",
                "kind": kind,
                "attempt_sha256": attempt.attempt_sha256,
                "ingest_proof_sha256": entries["ingest"]["sha256"],
                "publisher_proof_sha256": entries["publisher"]["sha256"],
            },
        )
        closures[field] = entries
    seal = canonical_json_bytes(
        {
            "schema": "dpone.composition-snapshot-generation-seal.v1",
            "generation": {
                "schema": "dpone.composition-snapshot-generation.v1",
                **{k: v for k, v in asdict(generation).items() if k != "record_sha256"},
            },
            "generation_sha256": generation.record_sha256,
            "closed_gates_sha256": closures["closed_gates"]["ingest"]["sha256"],
            "quiescence_sha256": closures["quiescence"]["ingest"]["sha256"],
            "observation": asdict(observed),
        }
    )
    capture = canonical_json_bytes(
        {
            "schema": "dpone.composition-remote-capture-originals.v1",
            "subject": ref(subject.to_bytes()),
            "captured": ref(captured.to_bytes()),
            "generation_seal": ref(seal),
        }
    )
    value = replace(value, generation=generation, closed_ingest_sha256=closures["closed_gates"]["ingest"]["sha256"])
    published_observation = replace(observation(value, published=True), target_uuid=subject.generation_uuid)
    published = SnapshotPublicationRecord(
        value,
        "PUBLISHED",
        3,
        SnapshotPublisherClosure(
            value.intent_sha256,
            closures["closed_gates"]["publisher"]["sha256"],
            closures["quiescence"]["publisher"]["sha256"],
        ),
        published_observation,
    )
    publication = published.to_bytes()
    outcome = proof(
        "OUTCOME",
        tuple(sorted(principals)),
        {
            "schema": "dpone.composition-clickhouse-terminal-outcome.v1",
            "attempt_sha256": attempt.attempt_sha256,
            "state": "SUCCEEDED",
            "capture_sha256": evidence_digest(capture),
            "publication_sha256": evidence_digest(publication),
        },
        "SUCCEEDED",
    )
    attempt_body = {"schema": "dpone.composition-attempt.v1", **asdict(attempt)}
    receipt = canonical_json_bytes(
        {
            "schema": "dpone.composition-remote-attempt-observation.v1",
            "attempt_document": attempt_body,
            "attempt_sha256": attempt.attempt_sha256,
            "state": "SUCCEEDED",
            "closed_gates_sha256": closures["closed_gates"]["terminal"]["sha256"],
            "quiescence_sha256": closures["quiescence"]["terminal"]["sha256"],
            "outcome_evidence_sha256": outcome["sha256"],
        }
    )
    return attempt, {
        "schema": "dpone.composition-remote-transfer-result.v1",
        "attempt_document": attempt_body,
        "attempt_sha256": attempt.attempt_sha256,
        "capture_document": strict_json_object(capture),
        "capture_sha256": evidence_digest(capture),
        "publication_document": strict_json_object(publication),
        "publication_sha256": evidence_digest(publication),
        "terminal_receipt_document": strict_json_object(receipt),
        "terminal_receipt_sha256": evidence_digest(receipt),
        **closures,
        "outcome": outcome,
        "rows": rows,
    }


@pytest.mark.parametrize("rows", [0, 2])
def test_real_originals_reconstruct_success(rows):
    attempt, body = result_body(rows)
    raw = canonical_json_bytes(body)
    assert decode_result(raw, evidence_digest(raw), attempt=attempt).rows == rows


@pytest.mark.parametrize("field", ["rows", "capture_sha256", "publication_sha256", "terminal_receipt_sha256", "extra"])
def test_result_rejects_count_hash_or_open_shape(field):
    attempt, body = result_body()
    body[field] = True if field == "rows" else digest("foreign")
    raw = canonical_json_bytes(body)
    with pytest.raises(CompositionAdmissionError):
        decode_result(raw, evidence_digest(raw), attempt=attempt)


def test_proof_evidence_must_match_and_both_purposes_are_required():
    for mode in ("evidence", "missing", "receipt", "foreign_attempt"):
        attempt, body = result_body()
        if mode == "evidence":
            body["outcome"]["evidence_document"]["publication_sha256"] = digest("foreign")
        elif mode == "missing":
            del body["closed_gates"]["publisher"]
        elif mode == "receipt":
            body["terminal_receipt_document"]["state"] = "FAILED"
            body["terminal_receipt_sha256"] = evidence_digest(canonical_json_bytes(body["terminal_receipt_document"]))
        else:
            attempt = replace(attempt, try_number=2)
        raw = canonical_json_bytes(body)
        with pytest.raises(CompositionAdmissionError):
            decode_result(raw, evidence_digest(raw), attempt=attempt)


def status_body(state="RUNNING"):
    attempt = intent().attempt
    receipt = canonical_json_bytes(
        {
            "schema": "dpone.composition-remote-attempt-observation.v1",
            "attempt_document": {"schema": "dpone.composition-attempt.v1", **asdict(attempt)},
            "attempt_sha256": attempt.attempt_sha256,
            "state": state,
            "closed_gates_sha256": None,
            "quiescence_sha256": None,
            "outcome_evidence_sha256": None,
        }
    )
    return attempt, {"schema": "dpone.composition-remote-transfer-status.v1", "receipt": ref(receipt), "references": []}


def test_status_is_readback_not_success():
    attempt, body = status_body()
    raw = canonical_json_bytes(body)
    assert decode_status(raw, status="IN_PROGRESS", attempt=attempt).state == "RUNNING"
    assert decode_status(raw, status="UNKNOWN", attempt=attempt).state == "RUNNING"
    with pytest.raises(CompositionAdmissionError):
        decode_status(raw, status="FAILED", attempt=attempt)
    body["references"] = [{"kind": "URL", "sha256": digest("path")}]
    with pytest.raises(CompositionAdmissionError):
        decode_status(canonical_json_bytes(body), status="UNKNOWN", attempt=attempt)


@pytest.mark.parametrize("mutation", ["source", "generation", "count", "target", "epoch", "authority", "gate"])
def test_rehashed_foreign_nested_originals_still_reject(mutation):
    attempt, body = result_body()
    if mutation in {"source", "generation", "count"}:
        capture = body["capture_document"]
        if mutation == "source":
            capture["subject"]["document"]["source_binding_sha256"] = digest("foreign")
            changed = capture["subject"]
        elif mutation == "generation":
            capture["generation_seal"]["document"]["generation_sha256"] = digest("foreign")
            changed = capture["generation_seal"]
        else:
            capture["captured"]["document"]["rows"] += 1
            changed = capture["captured"]
        changed["sha256"] = evidence_digest(canonical_json_bytes(changed["document"]))
        body["capture_sha256"] = evidence_digest(canonical_json_bytes(capture))
    elif mutation == "target":
        body["publication_document"]["intent"]["target"]["target_table"] = "foreign"
        body["publication_sha256"] = evidence_digest(canonical_json_bytes(body["publication_document"]))
    else:
        proof = body["closed_gates"]["ingest"]
        if mutation == "epoch":
            proof["document"]["guard_epochs_sha256"] = digest("foreign")
        elif mutation == "authority":
            proof["document"]["authorities"] = body["closed_gates"]["publisher"]["document"]["authorities"]
        else:
            proof["evidence_document"]["gate_key"] = digest("foreign")
            proof["document"]["evidence_sha256"] = evidence_digest(canonical_json_bytes(proof["evidence_document"]))
        proof["sha256"] = evidence_digest(canonical_json_bytes(proof["document"]))
    raw = canonical_json_bytes(body)
    with pytest.raises(CompositionAdmissionError):
        decode_result(raw, evidence_digest(raw), attempt=attempt)


def test_status_reference_order_duplicates_and_size_are_closed():
    attempt, body = status_body()
    reference = {"kind": "CAPTURE", "sha256": digest("capture")}
    for references in (
        [reference, reference],
        [reference] * 17,
        [{"kind": "OUTCOME", "sha256": digest("o")}, reference],
    ):
        body["references"] = references
        with pytest.raises(CompositionAdmissionError):
            decode_status(canonical_json_bytes(body), status="UNKNOWN", attempt=attempt)
    with pytest.raises(CompositionAdmissionError):
        decode_status(b" " * (1024 * 1024 + 1), status="UNKNOWN", attempt=attempt)
