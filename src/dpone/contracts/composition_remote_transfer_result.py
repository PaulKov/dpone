"""Exact whole-cell evidence consistency, never a replacement for protected SQL.

The terminal receipt is a derived SQL readback. Original capture, publication
and proof bytes must be read and authenticated by the protected producer; these
codecs reject contradictions but do not confer authority on caller-made bytes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionAttemptReceipt,
    decode_attempt_identity,
    decode_attempt_proof,
)
from dpone.contracts.composition_snapshot import SnapshotPublicationRecord
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    decode_generation_seal,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_EVIDENCE_BYTES = 1024 * 1024
RESULT_SCHEMA = "dpone.composition-remote-transfer-result.v1"
_FIELDS = {
    "schema",
    "attempt_document",
    "attempt_sha256",
    "capture_document",
    "capture_sha256",
    "publication_document",
    "publication_sha256",
    "terminal_receipt_document",
    "terminal_receipt_sha256",
    "closed_gates",
    "quiescence",
    "outcome",
    "rows",
}


def evidence_digest(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


def _object(document: bytes) -> dict[str, Any]:
    if type(document) is not bytes or not 0 < len(document) <= MAX_EVIDENCE_BYTES:
        raise ValueError
    body = strict_json_object(document)
    if canonical_json_bytes(body) != document:
        raise ValueError
    return body


def _bytes(body: Any, digest: str) -> bytes:
    require_digest(digest)
    document = canonical_json_bytes(body)
    _object(document)
    if evidence_digest(document) != digest:
        raise ValueError
    return document


def _original(value: Any) -> bytes:
    if type(value) is not dict or set(value) != {"document", "sha256"}:
        raise ValueError
    return _bytes(value["document"], value["sha256"])


def decode_receipt_observation(document: bytes, expected: str) -> CompositionAttemptReceipt:
    """Validate a derived current SQL receipt, without claiming stored wire bytes."""
    try:
        body = _object(document)
        if (
            evidence_digest(document) != expected
            or set(body)
            != {
                "schema",
                "attempt_document",
                "attempt_sha256",
                "state",
                "closed_gates_sha256",
                "quiescence_sha256",
                "outcome_evidence_sha256",
            }
            or body["schema"] != "dpone.composition-remote-attempt-observation.v1"
        ):
            raise ValueError
        attempt = decode_attempt_identity(canonical_json_bytes(body["attempt_document"]), body["attempt_sha256"])
        receipt = CompositionAttemptReceipt(
            attempt,
            body["state"],
            body["closed_gates_sha256"],
            body["quiescence_sha256"],
            body["outcome_evidence_sha256"],
        )
        receipt.__post_init__()
        return receipt
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("remote_transfer_receipt") from None


def _proof(value: Any, attempt: CompositionAttemptIdentity, kind: str) -> CompositionAttemptProof:
    if type(value) is not dict or set(value) != {"document", "sha256", "evidence_document"}:
        raise ValueError
    proof = decode_attempt_proof(_bytes(value["document"], value["sha256"]), value["sha256"]).require_attempt(attempt)
    _bytes(value["evidence_document"], proof.evidence_sha256)
    if proof.kind != kind:
        raise ValueError
    return proof


def _purpose_proofs(body, kind, attempt, principals, target):
    if type(body) is not dict or set(body) != {"ingest", "publisher"}:
        raise ValueError
    proofs = tuple(_proof(body[name], attempt, kind) for name in ("ingest", "publisher"))
    if tuple(p.authorities for p in proofs) != ((principals[0],), (principals[1],)):
        raise ValueError
    # Purpose evidence has no independent public decoder. Require the existing
    # producer's closed outer shape and identity; SQL reader validates its nested
    # supervisor/principal/dispatch originals against actual enrollment and claims.
    for name, principal in zip(("ingest", "publisher"), principals, strict=True):
        evidence = body[name]["evidence_document"]
        if set(evidence) != {
            "gate_key",
            "gate_id",
            "phase",
            "supervisor",
            "principal",
            "dispatch_terminals",
            "quiescence",
        }:
            raise ValueError
        expected_key = evidence_digest(
            canonical_json_bytes(
                {
                    "schema": "dpone.composition-clickhouse-gate.v1",
                    "attempt": asdict(attempt),
                    "target": asdict(target),
                    "purpose": name,
                }
            )
        )
        if evidence["gate_key"] != expected_key:
            raise ValueError
        if (
            evidence["phase"] != "CLOSED"
            or "clickhouse-user:" + evidence["gate_id"] != principal.principal_id
            or evidence["quiescence"] != "complete-dispatch-barrier-and-empty-server-work"
        ):
            raise ValueError
    return proofs


def _closures(body, kind, attempt, principals, target):
    if type(body) is not dict or set(body) != {"terminal", "ingest", "publisher"}:
        raise ValueError
    ingest, publisher = _purpose_proofs(
        {n: body[n] for n in ("ingest", "publisher")}, kind, attempt, principals, target
    )
    terminal = _proof(body["terminal"], attempt, kind)
    if terminal.authorities != tuple(sorted(principals)) or body["terminal"]["evidence_document"] != {
        "schema": "dpone.composition-clickhouse-terminal-closure.v1",
        "kind": kind,
        "attempt_sha256": attempt.attempt_sha256,
        "ingest_proof_sha256": ingest.proof_sha256,
        "publisher_proof_sha256": publisher.proof_sha256,
    }:
        raise ValueError
    return terminal, ingest, publisher


@dataclass(frozen=True, slots=True)
class PublicationOriginals:
    """Receipt-independent consistency of a sealed capture and publication."""

    subject: SnapshotCaptureSubject
    captured: SnapshotCaptureRecord
    published: SnapshotPublicationRecord


def decode_publication_originals(
    capture_document: bytes,
    publication_document: bytes,
    *,
    attempt: CompositionAttemptIdentity,
    closed_gates: dict[str, Any],
    quiescence: dict[str, Any],
) -> PublicationOriginals:
    """Validate nested originals before finalization; never synthesize a receipt."""
    try:
        capture = _object(capture_document)
        if (
            set(capture) != {"schema", "subject", "captured", "generation_seal"}
            or capture["schema"] != "dpone.composition-remote-capture-originals.v1"
        ):
            raise ValueError
        subject = SnapshotCaptureSubject.from_bytes(_original(capture["subject"]))
        captured = SnapshotCaptureRecord.from_bytes(_original(capture["captured"]))
        seal_document = _original(capture["generation_seal"])
        generation = decode_generation_seal(subject, captured, seal_document)
        seal = _object(seal_document)
        _object(publication_document)
        published = SnapshotPublicationRecord.from_bytes(publication_document, evidence_digest(publication_document))
        intent = published.intent
        if (
            subject.attempt != attempt
            or intent.attempt != attempt
            or (intent.target, intent.limits, intent.generation) != (subject.target, subject.limits, generation)
            or published.state != "PUBLISHED"
            or published.observation is None
            or published.observation.generation_rows != captured.rows
        ):
            raise ValueError
        principals = intent.ingest_principal, intent.publisher_principal
        closed = _purpose_proofs(closed_gates, "CLOSED_GATES", attempt, principals, intent.target)
        quiet = _purpose_proofs(quiescence, "QUIESCENCE", attempt, principals, intent.target)
        if (
            (seal["closed_gates_sha256"], seal["quiescence_sha256"], intent.closed_ingest_sha256)
            != (closed[0].proof_sha256, quiet[0].proof_sha256, closed[0].proof_sha256)
            or published.closure is None
            or (published.closure.closed_gates_sha256, published.closure.quiescence_sha256)
            != (closed[1].proof_sha256, quiet[1].proof_sha256)
        ):
            raise ValueError
        return PublicationOriginals(subject, captured, published)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("remote_transfer_publication_originals") from None


@dataclass(frozen=True, slots=True)
class RemoteTransferResult:
    """Validated evidence bytes and count; no source rows or secret material."""

    document: bytes
    attempt: CompositionAttemptIdentity
    rows: int

    @property
    def sha256(self) -> str:
        return evidence_digest(self.document)


def decode_result(
    document: bytes, expected_sha256: str, *, attempt: CompositionAttemptIdentity
) -> RemoteTransferResult:
    try:
        body = _object(document)
        if set(body) != _FIELDS or body["schema"] != RESULT_SCHEMA or evidence_digest(document) != expected_sha256:
            raise ValueError
        observed_attempt = decode_attempt_identity(
            canonical_json_bytes(body["attempt_document"]), body["attempt_sha256"]
        )
        if observed_attempt != attempt:
            raise ValueError
        nested = decode_publication_originals(
            _bytes(body["capture_document"], body["capture_sha256"]),
            _bytes(body["publication_document"], body["publication_sha256"]),
            attempt=attempt,
            closed_gates={n: body["closed_gates"][n] for n in ("ingest", "publisher")},
            quiescence={n: body["quiescence"][n] for n in ("ingest", "publisher")},
        )
        captured, published = nested.captured, nested.published
        intent = published.intent
        receipt = decode_receipt_observation(
            _bytes(body["terminal_receipt_document"], body["terminal_receipt_sha256"]), body["terminal_receipt_sha256"]
        )
        if (
            receipt.attempt != attempt
            or receipt.state != "SUCCEEDED"
            or type(body["rows"]) is not int
            or body["rows"] != captured.rows
        ):
            raise ValueError
        principals = (intent.ingest_principal, intent.publisher_principal)
        closed = _closures(body["closed_gates"], "CLOSED_GATES", attempt, principals, intent.target)
        quiet = _closures(body["quiescence"], "QUIESCENCE", attempt, principals, intent.target)
        outcome = _proof(body["outcome"], attempt, "OUTCOME")
        if (
            outcome.authorities != tuple(sorted(principals))
            or outcome.outcome_state != "SUCCEEDED"
            or (receipt.closed_gates_sha256, receipt.quiescence_sha256, receipt.outcome_evidence_sha256)
            != (closed[0].proof_sha256, quiet[0].proof_sha256, outcome.proof_sha256)
        ):
            raise ValueError
        if body["outcome"]["evidence_document"] != {
            "schema": "dpone.composition-clickhouse-terminal-outcome.v1",
            "attempt_sha256": attempt.attempt_sha256,
            "state": "SUCCEEDED",
            "capture_sha256": body["capture_sha256"],
            "publication_sha256": body["publication_sha256"],
        }:
            raise ValueError
        return RemoteTransferResult(document, attempt, captured.rows)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("remote_transfer_result") from None


def decode_status(document: bytes, *, status: str, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
    try:
        body = _object(document)
        if (
            set(body) != {"schema", "receipt", "references"}
            or body["schema"] != "dpone.composition-remote-transfer-status.v1"
        ):
            raise ValueError
        receipt = decode_receipt_observation(_original(body["receipt"]), body["receipt"]["sha256"])
        allowed = {"FAILED": {"FAILED"}, "IN_PROGRESS": {"RUNNING"}, "UNKNOWN": {"RUNNING", "COMMIT_UNKNOWN"}}
        if receipt.attempt != attempt or receipt.state not in allowed[status]:
            raise ValueError
        refs = body["references"]
        if type(refs) is not list or len(refs) > 16:
            raise ValueError
        keys = []
        for reference in refs:
            if (
                type(reference) is not dict
                or set(reference) != {"kind", "sha256"}
                or reference["kind"] not in {"CAPTURE", "PUBLICATION", "CLOSED_GATES", "QUIESCENCE", "OUTCOME"}
            ):
                raise ValueError
            require_digest(reference["sha256"])
            keys.append((reference["kind"], reference["sha256"]))
        if keys != sorted(set(keys)):
            raise ValueError
        return receipt
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("remote_transfer_status") from None
