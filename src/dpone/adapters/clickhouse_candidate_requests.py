"""Single-inflight candidate request CAS and atomic expected-content accounting."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
from dataclasses import asdict

from dpone.adapters.clickhouse_authority_schema import AuthorityVersion
from dpone.adapters.clickhouse_authority_storage import AuthorityStorage
from dpone.adapters.clickhouse_candidate_sqlite import (
    _candidate_row,
    _definition,
    _expected,
    _invocation_row,
    _json,
    _update_candidate,
)
from dpone.adapters.clickhouse_observation_profile import ProtectedObservationProfile
from dpone.adapters.clickhouse_publication_journal import SQLitePublicationJournal, _storage_errors
from dpone.contracts import clickhouse_candidate as values
from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError, OperationBinding, TransportState
from dpone.contracts.clickhouse_observation import MultisetState


def _request_row(db: sqlite3.Connection, operation_id: str, sequence: int) -> sqlite3.Row:
    cursor = db.cursor()
    try:
        row = cursor.execute(
            "SELECT * FROM candidate_requests WHERE operation_id=? AND sequence=?", (operation_id, sequence)
        ).fetchone()
        if row is None:
            raise AuthorityError("Unknown original candidate request")
        return sqlite3.Row(cursor, row)
    finally:
        cursor.close()


def _request(row: sqlite3.Row, binding: OperationBinding) -> values.CandidateMutationRequest:
    try:
        body = json.loads(row["request"])
        evidence = body["evidence"]
        result = values.CandidateMutationRequest(
            binding,
            body["kind"],
            body["sequence"],
            body["design_digest"],
            body["statement_digest"],
            body["payload_digest"],
            MultisetState(evidence["count"], evidence["total"], evidence["encoded_bytes"], tuple(evidence["nulls"])),
        )
        if (
            _json(asdict(result)) != row["request"]
            or result.query_id != row["query_id"]
            or result.sequence != row["sequence"]
        ):
            raise AuthorityError("Candidate request identity changed")
        return result
    except (TypeError, ValueError, KeyError) as error:
        raise AuthorityError("Invalid original candidate request") from error


class CandidateRequestJournal:
    """Registration is not send permission until its commit is acknowledged."""

    def __init__(self, storage: AuthorityStorage) -> None:
        if storage.version is not AuthorityVersion.V2:
            raise AuthorityError("Candidate requests require explicit authority v2")
        self._storage = storage

    def register(
        self, invocation: values.CandidateInvocation, request: values.CandidateMutationRequest
    ) -> values.CandidateRequestGrant:
        secret = secrets.token_hex(32)
        with _storage_errors(), self._storage.transaction() as db:
            operation = _invocation_row(self._storage, db, invocation)
            if (
                operation["admission_closed"]
                or operation["source_exhausted"]
                or operation["lifecycle"] not in {"registered", "loading"}
            ):
                raise AuthorityConflict("Candidate admission is closed")
            definition = _definition(operation)
            profile = ProtectedObservationProfile(definition.design, definition.limits)
            if request.binding != invocation.binding or request.design_digest != profile.design_digest:
                raise AuthorityConflict("Candidate request does not match original binding/design")
            if (
                len(request.evidence.nulls) != len(definition.design.columns)
                or request.evidence.count > definition.limits.max_batch_rows
                or request.evidence.encoded_bytes > definition.limits.max_batch_bytes
            ):
                raise AuthorityConflict("Candidate request evidence exceeds the admitted profile")
            operation_id = invocation.binding.operation_id
            previous = db.execute(
                "SELECT sequence,state FROM candidate_requests WHERE operation_id=? ORDER BY sequence", (operation_id,)
            ).fetchall()
            if request.sequence != len(previous) or any(
                state != TransportState.CLOSED_TERMINAL for _, state in previous
            ):
                raise AuthorityConflict("Candidate requires successful CREATE and one request in flight")
            if previous and [sequence for sequence, _ in previous] != list(range(len(previous))):
                raise AuthorityError("Candidate request frontier is incomplete")
            db.execute(
                "INSERT INTO candidate_requests(operation_id,sequence,query_id,request,grant_hash) VALUES (?,?,?,?,?)",
                (
                    operation_id,
                    request.sequence,
                    request.query_id,
                    _json(asdict(request)),
                    hashlib.sha256(secret.encode()).hexdigest(),
                ),
            )
            _update_candidate(db, operation, "register", {"lifecycle": "loading"}, request.query_id)
        return values.CandidateRequestGrant._issue(invocation, request, secret)

    def _grant_rows(
        self, db: sqlite3.Connection, grant: values.CandidateRequestGrant
    ) -> tuple[sqlite3.Row, sqlite3.Row]:
        grant.assert_current()
        operation = _invocation_row(self._storage, db, grant._invocation)
        request = grant.request
        row = _request_row(db, request.binding.operation_id, request.sequence)
        if (
            _request(row, grant._invocation.binding) != request
            or grant._revision != 0
            or not hmac.compare_digest(row["grant_hash"], hashlib.sha256(grant._secret.encode()).hexdigest())
        ):
            raise AuthorityConflict("Foreign or changed candidate request grant")
        return operation, row

    def begin_send(self, grant: values.CandidateRequestGrant) -> None:
        """Consume sole send entry before I/O; ambiguous ACK is never retried."""
        grant.assert_current()
        with _storage_errors(), self._storage.transaction() as db:
            operation, row = self._grant_rows(db, grant)
            if (
                row["state"] != TransportState.NOT_STARTED
                or row["revision"] != 0
                or operation["lifecycle"] == "retained"
            ):
                raise AuthorityConflict("Candidate send permission consumed or revoked")
            changed = db.execute(
                "UPDATE candidate_requests SET state='may_have_sent',revision=1 WHERE operation_id=? AND sequence=? AND revision=0 AND state='not_started'",
                (row["operation_id"], row["sequence"]),
            )
            if changed.rowcount != 1:
                raise AuthorityConflict("Candidate send entry changed")
            _update_candidate(db, operation, "begin_send", {"lifecycle": operation["lifecycle"]}, row["query_id"])

    def record_terminal(self, grant: values.CandidateRequestGrant, completion: values.CandidateCompletion) -> None:
        """Only a trusted validated transport return closes work; add rows once."""
        grant.assert_current()
        completion.require_matches(grant.request)
        digest = completion.digest
        with _storage_errors(), self._storage.transaction() as db:
            operation, row = self._grant_rows(db, grant)
            if (
                row["state"] == TransportState.CLOSED_TERMINAL
                and row["completion_digest"] == digest
                and row["revision"] == 2
            ):
                return
            if row["state"] != TransportState.MAY_HAVE_SENT or row["revision"] != 1:
                raise AuthorityConflict("Candidate completion cannot replace unsent or closed work")
            prior, addition = _expected(operation), grant.request.evidence
            aggregate = MultisetState(
                prior.count + addition.count,
                (prior.total + addition.total) % (1 << 256),
                prior.encoded_bytes + addition.encoded_bytes,
                tuple(a + b for a, b in zip(prior.nulls, addition.nulls, strict=True)),
            )
            changed = db.execute(
                "UPDATE candidate_requests SET state='closed_terminal',revision=2,completion_digest=? WHERE operation_id=? AND sequence=? AND revision=1 AND state='may_have_sent'",
                (digest, row["operation_id"], row["sequence"]),
            )
            if changed.rowcount != 1:
                raise AuthorityConflict("Candidate completion revision changed")
            _update_candidate(db, operation, "record_terminal", {"expected": _json(asdict(aggregate))}, row["query_id"])

    def close_unsent(self, operation_id: str) -> None:
        """Revoke provably unsent work; cancellation aborts the candidate load."""
        with _storage_errors(), self._storage.transaction() as db:
            operation = _candidate_row(db, operation_id)
            rows = db.execute(
                "SELECT sequence,query_id,state FROM candidate_requests WHERE operation_id=?", (operation_id,)
            ).fetchall()
            if any(state == TransportState.MAY_HAVE_SENT for _, _, state in rows):
                raise AuthorityConflict("Possibly sent candidate request is not closed")
            unsent = [(sequence, query_id) for sequence, query_id, state in rows if state == TransportState.NOT_STARTED]
            for sequence, query_id in unsent:
                db.execute(
                    "UPDATE candidate_requests SET state='closed_without_send',revision=revision+1 WHERE operation_id=? AND sequence=? AND state='not_started'",
                    (operation_id, sequence),
                )
                _update_candidate(
                    db,
                    operation,
                    "close_unsent",
                    {"admission_closed": 1, "lifecycle": "retained", "retained_reason": "candidate_request_cancelled"},
                    query_id,
                )
                operation = _candidate_row(db, operation_id)

    def requests(self, operation_id: str) -> tuple[values.CandidateRequestStatus, ...]:
        """Read the immutable accepted frontier without recreating its grants."""
        with _storage_errors(), self._storage.transaction() as db:
            _definition(_candidate_row(db, operation_id))
            binding = SQLitePublicationJournal._binding(db, operation_id)
            sequences = db.execute(
                "SELECT sequence FROM candidate_requests WHERE operation_id=? ORDER BY sequence", (operation_id,)
            ).fetchall()
            results = []
            for (sequence,) in sequences:
                row = _request_row(db, operation_id, sequence)
                results.append(
                    values.CandidateRequestStatus(
                        _request(row, binding), TransportState(row["state"]), row["revision"], row["completion_digest"]
                    )
                )
            return tuple(results)
