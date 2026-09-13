"""Exact bounded journal originals under the existing global SQL transaction.

These queries grant no dispatch or closure authority. The owning store brackets
protected gate/budget/observer callbacks and commits the resulting originals.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Any

from dpone.adapters.composition_clickhouse_dispatch_schema import require_clickhouse_dispatch_schema
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatch,
    ClickHouseDispatchStatus,
    decode_clickhouse_dispatch,
)
from dpone.contracts.composition_clickhouse_dispatch import (
    DispatchBinding as DispatchBinding,
)
from dpone.contracts.composition_clickhouse_dispatch import (
    require_terminal_document as require_terminal_document,
)
from dpone.contracts.composition_clickhouse_dispatch import (
    terminal_document as terminal_document,
)
from dpone.contracts.composition_identity import CompositionAdmissionError


def require(value: bool, reason: str) -> None:
    if not value:
        raise CompositionAdmissionError("clickhouse_journal_" + reason)


def document_sha256(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


class DispatchQueries:
    """Pin transaction identity across every bounded read and append."""

    def __init__(self, ledger: CompositionMssqlLedger, binding: DispatchBinding) -> None:
        self.ledger, self.binding = ledger, binding
        self._cursor, self._schema, self._service = ledger.cursor, ledger.schema, ledger.expected_service_id
        self.transaction = ledger.require_transaction()
        require_clickhouse_dispatch_schema(ledger.cursor, ledger.schema)
        self.check()

    def check(self) -> None:
        require(
            self.ledger.cursor is self._cursor
            and (self.ledger.schema, self.ledger.expected_service_id) == (self._schema, self._service),
            "context_identity",
        )
        self.ledger.require_transaction(self.transaction)

    def rows(self, sql: str, *parameters: Any) -> tuple[tuple[Any, ...], ...]:
        self.check()
        self.ledger.cursor.execute(sql, *parameters)
        rows = tuple(tuple(value) for value in self.ledger.cursor.fetchall())
        self.check()
        return rows

    def append(self, table: str, columns: str, values: tuple[object, ...]) -> None:
        self.check()
        placeholders = ", ".join("?" for _ in values)
        self.ledger.cursor.execute(
            f"INSERT INTO {self.ledger.table(table)} ({columns}) VALUES ({placeholders});", *values
        )
        self.check()

    def require_scope(self, *, recovery: bool):
        self.check()
        attempt = self.binding.attempt
        occurrence, receipt = require_existing_execution_in(
            self.ledger,
            attempt,
            expected_service_id=self.ledger.expected_service_id,
            terminal_validator=self.ledger.terminal_validator,
        )
        self.check()
        require(occurrence.receipt.state in ({"ACTIVE", "RETIRING"} if recovery else {"ACTIVE"}), "occurrence_state")
        require(receipt.state in ({"RUNNING", "COMMIT_UNKNOWN"} if recovery else {"RUNNING"}), "attempt_state")
        self._require_membership(occurrence)
        return occurrence, receipt

    def require_status_scope(self):
        """Audit retained history without granting any mutation or recovery permit."""
        self.check()
        original = require_existing_execution_in(
            self.ledger,
            self.binding.attempt,
            expected_service_id=self.ledger.expected_service_id,
            terminal_validator=self.ledger.terminal_validator,
        )
        self.check()
        self._require_membership(original[0])
        if original[1].state in {"SUCCEEDED", "FAILED"}:
            # The existing-operation audit exempts the current nonretired
            # operation. A status read must still reopen its terminal proofs.
            self.check()
            self.ledger.terminal_validator(self.ledger, *original)
            self.check()
        return original

    def _require_membership(self, occurrence) -> None:
        attempt, target = self.binding.attempt, self.binding.target
        workload = next(row for row in occurrence.request.workloads if row.workload_id == attempt.workload_id)
        resource = next((row for row in occurrence.request.resources if row.guard_id == target.guard_id), None)
        require(resource is not None, "target_guard")
        assert resource is not None
        require(
            (resource.connector, resource.service_id, resource.physical_subject_sha256)
            == ("clickhouse", target.service_id, target.physical_subject_sha256)
            and workload.execution_cell == "mssql_clickhouse_full_refresh_v1"
            and target.write_subject_sha256 in workload.write_subjects
            and target.write_subject_sha256 in resource.write_subjects,
            "target_scope",
        )
        issued = self.rows(
            "SELECT TOP (2) /* dispatch_issued */ operation_key FROM "
            + self.ledger.table("issued_authorities")
            + " WITH (HOLDLOCK) WHERE connector='clickhouse' AND service_id=? AND principal_id=?;",
            target.service_id,
            self.binding.principal_id,
        )
        require(issued == ((attempt.attempt_sha256,),), "issued_identity")

    def status(self, dispatch_sha256: str) -> ClickHouseDispatchStatus | None:
        rows = self.rows(self._claim_select() + "WHERE dispatch_sha256=?;", dispatch_sha256)
        require(len(rows) <= 1, "claim_original")
        if not rows:
            return None
        dispatch = self.decode_claim(rows[0])
        require(dispatch.dispatch_sha256 == dispatch_sha256, "claim_identity")
        return ClickHouseDispatchStatus(self.binding, dispatch_sha256, rows[0][5], self.terminal(dispatch))

    def require_open(self) -> None:
        rows = self.rows(
            f"SELECT TOP (3) phase FROM {self.ledger.table('ch_dispatch_closures')} WITH (UPDLOCK,HOLDLOCK) WHERE gate_id=?;",
            self.binding.gate_id,
        )
        require(not rows, "gate_closing")

    def _claim_select(self) -> str:
        return (
            "SELECT TOP (2) dispatch_sha256,claim_key,operation_key,LOWER(CONVERT(char(36),gate_id)),query_id,"
            "CASE WHEN DATALENGTH(dispatch_document) BETWEEN 1 AND 1048576 THEN dispatch_document END "
            f"FROM {self.ledger.table('ch_dispatches')} WITH (UPDLOCK,HOLDLOCK) "
        )

    def require_unclaimed(self, dispatch: ClickHouseDispatch) -> None:
        rows = self.rows(
            self._claim_select() + "WHERE claim_key=? OR dispatch_sha256=? OR query_id=?;",
            dispatch.claim_key,
            dispatch.dispatch_sha256,
            dispatch.query_id,
        )
        require(not rows, "dispatch_replay")

    def decode_claim(self, row: tuple[Any, ...]) -> ClickHouseDispatch:
        require(len(row) == 6, "claim_original")
        dispatch = decode_clickhouse_dispatch(row[5], row[0])
        self.binding.require_dispatch(dispatch)
        require(
            row[:5]
            == (
                dispatch.dispatch_sha256,
                dispatch.claim_key,
                dispatch.attempt.attempt_sha256,
                self.binding.gate_id,
                dispatch.query_id,
            ),
            "claim_identity",
        )
        return dispatch

    def require_claim(self, dispatch: ClickHouseDispatch) -> None:
        rows = self.rows(self._claim_select() + "WHERE dispatch_sha256=?;", dispatch.dispatch_sha256)
        require(len(rows) == 1 and self.decode_claim(rows[0]) == dispatch, "claim_missing")

    def terminal(self, dispatch: ClickHouseDispatch) -> tuple[str, bytes] | None:
        rows = self.rows(
            "SELECT TOP (2) terminal_sha256,CASE WHEN DATALENGTH(terminal_document) BETWEEN 1 AND 65536 "
            f"THEN terminal_document END FROM {self.ledger.table('ch_dispatch_terminals')} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE dispatch_sha256=?;",
            dispatch.dispatch_sha256,
        )
        require(len(rows) <= 1 and all(len(row) == 2 for row in rows), "terminal_original")
        if not rows:
            return None
        digest, document = rows[0]
        require_terminal_document(dispatch, digest, document)
        return digest, document

    def drained_links(self) -> tuple[tuple[str, str], ...]:
        """Audit every original, including pending pre-send/partial-response claims."""
        links: list[tuple[str, str]] = []
        prior = None
        claims, queries = set(), set()
        while True:
            rows = self.rows(
                self._claim_select().replace("TOP (2)", "TOP (1)")
                + "WHERE gate_id=? "
                + ("" if prior is None else "AND dispatch_sha256>? ")
                + "ORDER BY dispatch_sha256;",
                self.binding.gate_id,
                *(() if prior is None else (prior,)),
            )
            if not rows:
                return tuple(links)
            require(len(rows) == 1 and len(links) < 8192, "dispatch_budget")
            dispatch = self.decode_claim(rows[0])
            key = dispatch.dispatch_sha256
            require(prior is None or key > prior, "dispatch_order")
            require(dispatch.claim_key not in claims and dispatch.query_id not in queries, "dispatch_duplicate")
            claims.add(dispatch.claim_key)
            queries.add(dispatch.query_id)
            terminal = self.terminal(dispatch)
            require(terminal is not None, "dispatch_unresolved")
            assert terminal is not None
            links.append((key, terminal[0]))
            prior = key

    def phase(self, phase: str) -> tuple[str, bytes] | None:
        rows = self.rows(
            "SELECT TOP (2) evidence_sha256,CASE WHEN DATALENGTH(evidence_document) BETWEEN 1 AND 8388608 "
            f"THEN evidence_document END FROM {self.ledger.table('ch_dispatch_closures')} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE gate_id=? AND phase=?;",
            self.binding.gate_id,
            phase,
        )
        require(len(rows) <= 1 and all(len(row) == 2 for row in rows), "closure_original")
        if not rows:
            return None
        digest, document = rows[0]
        require(type(document) is bytes and document_sha256(document) == digest, "closure_original")
        return digest, document

    def require_closing(self) -> None:
        original = self.binding.closing_document()
        require(self.phase("CLOSING") == (document_sha256(original), original), "closing_required")
