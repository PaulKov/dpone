"""Protected SQL persistence for supervisor-only dbt originals and dispatch.

The mandatory source verifier derives registration from verified immutable
release/plan/toolchain inputs, not a worker-supplied intent. SQL independently
proves the ACTIVE parent, RUNNING attempt and original issued login SID. Control
credentials and this store are never exposed to the dbt child UID.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from dpone.adapters.composition_dbt_capture_schema import require_dbt_capture_schema
from dpone.adapters.composition_mssql_attempts import ConnectionFactory
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_issuance import require_login
from dpone.adapters.composition_mssql_login_gate import _read_gate
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import close, rollback
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_capture_codec import (
    decode_event,
    decode_registration,
    document_sha256,
    encode_event,
    encode_registration,
)
from dpone.contracts.composition_dbt_outcome import (
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtDispatchIntent,
    DbtExitRecord,
    DbtOutcomeExpectation,
)
from dpone.contracts.strict_json import strict_json_object

SourceVerifier = Callable[[CompositionAttemptIdentity], tuple[DbtDispatchIntent, DbtOutcomeExpectation]]


class MssqlDbtCaptureStore:
    """Append-only records with committed originals reopened on a fresh session.

    Dispatch is never idempotently acknowledged: once its row exists, every
    retry rejects without an executor permit, including after an ACK was lost.
    Other exact immutable repeats permit recovery without changing originals.
    ``closure_verifier`` is an optional protected controller observer, never a
    caller closure document. It is required only to register UNDISPATCHED.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        control_database: str,
        source_verifier: SourceVerifier,
        control_schema: str = "dpone_control",
        closure_verifier: Callable[..., bytes] | None = None,
    ):
        if not callable(source_verifier) or not control_database:
            raise DbtCaptureError("capture_source_verifier")
        self._factory, self._service_id = connection_factory, expected_service_id
        self._database, self._schema = control_database, require_control_schema(control_schema)
        self._verify_source, self._verify_closure = source_verifier, closure_verifier

    def register(self, attempt: CompositionAttemptIdentity) -> DbtDispatchIntent:
        """Derive registration once; no supplied DTO can self-assert provenance."""
        attempt.__post_init__()
        try:
            intent, expectation = self._verify_source(attempt)
        except Exception:
            raise DbtCaptureError("capture_source_verification_failed") from None
        if (
            type(intent) is not DbtDispatchIntent
            or type(expectation) is not DbtOutcomeExpectation
            or intent.attempt != attempt
        ):
            raise DbtCaptureError("capture_registration_subject")
        if intent.supervisor_uid != 0 or min(intent.child_uid, intent.child_gid) <= 0:
            raise DbtCaptureError("capture_supervisor_boundary")
        if (
            attempt.constituent_id != "native"
            or dict(expectation.evidence_subject).get("toolchain_sha256") != intent.toolchain_sha256
        ):
            raise DbtCaptureError("capture_registration_subject")
        raw = encode_registration(intent, expectation)
        with self._transaction() as ledger:
            self._authorize(ledger, intent, active=True)
            previous = self._registration(ledger, attempt)
            if previous is None:
                ledger.cursor.execute(
                    f"INSERT INTO {ledger.table('dbt_registrations')} "
                    "(operation_key,intent_sha256,login_sid,document_sha256,document) VALUES (?,?,?,?,?);",
                    attempt.attempt_sha256,
                    intent.intent_sha256,
                    bytes.fromhex(intent.sql_principal_sid.removeprefix("mssql-sid:")),
                    document_sha256(raw),
                    raw,
                )
            elif previous != (intent, expectation):
                raise DbtCaptureError("capture_registration_conflict")
            if self._registration(ledger, attempt) != (intent, expectation):
                raise DbtCaptureError("capture_registration_readback")
        if self._load(attempt)[0] != (intent, expectation):
            raise DbtCaptureError("capture_registration_readback")
        return intent

    def load_intent(self, attempt: CompositionAttemptIdentity) -> DbtDispatchIntent:
        return self._load(attempt)[0][0]

    def load_expectation(self, attempt: CompositionAttemptIdentity) -> DbtOutcomeExpectation:
        return self._load(attempt)[0][1]

    def record_dispatch_once(self, intent: DbtDispatchIntent, preflight: DbtArtifactOriginal) -> None:
        self._append(intent, "DISPATCH", preflight)

    def record_exit_once(self, value: DbtExitRecord) -> None:
        self._append(value.intent, "EXIT", value)

    def capture_once(self, value: DbtCaptureRecord) -> None:
        if value.phase != "CAPTURED":
            raise DbtCaptureError("capture_phase")
        self._append(value.intent, "CAPTURE", value)

    def read_exit(self, attempt: CompositionAttemptIdentity) -> DbtExitRecord | None:
        return self._load(attempt)[1].get("EXIT")

    def read_capture(self, attempt: CompositionAttemptIdentity) -> DbtCaptureRecord | None:
        events = self._load(attempt)[1]
        return events.get("CAPTURE", events.get("UNDISPATCHED"))

    def record_undispatched(self, attempt: CompositionAttemptIdentity) -> None:
        """Prove protected closure and absence of dispatch under one ledger lock."""
        if self._verify_closure is None:
            raise DbtCaptureError("capture_closure_verifier")
        with self._transaction() as ledger:
            registration, events = self._load_in(ledger, attempt)
            intent = registration[0]
            gate = self._authorize(ledger, intent, active=False)
            if gate.state != "CLOSED" or events:
                raise DbtCaptureError("capture_undispatched_conflict")
            transaction = ledger.require_transaction()
            closure = self._verify_closure(ledger, attempt)
            ledger.require_transaction(transaction)
            expected = {
                "attempt_sha256": attempt.attempt_sha256,
                "intent_sha256": intent.intent_sha256,
                "build_dispatched": False,
                "closed": True,
            }
            if (
                type(closure) is not bytes
                or strict_json_object(closure) != expected
                or strict_json_object(closure).get("closed") is not True
                or strict_json_object(closure).get("build_dispatched") is not False
            ):
                raise DbtCaptureError("capture_undispatched_closure")
            value = DbtCaptureRecord(intent, "UNDISPATCHED", undispatched_closure_original=closure)
            self._insert(ledger, intent, "UNDISPATCHED", value)
            if self._load_in(ledger, attempt)[1] != {"UNDISPATCHED": value}:
                raise DbtCaptureError("capture_event_readback")
        if self.read_capture(attempt) != value:
            raise DbtCaptureError("capture_event_readback")

    @contextmanager
    def _transaction(self):
        connection = cursor = None
        committing = False
        try:
            connection = self._factory()
            connection.autocommit = False
            cursor = connection.cursor()
            ledger = CompositionMssqlLedger(cursor, self._schema)
            transaction = ledger.begin(self._service_id)
            require_dbt_capture_schema(cursor, self._schema)
            yield ledger
            ledger.require_transaction(transaction)
            committing = True
            connection.commit()
        except (DbtCaptureError, CompositionAdmissionError):
            if committing:
                raise DbtCaptureError("capture_commit_unknown") from None
            rollback(connection)
            raise
        except Exception:
            if not committing:
                rollback(connection)
            raise DbtCaptureError("capture_commit_unknown" if committing else "capture_control_unknown") from None
        finally:
            close(cursor)
            close(connection)

    def _authorize(self, ledger: Any, intent: DbtDispatchIntent, *, active: bool):
        occurrence, receipt = require_existing_execution_in(
            ledger, intent.attempt, expected_service_id=self._service_id, terminal_validator=ledger.terminal_validator
        )
        if active:
            occurrence.require_state("ACTIVE")
            if receipt.state != "RUNNING":
                raise DbtCaptureError("capture_attempt_not_running")
        gate = _read_gate(ledger, intent.attempt, self._database)
        if "mssql-sid:" + gate.sid.hex() != intent.sql_principal_sid or (active and gate.state != "READY"):
            raise DbtCaptureError("capture_issued_sid")
        if gate.state == "JOURNALED":
            raise DbtCaptureError("capture_gate_not_issued")
        require_login(ledger, gate.name, gate.sid, disabled=gate.state != "READY")
        ledger.require_transaction()
        return gate

    def _registration(self, ledger: Any, attempt: CompositionAttemptIdentity):
        ledger.cursor.execute(
            f"SELECT TOP (2) intent_sha256,login_sid,document_sha256,document FROM {ledger.table('dbt_registrations')} WITH (HOLDLOCK) WHERE operation_key=?;",
            attempt.attempt_sha256,
        )
        rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 4:
            raise DbtCaptureError("capture_registration_rows")
        intent, expectation = decode_registration(rows[0][3], rows[0][2])
        if (
            intent.attempt != attempt
            or intent.intent_sha256 != rows[0][0]
            or rows[0][1] != bytes.fromhex(intent.sql_principal_sid.removeprefix("mssql-sid:"))
        ):
            raise DbtCaptureError("capture_registration_identity")
        return intent, expectation

    def _load_in(self, ledger: Any, attempt: CompositionAttemptIdentity):
        transaction = ledger.require_transaction()
        require_dbt_capture_schema(ledger.cursor, self._schema)
        registration = self._registration(ledger, attempt)
        if registration is None:
            raise DbtCaptureError("capture_registration_missing")
        self._authorize(ledger, registration[0], active=False)
        ledger.cursor.execute(
            f"SELECT TOP (5) phase,document_sha256,document FROM {ledger.table('dbt_events')} WITH (HOLDLOCK) WHERE operation_key=?;",
            attempt.attempt_sha256,
        )
        events = {}
        for row in ledger.cursor.fetchall():
            if len(row) != 3 or row[0] in events:
                raise DbtCaptureError("capture_event_rows")
            events[row[0]] = decode_event(row[2], row[1], row[0])
        self._require_events(registration[0], events)
        ledger.require_transaction(transaction)
        return registration, events

    def _load(self, attempt: CompositionAttemptIdentity):
        with self._transaction() as ledger:
            return self._load_in(ledger, attempt)

    @staticmethod
    def _require_events(intent: DbtDispatchIntent, events: dict[str, Any]) -> None:
        if "UNDISPATCHED" in events:
            if set(events) != {"UNDISPATCHED"} or events["UNDISPATCHED"].intent != intent:
                raise DbtCaptureError("capture_phase_order")
        if "DISPATCH" in events:
            original = events["DISPATCH"]
            if (original.role, original.relative_path) != intent.artifact_paths[
                0
            ] or original.sha256 != intent.preflight_manifest_sha256:
                raise DbtCaptureError("capture_preflight_identity")
        if "EXIT" in events:
            exited = events["EXIT"]
            if exited.intent != intent or exited.preflight_original != events.get("DISPATCH"):
                raise DbtCaptureError("capture_phase_order")
        if "CAPTURE" in events:
            captured = events["CAPTURE"]
            if captured.intent != intent or captured.exit_record != events.get("EXIT"):
                raise DbtCaptureError("capture_phase_order")

    def _append(self, intent: DbtDispatchIntent, phase: str, value: Any) -> None:
        with self._transaction() as ledger:
            registration, events = self._load_in(ledger, intent.attempt)
            if registration[0] != intent:
                raise DbtCaptureError("capture_intent_mismatch")
            if phase == "DISPATCH":
                self._authorize(ledger, intent, active=True)
            if phase in events:
                if phase == "DISPATCH" or events[phase] != value:
                    raise DbtCaptureError(
                        "capture_dispatch_replay" if phase == "DISPATCH" else "capture_event_conflict"
                    )
            else:
                _, receipt = require_existing_execution_in(
                    ledger,
                    intent.attempt,
                    expected_service_id=self._service_id,
                    terminal_validator=ledger.terminal_validator,
                )
                if receipt.state not in {"RUNNING", "COMMIT_UNKNOWN"}:
                    raise DbtCaptureError("capture_attempt_terminal")
                self._require_events(intent, {**events, phase: value})
                self._insert(ledger, intent, phase, value)
            if self._load_in(ledger, intent.attempt)[1].get(phase) != value:
                raise DbtCaptureError("capture_event_readback")
        if self._load(intent.attempt)[1].get(phase) != value:
            raise DbtCaptureError("capture_event_readback")

    @staticmethod
    def _insert(ledger: Any, intent: DbtDispatchIntent, phase: str, value: Any) -> None:
        raw = encode_event(phase, value)
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('dbt_events')} (operation_key,phase,document_sha256,document) VALUES (?,?,?,?);",
            intent.attempt.attempt_sha256,
            phase,
            document_sha256(raw),
            raw,
        )
