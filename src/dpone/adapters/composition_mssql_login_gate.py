"""One-time SQL writer authority over a monotonic, synchronous LOGON gate.

No constructor or method provisions the control schema. A trusted controller
connection and freshly enrolled exclusive target databases are required. Every
creation/enablement/commit ambiguity returns no secret and attempts closure;
unsuccessful reconciliation remains blocking in durable RUNNING/CLOSING records.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.adapters.composition_mssql_attempts import (
    ConnectionFactory,
    composition_control_transaction,
    read_attempt,
)
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_gate_proofs import gate_proof, observe_quiescence, persist_gate_proof
from dpone.adapters.composition_mssql_issuance import (
    MssqlIssuedCredentials,
    create_login,
    new_credentials,
    require_enrollments,
    require_gate_policy,
    require_login,
    require_worker_users,
)
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptProof,
)

if TYPE_CHECKING:
    from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
    from dpone.adapters.composition_mssql_transfer_access import MssqlCompositionTransferAccess


@dataclass(frozen=True, slots=True)
class _Gate:
    sid: bytes
    name: str
    state: str
    disabled_evidence: str | None


def _read_gate(
    ledger: CompositionMssqlLedger,
    attempt: CompositionAttemptIdentity,
    control_database: str,
) -> _Gate:
    transaction = ledger.require_transaction()
    read_attempt(ledger, attempt)
    ledger.require_transaction(transaction)
    require_gate_policy(ledger, control_database)
    ledger.require_transaction(transaction)
    ledger.cursor.execute(
        f"SELECT TOP (2) login_sid, login_name, gate_state, disabled_evidence_sha256 FROM {ledger.table('login_gates')} "
        "WITH (UPDLOCK, HOLDLOCK) WHERE operation_key=? AND operation_family='execution';",
        attempt.attempt_sha256,
    )
    records = tuple(tuple(value) for value in ledger.cursor.fetchall())
    ledger.require_transaction(transaction)
    if len(records) != 1 or len(records[0]) != 4:
        raise CompositionAdmissionError("login_gate_missing")
    gate = _Gate(*records[0])
    if (
        type(gate.sid) is not bytes
        or len(gate.sid) != 16
        or gate.name != "dpone_v3_" + attempt.attempt_sha256[7:]
        or gate.state not in {"JOURNALED", "READY", "CLOSING", "CLOSED"}
    ):
        raise CompositionAdmissionError("login_gate_identity")
    ledger.cursor.execute(
        "SELECT TOP (2) principal_id FROM " + ledger.table("issued_authorities") + " WITH (HOLDLOCK) "
        "WHERE operation_key=? AND connector='mssql' AND service_id=?;",
        attempt.attempt_sha256,
        ledger.expected_service_id,
    )
    if tuple(tuple(value) for value in ledger.cursor.fetchall()) != (("mssql-sid:" + gate.sid.hex(),),):
        raise CompositionAdmissionError("login_issued_identity")
    ledger.require_transaction(transaction)
    return gate


class MssqlCompositionLoginGate:
    """Issue once; close reconnection before independent server quiescence proof."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        control_database: str,
        control_schema: str = "dpone_control",
        transfer_access: MssqlCompositionTransferAccess | None = None,
    ) -> None:
        if str(UUID(expected_service_id)) != expected_service_id:
            raise CompositionAdmissionError("control_service")
        self._transfer_access = transfer_access
        self._factory = connection_factory
        self._service_id = expected_service_id
        self._database = require_control_schema(control_database)
        self._schema = require_control_schema(control_schema)

    def _transaction(self):
        return composition_control_transaction(self._factory, self._schema, self._service_id)

    def issue_once(self, attempt: CompositionAttemptIdentity) -> MssqlIssuedCredentials:
        """Return credentials only to the invocation that journaled and created.

        A JOURNALED replay cannot finish another invocation's provisioning. Even
        absent/disabled logins do not authorize password reset, enable or reissue.
        Unknown results initiate best-effort closure and preserve durable claims.
        """
        attempt.__post_init__()
        credentials = new_credentials(attempt)
        journal_started = False
        try:
            with self._transaction() as ledger:
                self._require_running(ledger, attempt)
                require_gate_policy(ledger, self._database)
                require_enrollments(ledger, attempt, self._service_id)
                ledger.cursor.execute(
                    f"SELECT TOP (2) operation_key FROM {ledger.table('login_gates')} WITH (UPDLOCK, HOLDLOCK) "
                    "WHERE operation_key=?;",
                    attempt.attempt_sha256,
                )
                if row(ledger.cursor) is not None:
                    raise CompositionAdmissionError("login_issuance_replay")
                journal_started = True
                ledger.cursor.execute(
                    f"INSERT INTO {ledger.table('login_gates')} (operation_key, operation_family, login_sid, login_name, gate_state) "
                    "VALUES (?, 'execution', ?, ?, 'JOURNALED');",
                    attempt.attempt_sha256,
                    credentials.login_sid,
                    credentials.login_name,
                )
                ledger.cursor.execute(
                    f"INSERT INTO {ledger.table('issued_authorities')} (operation_key, connector, service_id, principal_id) "
                    "VALUES (?, 'mssql', ?, ?);",
                    attempt.attempt_sha256,
                    self._service_id,
                    "mssql-sid:" + credentials.login_sid.hex(),
                )
            with self._transaction() as ledger:
                self._require_running(ledger, attempt)
                self._require_gate(ledger, attempt, credentials, "JOURNALED")
                enrollments = require_enrollments(ledger, attempt, self._service_id)
                create_login(ledger, credentials, enrollments)
                if self._transfer_access is not None:
                    self._transfer_access.grant(ledger, credentials)
                require_worker_users(ledger, credentials, enrollments)
                self._transition(ledger, attempt, "JOURNALED", "READY")
            with self._transaction() as ledger:
                self._require_running(ledger, attempt)
                self._require_gate(ledger, attempt, credentials, "READY")
                enrollments = require_enrollments(ledger, attempt, self._service_id)
                require_worker_users(ledger, credentials, enrollments)
                if self._transfer_access is not None:
                    self._transfer_access.require(ledger, credentials)
                require_login(ledger, credentials.login_name, credentials.login_sid, disabled=False)
            return credentials
        except Exception:
            if journal_started:
                try:
                    self.close(attempt)
                except Exception:
                    pass
            raise

    @staticmethod
    def _require_running(ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity) -> None:
        occurrence, receipt = require_existing_execution_in(
            ledger,
            attempt,
            expected_service_id=ledger.expected_service_id,
            terminal_validator=ledger.terminal_validator,
        )
        occurrence.require_state("ACTIVE")
        if receipt.state != "RUNNING":
            raise CompositionAdmissionError("login_attempt_state")

    def _require_gate(
        self,
        ledger: CompositionMssqlLedger,
        attempt: CompositionAttemptIdentity,
        credentials: MssqlIssuedCredentials,
        state: str,
    ) -> None:
        gate = _read_gate(ledger, attempt, self._database)
        if (gate.sid, gate.name, gate.state) != (credentials.login_sid, credentials.login_name, state):
            raise CompositionAdmissionError("login_gate_readback")

    @staticmethod
    def _transition(
        ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity, previous: str, following: str
    ) -> None:
        ledger.cursor.execute(
            "DECLARE @transition TABLE (gate_state varchar(16)); "
            f"UPDATE {ledger.table('login_gates')} SET gate_state=? OUTPUT inserted.gate_state INTO @transition "
            "WHERE operation_key=? AND gate_state=?; SELECT gate_state FROM @transition;",
            following,
            attempt.attempt_sha256,
            previous,
        )
        if row(ledger.cursor) != (following,):
            raise CompositionAdmissionError("login_gate_transition")

    def close(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """Commit CLOSING before disabling, then independently verify exact SID.

        Missing or recreated principal is ambiguous and remains CLOSING. Closing
        a gate does not kill existing sessions or establish business-data outcome.
        """
        attempt.__post_init__()
        with self._transaction() as ledger:
            gate = _read_gate(ledger, attempt, self._database)
            if gate.state in {"JOURNALED", "READY"}:
                self._transition(ledger, attempt, gate.state, "CLOSING")
        with self._transaction() as ledger:
            gate = _read_gate(ledger, attempt, self._database)
            if gate.state not in {"CLOSING", "CLOSED"}:
                raise CompositionAdmissionError("login_gate_close_readback")
            ledger.cursor.execute(
                "DECLARE @name sysname=?, @sid binary(16)=?; "
                "IF (SELECT COUNT(*) FROM sys.server_principals WHERE (name=@name OR sid=@sid))<>1 "
                "OR NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name=@name AND sid=@sid AND type='S') "
                "THROW 51000, 'DPONE_COMPOSITION_LOGIN_IDENTITY', 1; "
                "DECLARE @sql nvarchar(max)=N'ALTER LOGIN '+QUOTENAME(@name)+N' DISABLE'; EXEC sys.sp_executesql @sql;",
                gate.name,
                gate.sid,
            )
        with self._transaction() as ledger:
            gate = _read_gate(ledger, attempt, self._database)
            if gate.state not in {"CLOSING", "CLOSED"}:
                raise CompositionAdmissionError("login_gate_close_readback")
            require_login(ledger, gate.name, gate.sid, disabled=True)
            evidence = self._evidence(attempt, gate, "CLOSED_GATES")
            proof = gate_proof(
                attempt, service_id=self._service_id, sid=gate.sid, kind="CLOSED_GATES", evidence=evidence
            )
            ledger.cursor.execute(
                "DECLARE @transition TABLE (gate_state varchar(16)); "
                f"UPDATE {ledger.table('login_gates')} SET gate_state='CLOSED', disabled_evidence_sha256=? "
                "OUTPUT inserted.gate_state INTO @transition WHERE operation_key=? AND gate_state IN ('CLOSING','CLOSED') "
                "AND (disabled_evidence_sha256 IS NULL OR disabled_evidence_sha256=?); SELECT gate_state FROM @transition;",
                proof.evidence_sha256,
                attempt.attempt_sha256,
                proof.evidence_sha256,
            )
            if row(ledger.cursor) != ("CLOSED",):
                raise CompositionAdmissionError("login_gate_closed_transition")
            persist_gate_proof(ledger, proof, evidence)
        with self._transaction() as ledger:
            observed = _read_gate(ledger, attempt, self._database)
            if observed.state != "CLOSED" or observed.disabled_evidence != proof.evidence_sha256:
                raise CompositionAdmissionError("login_gate_closed_readback")
            require_login(ledger, observed.name, observed.sid, disabled=True)
        return proof

    def prove_quiescence(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """Produce SQL-only proof after the durable authentication barrier."""
        attempt.__post_init__()
        with self._transaction() as ledger:
            gate = _read_gate(ledger, attempt, self._database)
            if gate.state != "CLOSED" or gate.disabled_evidence is None:
                raise CompositionAdmissionError("login_gate_not_closed")
            require_login(ledger, gate.name, gate.sid, disabled=True)
            enrollments = require_enrollments(ledger, attempt, self._service_id)
            observe_quiescence(ledger, gate.sid, enrollments)
            evidence = self._evidence(attempt, gate, "QUIESCENCE")
            evidence["database_ids"] = [enrollment.database_id for enrollment in enrollments]
            evidence["sessions"] = evidence["transactions"] = 0
            proof = gate_proof(attempt, service_id=self._service_id, sid=gate.sid, kind="QUIESCENCE", evidence=evidence)
            persist_gate_proof(ledger, proof, evidence)
        return proof

    def _evidence(self, attempt: CompositionAttemptIdentity, gate: _Gate, kind: str) -> dict[str, object]:
        return {
            "schema": "dpone.composition-mssql-gate-observation.v1",
            "attempt_sha256": attempt.attempt_sha256,
            "service_id": self._service_id,
            "login_sid": gate.sid.hex(),
            "login_name": gate.name,
            "kind": kind,
            "login_disabled": True,
            "authentication_barrier": "CLOSING",
        }
