"""Reopen retained MSSQL closure originals without observing successor activity.

The current irreversible gate and exact disabled SID remain required. Historical
quiescence is its immutable producer observation plus immutable enrollment; a
new business transaction must not invalidate an older closed writer. The owning
boundary and injected gate-policy dependency retain their exact catalog audits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.adapters.composition_mssql_issuance import require_gate_policy
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
)
from dpone.contracts.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from dpone.ports.composition_sql import CompositionSqlContext


def _table(context: CompositionSqlContext, name: str) -> str:
    """Check the transport's namespace before it also reaches the policy callback."""
    try:
        canonical = f"[{require_control_schema(context.schema)}].[composition_{name}]"
        if context.table(name) == canonical:
            return canonical
    except (ValueError, TypeError, AttributeError):
        pass
    raise CompositionAdmissionError("control_schema")


def _rows_in(
    context: CompositionSqlContext,
    sql: str,
    *parameters: object,
    expected_service_id: str,
    transaction_id: int,
) -> tuple[tuple[Any, ...], ...]:
    """Detach one bounded observation, checking the same transaction on both sides."""
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction_id)
    context.cursor.execute(sql, *parameters)
    rows = tuple(tuple(value) for value in context.cursor.fetchall())
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction_id)
    return rows


def _require_attempt(attempt: CompositionAttemptIdentity) -> None:
    if type(attempt) is not CompositionAttemptIdentity:
        raise CompositionAdmissionError("terminal_attempt_identity")
    attempt.__post_init__()
    if len(attempt.guard_epochs) > 8192 or any(epoch >= 2**63 for _, epoch in attempt.guard_epochs):
        raise CompositionAdmissionError("attempt_guard_epochs")


def _database_ids(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    *,
    expected_service_id: str,
    transaction_id: int,
) -> list[int]:
    result: list[int] = []
    for guard, _ in attempt.guard_epochs:
        rows = _rows_in(
            context,
            "SELECT TOP (2) d.connector,LOWER(CONVERT(char(36),d.service_id)),e.database_name,e.database_id,"
            "LOWER(CONVERT(char(36),e.database_guid)),e.database_create_token,e.writer_role "
            f"FROM {_table(context, 'domains')} d WITH (HOLDLOCK) "
            f"LEFT JOIN {_table(context, 'mssql_enrollments')} e WITH (HOLDLOCK) ON e.guard_id=d.guard_id WHERE d.guard_id=?;",
            guard,
            expected_service_id=expected_service_id,
            transaction_id=transaction_id,
        )
        if len(rows) != 1 or len(rows[0]) != 7 or rows[0][:2] != ("mssql", expected_service_id):
            raise CompositionAdmissionError("terminal_mssql_enrollment")
        _, _, database, database_id, guid, token, role = rows[0]
        try:
            require_control_schema(database)
            require_control_schema(role)
            valid_guid = type(guid) is str and str(UUID(guid)) == guid
        except (ValueError, TypeError, AttributeError):
            raise CompositionAdmissionError("terminal_mssql_enrollment") from None
        if (
            type(database_id) is not int
            or not 4 < database_id < 2**31
            or not valid_guid
            or type(token) is not str
            or not 1 <= len(token) <= 33
        ):
            raise CompositionAdmissionError("terminal_mssql_enrollment")
        schemas = _rows_in(
            context,
            f"SELECT TOP (8193) schema_name FROM {_table(context, 'mssql_managed_schemas')} WITH (HOLDLOCK) "
            "WHERE guard_id=? ORDER BY schema_name COLLATE Latin1_General_100_BIN2;",
            guard,
            expected_service_id=expected_service_id,
            transaction_id=transaction_id,
        )
        if not 1 <= len(schemas) <= 8192 or any(len(value) != 1 or type(value[0]) is not str for value in schemas):
            raise CompositionAdmissionError("terminal_mssql_enrollment")
        if schemas != tuple(sorted(set(schemas))):
            raise CompositionAdmissionError("terminal_mssql_enrollment")
        for value in schemas:
            try:
                require_control_schema(value[0])
            except ValueError:
                raise CompositionAdmissionError("terminal_mssql_enrollment") from None
        result.append(database_id)
    if len(set(result)) != len(result):
        raise CompositionAdmissionError("terminal_mssql_enrollment")
    return result


def _evidence(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    proof: CompositionAttemptProof,
    *,
    expected_service_id: str,
    transaction_id: int,
    sid: bytes,
    name: str,
    database_ids: list[int],
) -> None:
    expected: dict[str, object] = {
        "schema": "dpone.composition-mssql-gate-observation.v1",
        "attempt_sha256": attempt.attempt_sha256,
        "service_id": expected_service_id,
        "login_sid": sid.hex(),
        "login_name": name,
        "kind": proof.kind,
        "login_disabled": True,
        "authentication_barrier": "CLOSING",
    }
    if proof.kind == "QUIESCENCE":
        expected.update(database_ids=database_ids, sessions=0, transactions=0)
    document = canonical_json_bytes(expected)
    rows = _rows_in(
        context,
        "SELECT TOP (2) CASE WHEN DATALENGTH(evidence_document) BETWEEN 1 AND 8388608 THEN evidence_document END "
        f"FROM {_table(context, 'mssql_gate_evidence')} WITH (HOLDLOCK) WHERE operation_key=? AND evidence_sha256=?;",
        attempt.attempt_sha256,
        proof.evidence_sha256,
        expected_service_id=expected_service_id,
        transaction_id=transaction_id,
    )
    if (
        rows != ((document,),)
        or type(rows[0][0]) is not bytes
        or canonical_fingerprint(expected) != proof.evidence_sha256
    ):
        raise CompositionAdmissionError("terminal_mssql_evidence")


def require_historical_mssql_gate(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    issued: tuple[CompositionProofAuthority, ...],
    closed: CompositionAttemptProof,
    quiescent: CompositionAttemptProof,
    *,
    expected_service_id: str,
) -> None:
    """Require the current complete gate policy and retained original closure bytes.

    The existing single-MSSQL producer cannot certify aggregate mixed issuance.
    No new quiescence, business-database policy or worker observation occurs.
    Immutable enrollment rows retain the original attempt's ordered database IDs.
    """
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    _require_attempt(attempt)
    if type(issued) is not tuple or len(issued) != 1 or type(issued[0]) is not CompositionProofAuthority:
        raise CompositionAdmissionError("terminal_mssql_authorities")
    issued[0].__post_init__()
    if issued[0].connector != "mssql" or issued[0].service_id != expected_service_id:
        raise CompositionAdmissionError("terminal_mssql_authorities")
    if type(closed) is not CompositionAttemptProof or type(quiescent) is not CompositionAttemptProof:
        raise CompositionAdmissionError("login_gate_identity")
    for proof in (closed, quiescent):
        proof.require_attempt(attempt)
        if proof.authorities != issued:
            raise CompositionAdmissionError("terminal_mssql_authorities")
    if closed.kind != "CLOSED_GATES" or quiescent.kind != "QUIESCENCE":
        raise CompositionAdmissionError("login_gate_identity")
    gate_table = _table(context, "login_gates")
    databases = _rows_in(
        context, "SELECT DB_NAME();", expected_service_id=expected_service_id, transaction_id=transaction
    )
    if len(databases) != 1 or len(databases[0]) != 1 or type(databases[0][0]) is not str:
        raise CompositionAdmissionError("login_gate_policy")
    try:
        require_control_schema(databases[0][0])
    except ValueError:
        raise CompositionAdmissionError("login_gate_policy") from None
    require_gate_policy(context, databases[0][0])
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    rows = _rows_in(
        context,
        "SELECT TOP (2) operation_family,login_sid,login_name,gate_state,disabled_evidence_sha256 "
        f"FROM {gate_table} WITH (HOLDLOCK) WHERE operation_key=?;",
        attempt.attempt_sha256,
        expected_service_id=expected_service_id,
        transaction_id=transaction,
    )
    if len(rows) != 1 or len(rows[0]) != 5:
        raise CompositionAdmissionError("login_gate_missing")
    family, sid, name, state, disabled = rows[0]
    if (
        family != "execution"
        or type(sid) is not bytes
        or len(sid) != 16
        or type(name) is not str
        or name != "dpone_v3_" + attempt.attempt_sha256[7:]
        or state != "CLOSED"
        or issued[0].principal_id != "mssql-sid:" + sid.hex()
        or disabled != closed.evidence_sha256
    ):
        raise CompositionAdmissionError("login_gate_identity")
    # A bounded read preserves the existing exact login/privilege projection.
    login = _rows_in(
        context,
        "SELECT TOP (2) p.name,p.sid,p.type,p.is_disabled,"
        "(SELECT COUNT(*) FROM sys.server_role_members r WHERE r.member_principal_id=p.principal_id),"
        "(SELECT COUNT(*) FROM sys.server_permissions x WHERE x.grantee_principal_id=p.principal_id "
        "AND NOT (x.permission_name='CONNECT SQL' AND x.state='G')) "
        "FROM sys.server_principals p WHERE p.name=? OR p.sid=?;",
        name,
        sid,
        expected_service_id=expected_service_id,
        transaction_id=transaction,
    )
    if login != ((name, sid, "S", True, 0, 0),) or type(login[0][1]) is not bytes:
        raise CompositionAdmissionError("login_principal_readback")
    ids = _database_ids(context, attempt, expected_service_id=expected_service_id, transaction_id=transaction)
    for proof in (closed, quiescent):
        _evidence(
            context,
            attempt,
            proof,
            expected_service_id=expected_service_id,
            transaction_id=transaction,
            sid=sid,
            name=name,
            database_ids=ids,
        )
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
