"""Occurrence-owned materialization openers for the supervised native dbt cell.

These seams are supplied by the activation root. Pack-exec must not invent a
second SQL opener or accept a caller-supplied closure document.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.adapters.composition_dbt_capture_schema import require_dbt_capture_schema
from dpone.adapters.composition_mssql_enrollment import mssql_target_pin
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_dbt_capture_codec import decode_registration
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory

RequireTargetService = Callable[..., None]


def build_composition_materialization_seams(
    *,
    target: Any | None = None,
    expected_service_id: str | None = None,
    context: Any | None = None,
    require_target_service: RequireTargetService | None = None,
) -> Mapping[str, Any]:
    """Return production openers. Invoking them without a target fail-closes."""

    def open_target(attempt: Any, write: Any) -> tuple[Any, Any]:
        del attempt
        if target is None:
            raise CompositionAdmissionError("materialization_target")
        pin = mssql_target_pin(target, write)
        connection = ResolvedConnectorFactory.create(target, autocommit=False).connection
        return connection, pin

    def require_target(connection: Any, attempt: Any, write: Any, pin: Any) -> None:
        del attempt
        if connection is None or not callable(getattr(connection, "cursor", None)):
            raise CompositionAdmissionError("materialization_target")
        if target is None:
            raise CompositionAdmissionError("materialization_target")
        if mssql_target_pin(target, write) != pin:
            raise CompositionAdmissionError("materialization_database_pin")
        if require_target_service is None or expected_service_id is None or context is None:
            raise CompositionAdmissionError("materialization_target")
        try:
            require_target_service(target, expected_service_id, context)
        except CompositionAdmissionError as exc:
            if exc.reason in {"materialization_target", "target_service_pin"}:
                raise
            raise CompositionAdmissionError("target_service_pin") from None

    return {
        "open_target": open_target,
        "require_target": require_target,
        "observe_undispatched_closure": observe_undispatched_closure,
    }


def bind_composition_materialization_seams(
    *,
    require_target_service: RequireTargetService | None,
    target: Any | None = None,
    expected_service_id: str | None = None,
    context: Any | None = None,
) -> Mapping[str, Any]:
    """Return activation-owned openers that cannot omit target-service authority."""

    if require_target_service is None:
        raise CompositionAdmissionError("materialization_target")
    return build_composition_materialization_seams(
        target=target,
        expected_service_id=expected_service_id,
        context=context,
        require_target_service=require_target_service,
    )


def observe_undispatched_closure(ledger: Any, attempt: Any) -> bytes:
    """Reopen the registered intent under the caller's protected transaction."""

    try:
        transaction = ledger.require_transaction()
        require_dbt_capture_schema(ledger.cursor, ledger.schema)
        ledger.cursor.execute(
            f"SELECT TOP (2) intent_sha256,login_sid,document_sha256,document "
            f"FROM {ledger.table('dbt_registrations')} WITH (HOLDLOCK) WHERE operation_key=?;",
            attempt.attempt_sha256,
        )
        rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if len(rows) != 1 or len(rows[0]) != 4:
            raise DbtCaptureError("capture_registration_missing")
        intent, _expectation = decode_registration(rows[0][3], rows[0][2])
        if intent.attempt != attempt or intent.intent_sha256 != rows[0][0]:
            raise DbtCaptureError("capture_registration_identity")
        ledger.cursor.execute(
            f"SELECT TOP (5) phase FROM {ledger.table('dbt_events')} WITH (HOLDLOCK) WHERE operation_key=?;",
            attempt.attempt_sha256,
        )
        phases = {row[0] for row in ledger.cursor.fetchall()}
        if phases - {"UNDISPATCHED"}:
            raise DbtCaptureError("capture_undispatched_conflict")
        ledger.require_transaction(transaction)
        return canonical_json_bytes(
            {
                "attempt_sha256": attempt.attempt_sha256,
                "intent_sha256": intent.intent_sha256,
                "build_dispatched": False,
                "closed": True,
            }
        )
    except DbtCaptureError:
        raise
    except Exception:
        raise DbtCaptureError("capture_undispatched_closure") from None


__all__ = [
    "bind_composition_materialization_seams",
    "build_composition_materialization_seams",
    "observe_undispatched_closure",
]
