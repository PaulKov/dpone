"""Protected SQL Server persistence for complete composition occurrences.

The injected connection factory must open independent connections to one trusted
control database. A transaction-owned global lock linearizes ledger operations;
it is never a writer fence. External provisioning supplies authority/enrollment.
Every attempted mutation commit closes its connection and independently reads
the exact result. Unknown readback retains reservations and requires recovery;
the adapter never replays a mutation to infer whether a commit succeeded.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import close, rollback
from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionActivationRequest,
    CompositionAdmissionError,
    encode_activation_request,
)

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlCompositionActivationStore:
    """Implement the parent occurrence port with non-expiring epoch ownership.

    ``expected_service_id`` is a canonical UUID from trusted protected binding
    configuration, never an identity inferred from an endpoint or SQL principal.
    No constructor or lifecycle operation provisions the schema or enrollment.
    Explicit predecessor retirement must finish before another request can reserve
    its domains. A PREPARED retry is valid only for exactly the same request.
    """

    def __init__(
        self,
        connection_factory: Callable[[], SqlControlConnection],
        *,
        expected_service_id: str,
        control_schema: str = "dpone_control",
    ) -> None:
        self._schema = require_control_schema(control_schema)
        try:
            valid = str(UUID(expected_service_id)) == expected_service_id
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("control_service_id")
        self._factory = connection_factory
        self._service_id = expected_service_id

    def read(self, activation_id: str) -> CompositionActivationOccurrence | None:
        """Read complete protected state on a new transaction, without committing."""
        try:
            valid = str(UUID(activation_id)) == activation_id and UUID(activation_id).version == 4
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("activation_id")
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._factory()
            connection.autocommit = False
            cursor = connection.cursor()
            ledger = CompositionMssqlLedger(cursor, self._schema)
            transaction = ledger.begin(self._service_id)
            occurrence = ledger.read(activation_id)
            ledger.require_transaction(transaction)
            return occurrence
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("durable_readback") from None
        finally:
            rollback(connection)
            close(cursor)
            close(connection)

    def prepare(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence:
        """Reserve every enrolled SQL Server and ClickHouse domain atomically."""
        return self._mutate(request, "PREPARED")

    def activate(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence:
        """Acknowledge ACTIVE only for the exact prepared occurrence and epochs."""
        return self._mutate(request, "ACTIVE")

    def begin_retirement(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence:
        """Close new admission while preserving all existing ownership and attempts."""
        return self._mutate(request, "RETIRING")

    def finalize_retirement(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence:
        """Release exact epochs only after durable, independently verified terminals."""
        return self._mutate(request, "RETIRED")

    def _mutate(self, request: CompositionActivationRequest, state: str) -> CompositionActivationOccurrence:
        encode_activation_request(request)
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        commit_started = False
        expected = None
        try:
            connection = self._factory()
            connection.autocommit = False
            cursor = connection.cursor()
            ledger = CompositionMssqlLedger(cursor, self._schema)
            transaction = ledger.begin(self._service_id)
            current = ledger.read(request.activation_id)
            self._apply(ledger, request, current, state)
            expected = ledger.read(request.activation_id)
            if expected is None or expected.request != request:
                raise CompositionAdmissionError("occurrence_readback")
            expected.require_state(state)
            if current is not None and expected.receipt.guard_epochs != current.receipt.guard_epochs:
                raise CompositionAdmissionError("guard_readback")
            ledger.require_transaction(transaction)
            commit_started = True
            connection.commit()
        except CompositionAdmissionError:
            if not commit_started:
                rollback(connection)
                raise
        except Exception:
            if not commit_started:
                rollback(connection)
                raise CompositionAdmissionError("durable_mutation") from None
        finally:
            close(cursor)
            close(connection)
        try:
            observed = self.read(request.activation_id)
            if expected is None or observed != expected:
                raise CompositionAdmissionError("occurrence_readback")
            return observed
        except Exception:
            raise CompositionAdmissionError("commit_unknown") from None

    @staticmethod
    def _apply(
        ledger: CompositionMssqlLedger,
        request: CompositionActivationRequest,
        current: CompositionActivationOccurrence | None,
        state: str,
    ) -> None:
        if current is None:
            if state != "PREPARED":
                raise CompositionAdmissionError("occurrence_missing")
            ledger.prepare(request)
            return
        if encode_activation_request(current.request) != encode_activation_request(request):
            raise CompositionAdmissionError("occurrence_mismatch")
        if current.receipt.state == state:
            return
        before = {"ACTIVE": "PREPARED", "RETIRING": "ACTIVE", "RETIRED": "RETIRING"}.get(state)
        if current.receipt.state != before:
            raise CompositionAdmissionError("occurrence_state")
        if state == "RETIRED":
            ledger.require_terminal(current)
            ledger.release(current)
        ledger.transition(current, state)
