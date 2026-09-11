"""Private invocation continuity for caller-owned registration transactions.

Observed IDs and revisions are comparisons, never cached authority. Every SQL
observation retains the caller's actual transaction; each write and successful
exit also compares the latest complete trust revision. This object never owns
connection, lock acquisition, commit, rollback or durable acknowledgement.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from dpone.adapters.nonproduction_mssql_registration_schema import require_nonproduction_registration_schema
from dpone.contracts.nonproduction_document import NonproductionAuthorityError

if TYPE_CHECKING:
    from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
    from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider, NonproductionTrustRevision


class _RegistrationBoundary:
    """One invocation's independently pinned ledger, revision and clock checks."""

    def __init__(
        self,
        trust: MssqlNonproductionTrustProvider,
        ledger: CompositionMssqlLedger,
        expected: NonproductionTrustRevision,
        clock: Callable[[], datetime],
    ) -> None:
        self.ledger, self._trust, self._expected, self._clock = ledger, trust, expected, clock

    def __enter__(self) -> _RegistrationBoundary:
        self._transaction = self._trust._require_ledger(self.ledger)
        self.current = self._trust.require_revision_in(self.ledger, self._expected)
        self.check_transaction()
        require_nonproduction_registration_schema(self.ledger.cursor, self.ledger.schema)
        self.check_current()
        self.started_at = self.now()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> Literal[False]:
        if exc_type is None:
            self.check_current()
        return False

    def check_transaction(self) -> None:
        """Use the provider's external service/schema pins, never ledger setup state."""
        self._trust._require_ledger(self.ledger, self._transaction)

    def check_current(self) -> None:
        """Bracket latest full revision comparison with the original transaction."""
        self.check_transaction()
        self._trust.require_revision_in(self.ledger, self.current)
        self.check_transaction()

    def now(self) -> datetime:
        """Check injected clock effects without masking a boundary failure as time."""
        self.check_transaction()
        try:
            now = self._clock()
            if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
                raise ValueError
            instant = now.astimezone(timezone.utc)  # noqa: UP017
        except Exception:
            raise NonproductionAuthorityError("registration_clock") from None
        self.check_current()
        return instant

    def rows(self, sql: str, *parameters: object) -> tuple[tuple[Any, ...], ...]:
        """Detach one bounded projection before nested observations reuse the cursor."""
        self.check_transaction()
        self.ledger.cursor.execute(sql, *parameters)
        rows = tuple(tuple(row) for row in self.ledger.cursor.fetchall())
        self.check_transaction()
        return rows

    def write(self, sql: str, *parameters: object) -> None:
        """Reject boundary changes before every append and stop on changed readback."""
        self.check_current()
        self.ledger.cursor.execute(sql, *parameters)
        self.check_transaction()
