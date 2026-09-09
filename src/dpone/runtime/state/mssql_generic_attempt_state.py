"""Monotonic invocation-wide SQL Server target generation allocation."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from dpone.runtime.state.mssql_generic_state_base import MssqlGenericStateBase
from dpone.runtime.state.mssql_generic_transaction_names import ATTEMPT_TABLE, FENCE_TABLE
from dpone.runtime.state.mssql_generic_transaction_rows import attempt_from_rows
from dpone.runtime.state.mssql_generic_transaction_sql import allocation_params, allocation_sql

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlAttemptRequest, MssqlTransactionAttempt


class MssqlAttemptAllocationOutcomeUnknown(RuntimeError):
    """Allocation ACK was lost and no exact attempt could be proved."""

    code = "mssql_transaction.attempt_allocation_outcome_unknown"

    def __init__(self) -> None:
        super().__init__(self.code)


class MssqlGenericAttemptState(MssqlGenericStateBase):
    """Allocate and query one immutable attempt per scheduler invocation."""

    def allocate(self, request: MssqlAttemptRequest) -> MssqlTransactionAttempt:
        """Idempotently allocate one monotonic generation before source I/O."""

        started = False
        commit_attempted = False
        try:
            self.connector.begin()
            started = True
            rows = self.connector.get_records(
                allocation_sql(fence=self.fence_table, attempt=self.attempt_table),
                allocation_params(request),
                as_dict=True,
            )
            attempt = attempt_from_rows(request, rows)
            commit_attempted = True
            self.connector.commit_transaction()
            started = False
            return attempt
        except Exception as exc:
            if commit_attempted:
                self.close(self.connector)
                probed = self.probe_fresh(request)
                if probed is not None:
                    return probed
                raise MssqlAttemptAllocationOutcomeUnknown() from exc
            if started:
                with suppress(Exception):
                    self.connector.rollback()
            raise

    def probe_fresh(self, request: MssqlAttemptRequest) -> MssqlTransactionAttempt | None:
        with self.fresh_sessions.open(self.connector) as session:
            return self.probe(request, connector=session)

    def probe(
        self,
        request: MssqlAttemptRequest,
        *,
        connector: Any | None = None,
    ) -> MssqlTransactionAttempt | None:
        session = connector or self.connector
        rows = session.get_records(
            f"""
            SELECT a.attempt_key, a.target_identity, a.generation, a.invocation_digest,
                   a.route_fingerprint, a.target_database, a.target_schema, a.target_table, a.strategy,
                   CAST(CASE WHEN f.current_attempt_key = a.attempt_key
                                  AND f.current_generation = a.generation
                                  AND f.current_route_fingerprint = a.route_fingerprint
                             THEN 1 ELSE 0 END AS bit) AS is_current_generation
            FROM {self.attempt_table} AS a WITH (HOLDLOCK)
            LEFT JOIN {self.fence_table} AS f WITH (HOLDLOCK)
              ON f.target_identity = a.target_identity
            WHERE a.attempt_key = ?
            """,
            (request.attempt_key,),
            as_dict=True,
        )
        if not rows:
            return None
        return attempt_from_rows(request, rows)

    @property
    def fence_table(self) -> str:
        return self.qualified(FENCE_TABLE)

    @property
    def attempt_table(self) -> str:
        return self.qualified(ATTEMPT_TABLE)


__all__ = ["MssqlAttemptAllocationOutcomeUnknown", "MssqlGenericAttemptState"]
