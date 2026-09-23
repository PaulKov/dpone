"""Attempt capability consumed by the two original journal continuations."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from dpone.ports.mssql_tds_directory import TdsDirectoryGateway
from dpone.ports.mssql_tds_journal import TdsJournalGateway
from dpone.services.mssql_tds_writer_contracts import TdsAttemptSnapshot, TdsDirectorySnapshot

if TYPE_CHECKING:
    from dpone.services.mssql_tds_original_continuation import CreateSettlement, PreparationTransition


class AttemptHost(Protocol):
    """Complete finite access surface; operation records never replace the host."""

    _lifecycle: TdsJournalGateway
    _directory: TdsDirectoryGateway
    _busy: bool
    _poisoned: bool
    _create_settlement: CreateSettlement | None
    _preparation: PreparationTransition | None
    _prepared_origin: PreparationTransition | None

    @property
    def _departure_baseline(self) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot, object, float] | None: ...
    def _owned(self) -> None: ...
    def _require_unpoisoned(self) -> None: ...
    def _sequence(self, deadline: float) -> AbstractContextManager[None]: ...
    def _assert_observe(
        self, helper_id: UUID, *, deadline: float
    ) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]: ...
