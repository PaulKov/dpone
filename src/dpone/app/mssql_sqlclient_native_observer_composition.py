"""Durable, context-bounded observers for the SqlClient native route."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.mssql_tds_api import TdsAttemptIdentity, TdsAttemptSnapshot
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits, TdsDirectorySnapshot
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver


class _DurableSqlClientAttemptObserver:
    def __init__(self, admitted_factory: _AdmittedSqlClientStoreFactory) -> None:
        self._admitted_factory = admitted_factory

    def read(self, identity: TdsAttemptIdentity) -> TdsAttemptSnapshot | None:
        with self._admitted_factory() as store:
            return TdsAttemptJournal(store, backend="mssql_sqlclient").read(identity)


class _DurableSqlClientDirectoryObserver:
    def __init__(self, admitted_factory: _AdmittedSqlClientStoreFactory) -> None:
        self._admitted_factory = admitted_factory

    def read(self, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits) -> TdsDirectorySnapshot | None:
        with self._admitted_factory() as store:
            lifecycle = TdsAttemptJournal(store, backend="mssql_sqlclient")
            directory = TdsCoordinatorDirectoryJournal(store, parent_observer=lifecycle)
            return directory.read(parent, limits)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeObserverBundle:
    """Store-backed observers sharing one admitted state-domain identity."""

    lifecycle: TdsAttemptObserver
    directory: TdsDirectoryObserver
    admitted_factory: _AdmittedSqlClientStoreFactory


def compose_sqlclient_native_observers(
    admitted_factory: _AdmittedSqlClientStoreFactory,
) -> SqlClientNativeObserverBundle:
    """Compose restart-safe observers without opening the durable store."""
    if type(admitted_factory) is not _AdmittedSqlClientStoreFactory:
        raise ValueError("mssql_native.sqlclient_observer_composition_invalid")
    return SqlClientNativeObserverBundle(
        lifecycle=_DurableSqlClientAttemptObserver(admitted_factory),
        directory=_DurableSqlClientDirectoryObserver(admitted_factory),
        admitted_factory=admitted_factory,
    )


__all__ = (
    "SqlClientNativeObserverBundle",
    "compose_sqlclient_native_observers",
)
