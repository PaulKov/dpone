"""Injected SQL and transaction authority for the isolated SWITCH component."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from dpone.contracts.native_mssql_switch import NativeSwitchBinding, NativeSwitchPlan


class NativeSwitchSql(Protocol):
    """Same-session SQL access, with driver-bound ``?`` parameters.

    Implementations must not reconnect, retry, autocommit, or log parameter
    values. Query errors propagate; missing/unknown metadata is never a default.
    """

    def query(self, sql: str, parameters: tuple[object, ...] = ()) -> Sequence[Mapping[str, object]]:
        """Return named rows from the current database/session."""
        ...


class NativeSwitchTransaction(NativeSwitchSql, Protocol):
    """Capability supplied only by the caller's existing target finalizer.

    All methods use one caller-owned active transaction. The caller resolves an
    exact receipt before granting authority and holds its target/operation fence
    through commit. An exception after mutation requires complete caller rollback.
    The caller owns receipt insertion, commit ambiguity, retention and cleanup.
    """

    def assert_authority(self, owner_binding: NativeSwitchBinding) -> None:
        """Raise unless invocation, current generation, mutation and fence match.

        Also reject an unresolved/already committed operation. The approved
        environment must freeze database/server DDL-trigger configuration through
        transaction completion; table locks do not protect global configuration. Never begin a
        transaction, acquire authority by guessing, or return a Boolean hint.
        """
        ...

    def verify_prepared(self, plan: NativeSwitchPlan) -> None:
        """Verify complete typed content against durable authority under held locks.

        Called after executor locks all three tables. Bind verification to the
        plan's invocation/generation/mutation and authored interval; verify both
        business and framework metadata (including authoritative loaded_at).
        Raise on any difference or unavailable evidence. Do not mutate, release
        locks, reconnect, change transaction state, or return a Boolean hint.
        This required hook delegates to the existing caller integrity service.
        """
        ...

    def execute(self, sql: str) -> None:
        """Execute once, inside the same transaction; propagate every error."""
        ...
