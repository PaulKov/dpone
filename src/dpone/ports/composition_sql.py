"""Injected SQL context and execution-only proof verification for shared control.

These interfaces carry no transaction, enrollment, writer or lifecycle authority.
Protected readers must independently observe the current database transaction.
"""

from collections.abc import Callable
from typing import Protocol

from dpone.contracts.composition_activation import CompositionActivationOccurrence
from dpone.contracts.composition_attempt import CompositionAttemptReceipt
from dpone.ports.sql_connection import SqlControlCursor


class CompositionSqlContext(Protocol):
    """One caller-owned cursor and canonical control-schema namespace."""

    cursor: SqlControlCursor
    schema: str

    def table(self, name: str) -> str: ...


ExecutionTerminalValidator = Callable[
    [CompositionSqlContext, CompositionActivationOccurrence, CompositionAttemptReceipt], None
]
