"""Injected SQL-control connections with pyodbc-style parameter binding.

This is the small control-ledger capability used by native and composition
adapters. It does not promise interchangeable parameter styles for every PEP 249
driver, or acquire transaction, writer-session or credential authority itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class SqlControlCursor(Protocol):
    """Execute bound control statements and detach their observed rows."""

    def execute(self, sql: str, *parameters: object) -> SqlControlCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class SqlControlConnection(Protocol):
    """An independently created connection whose transaction the adapter owns."""

    autocommit: bool

    def cursor(self) -> SqlControlCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...
