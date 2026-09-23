"""Custody input and single-owner operation guard for observe helper evidence."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from threading import current_thread
from typing import Any, Protocol

from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity


class HelperCustodyFacts(Protocol):
    """Read-only existing custody observations; no close or authority capability."""

    @property
    def process(self) -> TdsProcessIdentity | None: ...
    @property
    def startup(self) -> TdsCoordinatorStartup | None: ...
    @property
    def local_exit(self) -> TdsChildExit | None: ...
    @property
    def raw_result(self) -> bytes | None: ...
    @property
    def child_closed(self) -> bool: ...
    @property
    def child(self) -> object | None: ...
    @property
    def unresolved_launch(self) -> object | None: ...
    @property
    def helper_evidence(self) -> object | None: ...
    @property
    def helper_evidence_closed(self) -> bool: ...


class ObserveHelperOperationMixin:
    """Guard each capture against re-entry, fork use, and thread transfer."""

    @contextmanager
    def _operation(self: Any) -> Iterator[None]:
        if self._busy or self.failed or os.getpid() != self._pid or current_thread() is not self._thread:
            self._reject()
        self._busy = True
        try:
            yield
        except BaseException:
            self.failed = True
            raise
        finally:
            self._busy = False
