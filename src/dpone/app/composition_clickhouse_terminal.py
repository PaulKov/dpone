"""Whole-cell worker closure without changing singleton publication gates."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.adapters.composition_clickhouse_terminal_store import ClickHouseTerminalStore
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, CompositionAttemptProof


class ClickHouseTerminalWorkerGate:
    """Issue ingest once, close existing purposes, return protected aggregates.

    Use only for the outer CompositionWorker. Capture, dispatch and publication
    keep their existing singleton purpose gates. Missing publisher binding fails
    after ingest cleanup, without creating users or fabricating terminal proofs.
    The same instance supplies the worker's independent outcome observer.
    """

    def __init__(
        self,
        *,
        ingest: Any,
        publisher: Any,
        terminal: ClickHouseTerminalStore,
        begin_cleanup: Callable[[], Any] | None = None,
    ) -> None:
        if begin_cleanup is not None and not callable(begin_cleanup):
            raise ValueError("clickhouse_terminal_cleanup")
        self._ingest, self._publisher, self._terminal = ingest, publisher, terminal
        self._begin_cleanup = begin_cleanup

    def issue_once(self, attempt: CompositionAttemptIdentity) -> Any:
        return self._ingest.issue_once(attempt)

    def close(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        # Only outer worker closure reaches here after business execution has
        # completed or failed. Successful ingest closure keeps its raw gate.
        if self._begin_cleanup is not None:
            self._begin_cleanup()
        self._ingest.close(attempt)
        self._ingest.prove_quiescence(attempt)
        self._terminal.require_issued(attempt)
        self._publisher.close(attempt)
        self._publisher.prove_quiescence(attempt)
        return self._terminal.persist_success(attempt).closed_gates

    def prove_quiescence(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        return self._terminal.read_proofs(attempt).quiescence

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        return self._terminal.read_proofs(attempt).outcome
