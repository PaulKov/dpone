"""Bounded worker execution with isolated attempts and fail-closed recovery.

Byte accounting belongs to the injected source/target streaming adapters. This
executor never materializes row collections and caps active workers. Evidence
and state callbacks must durably and idempotently write by plan.run_id. Each
callback MUST hold the external writer guard and atomically validate its lease
and expected prior state at the actual mutation; a preceding assertion is not
a fencing boundary. Across runs, source state uses monotonic/CAS advancement.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Generator, Sequence
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Event, Thread

from dpone.contracts.bounded_window import (
    ChunkReceipt,
    WindowChunk,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowPlan,
    WindowResult,
    WindowTransientError,
    decode_receipt,
    receipt_data,
)
from dpone.ports.bounded_window import WindowProgressJournal, WindowSource, WindowStore, WindowTarget


class BoundedWindowExecutor:
    """Coordinate one target lease, parallel chunk staging and one publication."""

    def __init__(
        self,
        *,
        source: WindowSource,
        target: WindowTarget,
        store: WindowStore,
        journal_factory: Callable[[WindowLease, str], WindowProgressJournal],
        evidence: Callable[[WindowPlan, str, Sequence[ChunkReceipt], WindowLease], None],
        advance_state: Callable[[WindowPlan, WindowLease], None],
        sleeper: Callable[[float], None],
        lease_ttl: float = 60.0,
    ) -> None:
        self.source, self.target, self.store = source, target, store
        self.journal_factory = journal_factory
        self.evidence, self.advance_state, self.sleeper = evidence, advance_state, sleeper
        self.lease_ttl = lease_ttl

    def execute(self, plan: WindowPlan, owner: str) -> WindowResult:
        """Resume durable identity; fail on unknown publication without retrying it."""
        self.target.validate(plan)
        lease = self.store.acquire(plan.target_id, owner, self.lease_ttl)
        try:
            return self.execute_leased(plan, lease)
        finally:
            primary = sys.exc_info()[1]
            try:
                self.store.release(lease)
            except BaseException as error:
                self._cleanup_error(primary, error)

    def execute_leased(self, plan: WindowPlan, lease: WindowLease) -> WindowResult:
        """Execute under a caller-owned lease acquired before source snapshot creation.

        The caller must retain ownership through source-context cleanup and then
        release the lease. This method neither acquires nor releases it. It checks
        current ownership, renews independently during execution, and stops its
        renewal thread before returning. Post-publication recovery never reads or
        validates the source; its immutable plan must come from the caller's
        fenced invocation registry rather than reopening a source snapshot.
        """
        if lease.target_id != plan.target_id:
            raise WindowContractError("Caller lease target does not match window target")
        self.store.assert_lease(lease)
        self.target.validate(plan)
        stopped, lost = Event(), Event()
        heartbeat = Thread(target=self._heartbeat, args=(lease, stopped, lost), daemon=True)
        heartbeat.start()
        try:
            return self._run(plan, lease, lost)
        finally:
            stopped.set()
            primary = sys.exc_info()[1]
            heartbeat.join(timeout=35.0)
            shutdown_error = (
                WindowContractError("Lease renewal thread did not stop within 35 seconds")
                if heartbeat.is_alive()
                else None
            )
            try:
                if shutdown_error is not None:
                    raise shutdown_error
            except BaseException as error:
                self._cleanup_error(primary, error)

    def _heartbeat(self, lease: WindowLease, stopped: Event, lost: Event) -> None:
        while not stopped.wait(self.lease_ttl / 3):
            try:
                self.store.renew(lease, self.lease_ttl)
            except Exception:
                lost.set()
                return

    def _check(self, lease: WindowLease, cancelled: Event) -> None:
        if cancelled.is_set():
            raise WindowContractError("Window execution cancelled or lease renewal failed")
        self.store.assert_lease(lease)

    def _run(self, plan: WindowPlan, lease: WindowLease, cancelled: Event) -> WindowResult:
        journal = self.journal_factory(lease, plan.run_id)
        state = journal.read("run")
        if state is None:
            self.source.validate(plan)
            journal.write("run", {"phase": "planned"})
            state = {"phase": "planned"}
        phase = state.get("phase")
        if phase not in ("planned", "publishing", "published", "evidence-complete", "succeeded"):
            raise WindowContractError("Unsupported run phase")
        if phase == "planned":
            self.source.validate(plan)
            receipts = self._chunks(plan, lease, journal, cancelled)
            self._check(lease, cancelled)
            generation = self.target.prepare(plan, receipts, lease)
            if not isinstance(generation, str) or not generation:
                raise WindowContractError("Target generation must be nonempty")
            journal.write(
                "run",
                {"phase": "publishing", "generation": generation, "receipts": [receipt_data(r) for r in receipts]},
            )
            self._check(lease, cancelled)
            # Persist intent before exchange; every exception/restart reconciles only.
            try:
                self.target.publish(plan, generation, lease)
            except Exception:
                recover_publication(self.target, plan, generation, lease)
            recover_publication(self.target, plan, generation, lease)
        else:
            persisted_generation = state["generation"]
            if not isinstance(persisted_generation, str) or not persisted_generation:
                raise WindowContractError("Invalid persisted generation")
            generation = persisted_generation
            receipts = self._load_receipts(plan, state)
            recover_publication(self.target, plan, generation, lease)
        self._check(lease, cancelled)
        completed = {"generation": generation, "receipts": [receipt_data(r) for r in receipts]}
        if phase != "succeeded":
            journal.write("run", dict(completed, phase="published"))
            self._check(lease, cancelled)
            self.evidence(plan, generation, receipts, lease)
            journal.write("run", dict(completed, phase="evidence-complete"))
            self._check(lease, cancelled)
            self.advance_state(plan, lease)
            journal.write("run", dict(completed, phase="succeeded"))
        return WindowResult(plan.run_id, generation, receipts)

    @staticmethod
    def _load_receipts(plan: WindowPlan, state: dict[str, object]) -> tuple[ChunkReceipt, ...]:
        records = state.get("receipts")
        if not isinstance(records, list):
            raise WindowContractError("Missing persisted publication receipts")
        receipts = tuple(decode_receipt(record) for record in records)
        expected = {chunk.chunk_id for chunk in plan.chunks}
        if len(receipts) != len(expected) or {r.chunk_id for r in receipts} != expected:
            raise WindowContractError("Publication receipts must cover each plan chunk exactly once")
        if any(r.attempt_id not in {f"{r.chunk_id}-{i}" for i in range(3)} for r in receipts):
            raise WindowContractError("Publication receipt has invalid attempt identity")
        return receipts

    def _chunks(
        self, plan: WindowPlan, lease: WindowLease, journal: WindowProgressJournal, cancelled: Event
    ) -> tuple[ChunkReceipt, ...]:
        chunks = iter(enumerate(plan.chunks))
        results: dict[int, ChunkReceipt] = {}
        with ThreadPoolExecutor(max_workers=plan.workers) as pool:
            pending = {}
            for index, chunk in chunks:
                pending[pool.submit(self._chunk, plan, chunk, lease, journal, cancelled)] = index
                if len(pending) == plan.workers:
                    break
            try:
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        results[pending.pop(future)] = future.result()
                    for _ in range(len(done)):
                        item = next(chunks, None)
                        if item is None:
                            break
                        index, chunk = item
                        pending[pool.submit(self._chunk, plan, chunk, lease, journal, cancelled)] = index
                return tuple(results[index] for index in range(len(results)))
            except BaseException:
                cancelled.set()
                for future in pending:
                    future.cancel()
                raise

    @staticmethod
    def _cleanup_error(primary: BaseException | None, cleanup: BaseException) -> None:
        if primary is None:
            raise cleanup
        add_note = getattr(primary, "add_note", None)
        if add_note is not None:
            add_note(f"Cleanup also failed: {type(cleanup).__name__}: {cleanup}")

    def _rows(
        self, plan: WindowPlan, chunk: WindowChunk, lease: WindowLease, cancelled: Event
    ) -> Generator[tuple[object, ...], None, None]:
        self._check(lease, cancelled)
        rows = self.source.read(plan, chunk)
        try:
            for index, row in enumerate(rows):
                if cancelled.is_set():
                    raise WindowContractError("Window execution cancelled")
                if index % 1024 == 0:
                    self._check(lease, cancelled)
                yield row
        finally:
            close = getattr(rows, "close", None)
            if close is not None:
                primary = sys.exc_info()[1]
                try:
                    close()
                except BaseException as error:
                    self._cleanup_error(None if isinstance(primary, GeneratorExit) else primary, error)

    def _chunk(
        self, plan: WindowPlan, chunk: WindowChunk, lease: WindowLease, journal: WindowProgressJournal, cancelled: Event
    ) -> ChunkReceipt:
        key = "chunk/" + chunk.chunk_id
        previous = journal.read(key)
        if previous and previous.get("phase") == "failed":
            raise WindowContractError("Previous chunk attempt failed terminally; start a new approved run")
        attempt = int(str(previous["attempt"])) if previous else 0
        while attempt <= 2:
            self._check(lease, cancelled)
            attempt_id = f"{chunk.chunk_id}-{attempt}"
            if previous:
                receipt = self.target.inspect_attempt(plan, chunk, attempt_id, lease)
                if receipt is not None:
                    self._verified(receipt, chunk, attempt_id)
                    if previous.get("phase") == "verified" and previous.get("receipt") != receipt_data(receipt):
                        raise WindowContractError("Verified staging receipt changed since checkpoint")
                    journal.write(key, {"attempt": attempt, "phase": "verified", "receipt": receipt_data(receipt)})
                    return receipt
                # A durable verified record with missing staging is corruption, not replay.
                if previous.get("phase") == "verified":
                    raise WindowContractError("Verified staging is missing or changed")
                if attempt == 2:
                    journal.write(key, {"attempt": attempt, "phase": "failed"})
                    raise WindowContractError("Chunk retry budget exhausted")
                attempt += 1
                attempt_id = f"{chunk.chunk_id}-{attempt}"
                journal.write(key, {"attempt": attempt, "phase": "staging"})
                self._check(lease, cancelled)
                # Re-confirm every older attempt: an earlier cleanup may have crashed.
                for prior_attempt in range(attempt):
                    self._check(lease, cancelled)
                    self.target.discard_attempt(plan, chunk, f"{chunk.chunk_id}-{prior_attempt}", lease)
                previous = None
            journal.write(key, {"attempt": attempt, "phase": "staging"})
            rows = self._rows(plan, chunk, lease, cancelled)
            try:
                receipt = self.target.stage(plan, chunk, attempt_id, rows, lease)
                self._verified(receipt, chunk, attempt_id)
                observed = self.target.inspect_attempt(plan, chunk, attempt_id, lease)
                if observed != receipt:
                    raise WindowContractError("Staging receipt failed server-side reconciliation")
            except WindowTransientError:
                self._check(lease, cancelled)
                observed = self.target.inspect_attempt(plan, chunk, attempt_id, lease)
                if observed is not None:
                    receipt = self._verified(observed, chunk, attempt_id)
                else:
                    if attempt == 2:
                        journal.write(key, {"attempt": attempt, "phase": "failed"})
                        raise
                    previous = {"attempt": attempt, "phase": "staging"}
                    self.sleeper(0.1 * 2**attempt)
                    continue
            except WindowContractError:
                journal.write(key, {"attempt": attempt, "phase": "failed"})
                raise
            finally:
                primary = sys.exc_info()[1]
                try:
                    rows.close()
                except BaseException as error:
                    self._cleanup_error(primary, error)
            journal.write(key, {"attempt": attempt, "phase": "verified", "receipt": receipt_data(receipt)})
            return receipt
        raise WindowContractError("Chunk retry budget exhausted")

    @staticmethod
    def _verified(receipt: ChunkReceipt, chunk: WindowChunk, attempt_id: str) -> ChunkReceipt:
        if receipt.chunk_id != chunk.chunk_id or receipt.attempt_id != attempt_id:
            raise WindowContractError("Staging receipt belongs to a different chunk attempt")
        return receipt


def recover_publication(target: WindowTarget, plan: WindowPlan, generation: str, lease: WindowLease) -> None:
    """A prior publishing marker permits reconciliation only, never EXCHANGE replay."""
    if target.inspect_publication(plan, generation, lease) != "published":
        raise WindowOutcomeUnknown("Publication is not proven; inspect target generation before recovery")
