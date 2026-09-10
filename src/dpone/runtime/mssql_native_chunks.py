"""Bounded single-query native staging with parent-owned CAS and spawned encoders.

Source creation and target publication are composition-owned. This service only
stages one acquired iterator and returns durable, independently verified receipts.
An interrupted partial query is never reconstructed from a mutable row offset.
"""

from __future__ import annotations

import multiprocessing
import os
import pickle
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event, Thread, get_ident
from typing import Any, cast

from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError, WindowLease, WindowOutcomeUnknown, WindowTransientError
from dpone.contracts.mssql_native_chunks import (
    EncodedNativeFile,
    NativeChunkLimits,
    NativeChunkPlan,
    NativeChunkReceipt,
    NativeStageComplete,
)
from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_native_chunks import NativeChunkImporter
from dpone.runtime.mssql_native_capacity import require_native_spool_capacity
from dpone.runtime.mssql_native_chunks_files import (
    NativeRow,
    discard_native_files,
    encode_native_frame,
    native_frames,
    verify_native_file,
)
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_models import SourceNativeWireContract

ImporterFactory = Callable[[], AbstractContextManager[NativeChunkImporter]]


class NativeReextractRequired(WindowContractError):
    """Partial staging is settled; restart the complete query with a new invocation."""


def _encode(*args: Any) -> tuple[EncodedNativeFile, dict[str, Any]]:
    start = time.monotonic()
    file = encode_native_frame(*args)
    return file, dict(phase="encode", start=start, end=time.monotonic(), worker=os.getpid())


@dataclass
class _Work:
    ordinal: int
    encoded_bytes: int
    file: EncodedNativeFile | None = None
    attempt: int = 0


class BoundedNativeChunks:
    """Bound rows/IPC/files/attempt objects while encoding and imports overlap.

    Each importer context owns one independent connection. The caller owns the
    target lease before opening the source; this service renews it while staging.
    Target importers must enforce server writer fencing at mutation boundaries.
    """

    def __init__(
        self,
        *,
        store: WindowStore,
        importer_factory: ImporterFactory,
        work_dir: Path,
        limits: NativeChunkLimits,
        lease_ttl: float = 60.0,
    ) -> None:
        if lease_ttl <= 0:
            raise ValueError("mssql_native.invalid_lease_ttl")
        self.store, self.importer_factory, self.work_dir = store, importer_factory, work_dir
        self.limits, self.lease_ttl = limits, lease_ttl

    def _check(self, lease: WindowLease, cancelled: Event) -> None:
        if cancelled.is_set():
            raise WindowContractError("mssql_native.cancelled_or_lease_lost")
        self.store.assert_lease(lease)

    def _capacity(
        self, importer: NativeChunkImporter, observation: dict[str, Any] | None = None, key: str = "allocated"
    ) -> int:
        allocated = importer.allocated_bytes()
        if type(allocated) is not int or allocated < 0:
            raise WindowContractError("mssql_native.invalid_allocation_observation")
        if observation is not None:
            observation[key] = allocated
        if allocated > self.limits.stage_allocated_bytes_stop_threshold:
            raise WindowContractError(f"mssql_native.stage_allocation_threshold_exceeded:{allocated}")
        return allocated

    def _heartbeat(self, lease: WindowLease, stopped: Event, cancelled: Event) -> None:
        while not stopped.wait(self.lease_ttl / 3):
            try:
                self.store.renew(lease, self.lease_ttl)
            except Exception:
                cancelled.set()
                return

    def stage(
        self,
        plan: NativeChunkPlan,
        rows: Iterator[NativeRow],
        contract: SourceNativeWireContract,
        lease: WindowLease,
        cancelled: Event | None = None,
        completion_metadata: Callable[[], dict[str, Any]] | None = None,
    ) -> NativeStageComplete:
        """Consume once; stage_complete commits only after EOF and every verification."""
        cancel, stopped = cancelled if cancelled is not None else Event(), Event()
        heartbeat: Thread | None = None
        try:
            if plan.wire_fingerprint != contract.type_layout_hash:
                raise WindowContractError("mssql_native.wire_identity_changed")
            journal = NativeChunkJournal(self.store, lease, plan)
            if journal.data is not None:
                return self.recover(plan, lease)
            self._check(lease, cancel)
            require_native_spool_capacity(self.work_dir, self.limits)
            journal.begin()
            journal.bind_limits(asdict(self.limits))
            heartbeat = Thread(target=self._heartbeat, args=(lease, stopped, cancel), daemon=True)
            heartbeat.start()
            return self._stage(journal, rows, contract, cancel, completion_metadata)
        finally:
            primary = sys.exc_info()[1]
            stopped.set()
            if heartbeat is not None:
                heartbeat.join(timeout=35)
            try:
                close = getattr(rows, "close", None)
                if close is not None:
                    close()
            except BaseException as error:
                self._cleanup_error(primary, error)
            if heartbeat is not None and heartbeat.is_alive():
                self._cleanup_error(primary, WindowContractError("mssql_native.lease_renewal_not_settled"))

    def recover(self, plan: NativeChunkPlan, lease: WindowLease) -> NativeStageComplete:
        """Reverify complete stages without source I/O; partial progress needs re-extraction.

        This is a staging-only recovery API. A publication owner must reconcile its
        target transaction receipt before asking to settle any partial extraction.
        """
        journal = NativeChunkJournal(self.store, lease, plan)
        journal.bind_limits(asdict(self.limits))
        publication = journal.publication.state()
        if publication is not None and publication["phase"] not in ("preparing", "prepared"):
            raise WindowOutcomeUnknown("mssql_native.publication_requires_reconciliation")
        result = journal.completed()
        with self.importer_factory() as importer:
            if result is not None:
                for receipt in result.receipts:
                    self.store.assert_lease(lease)
                    if importer.inspect(plan, receipt, lease) != receipt:
                        raise WindowContractError("mssql_native.recovered_stage_changed")
                self._capacity(importer)
                return result
            for attempt_id in journal.attempts():
                self.store.assert_lease(lease)
                importer.settle(plan, attempt_id, lease)
        self.store.assert_lease(lease)
        discard_native_files(self.work_dir / journal.key.rsplit("/", 1)[-1])
        if journal.data is not None and journal.data["phase"] == "staging":
            journal.reextract_required()
        raise NativeReextractRequired("mssql_native.reextract_required")

    def _import(
        self, plan: NativeChunkPlan, file: EncodedNativeFile, attempt: str, lease: WindowLease, cancelled: Event
    ) -> tuple[NativeChunkReceipt | Exception, dict[str, Any]]:
        start = time.monotonic()
        observation: dict[str, Any] = dict(phase="import_verify", start=start, worker=get_ident(), attempt=attempt)
        try:
            self._check(lease, cancelled)
            verify_native_file(file)
            with self.importer_factory() as importer:
                self.store.assert_lease(lease)
                self._capacity(importer, observation, "allocated_before")
                receipt = importer.import_file(plan, file, attempt, lease)
                observed = importer.inspect(plan, receipt, lease)
                if observed != receipt:
                    raise WindowContractError("mssql_native.import_verification_changed")
                self._capacity(importer, observation, "allocated_after")
            return receipt, dict(observation, end=time.monotonic(), outcome="verified")
        except Exception as error:
            return error, dict(observation, end=time.monotonic(), outcome="failed", error_type=type(error).__name__)

    def _stage(
        self,
        journal: NativeChunkJournal,
        rows: Iterator[NativeRow],
        contract: SourceNativeWireContract,
        cancelled: Event,
        completion_metadata: Callable[[], dict[str, Any]] | None,
    ) -> NativeStageComplete:
        limits, plan, lease = self.limits, journal.plan, journal.lease
        self.work_dir.mkdir(parents=True, exist_ok=True)
        directory = self.work_dir / journal.key.rsplit("/", 1)[-1]
        directory.mkdir(mode=0o700, exist_ok=False)
        envelope = (
            contract,
            (),
            directory / f"{limits.max_staging_tables}.native",
            limits.max_staging_tables,
            limits.max_row_bytes,
            limits.max_bytes,
        )
        ipc_overhead = len(pickle.dumps(envelope, protocol=5)) + 128
        if ipc_overhead + 64 > limits.max_bytes:
            raise WindowContractError("mssql_native.IPC_metadata_limit_exceeded")
        frames = native_frames(
            rows, contract, limits, check=lambda: self._check(lease, cancelled), ipc_overhead=ipc_overhead
        )
        encoder = MssqlNativeEncoder(contract, max_row_bytes=limits.max_row_bytes)
        pending: dict[Future[Any], _Work] = {}
        observations: list[dict[str, Any]] = []
        total, ordinal, eof = 0, 0, False
        with (
            ProcessPoolExecutor(
                max_workers=limits.parallelism, mp_context=multiprocessing.get_context("spawn")
            ) as encoders,
            ThreadPoolExecutor(max_workers=limits.parallelism) as imports,
        ):
            try:
                while not eof or pending:
                    while not eof and len(pending) < limits.parallelism + limits.max_pending:
                        self._check(lease, cancelled)
                        frame = next(frames, None)
                        if frame is None:
                            eof = True
                            break
                        # Reserve one additional stage for UNION ALL preparation.
                        if ordinal + 2 > limits.max_staging_tables:
                            raise WindowContractError("mssql_native.staging_object_limit_exceeded")
                        if len(pickle.dumps(frame, protocol=5)) > limits.max_bytes:
                            raise WindowContractError("mssql_native.IPC_frame_limit_exceeded")
                        size = sum(encoder.encoded_row_size(row) for row in frame)
                        if total + size > limits.max_total_encoded_bytes:
                            raise WindowContractError("mssql_native.total_encoded_bytes_exceeded")
                        with self.importer_factory() as importer:
                            self._capacity(importer)
                        total += size
                        args = (contract, frame, directory / f"{ordinal}.native", ordinal, limits.max_row_bytes, size)
                        if len(pickle.dumps(args, protocol=5)) + 128 > limits.max_bytes:
                            raise WindowContractError("mssql_native.IPC_task_limit_exceeded")
                        future = encoders.submit(_encode, *args)
                        pending[future] = _Work(ordinal, size)
                        ordinal += 1
                    if not pending:
                        continue
                    done, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                    self._check(lease, cancelled)
                    for future in done:
                        work = pending.pop(future)
                        try:
                            value, observation = future.result()
                            observations.append(observation)
                            journal.record_observations(tuple(observations))
                            if isinstance(value, Exception):
                                raise value
                        except WindowTransientError:
                            if work.file is None or work.attempt == 2:
                                raise
                            with self.importer_factory() as importer:
                                importer.settle(plan, journal.attempt_id(work.ordinal, work.attempt), lease)
                            work.attempt += 1
                            journal.attempt(work.ordinal, work.attempt, work.file)
                            pending[
                                imports.submit(
                                    self._import,
                                    plan,
                                    work.file,
                                    journal.attempt_id(work.ordinal, work.attempt),
                                    lease,
                                    cancelled,
                                )
                            ] = work
                            continue
                        if work.file is None:
                            file: EncodedNativeFile = value
                            if file.encoded_bytes != work.encoded_bytes:
                                raise WindowContractError("mssql_native.encoder_size_authority_changed")
                            work.file = file
                            journal.attempt(work.ordinal, 0, file)
                            pending[
                                imports.submit(
                                    self._import, plan, file, journal.attempt_id(work.ordinal, 0), lease, cancelled
                                )
                            ] = work
                        else:
                            journal.verified(cast(NativeChunkReceipt, value))
                            work.file.path.unlink()
                self._check(lease, cancelled)
                return journal.complete(
                    source_eof=eof,
                    observations=tuple(observations),
                    completion_metadata={} if completion_metadata is None else completion_metadata(),
                )
            except BaseException as primary:
                cancelled.set()
                for future in pending:
                    future.cancel()
                # Settle all writers before collecting their final failure evidence.
                imports.shutdown(wait=True, cancel_futures=True)
                encoders.shutdown(wait=True, cancel_futures=True)
                for future in pending:
                    if not future.cancelled():
                        try:
                            _value, observation = future.result()
                            observations.append(observation)
                        except BaseException as cleanup:
                            self._cleanup_error(primary, cleanup)
                try:
                    journal.record_observations(tuple(observations))
                except BaseException as cleanup:
                    self._cleanup_error(primary, cleanup)
                raise
            finally:
                cleanup_primary = sys.exc_info()[1]
                try:
                    frames.close()
                except BaseException as cleanup:
                    self._cleanup_error(cleanup_primary, cleanup)

    @staticmethod
    def _cleanup_error(primary: BaseException | None, cleanup: BaseException) -> None:
        if primary is None:
            raise cleanup
        add_note = getattr(primary, "add_note", None)
        if add_note is not None:
            add_note(f"Native staging cleanup also failed: {type(cleanup).__name__}")
