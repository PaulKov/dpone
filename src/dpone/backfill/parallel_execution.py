"""Fixed-lane parallel chunk scheduling with worker-scoped state sessions."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
from queue import Empty, SimpleQueue
from typing import TYPE_CHECKING, Any

from dpone.backfill.process_lane_parent import run_process_chunk_lanes
from dpone.backfill.state import FileBackfillStateStore
from dpone.runtime._backfill_admission import _ChunkAdmission

if TYPE_CHECKING:
    from dpone.backfill.worker_runtime import BackfillWorkerChunkRunnerFactory
    from dpone.backfill.worker_state import BackfillWorkerStateStoreFactory

WorkerChunkRunner = Callable[[Any, FileBackfillStateStore, list[str], Callable[[Any], Any] | None], bool]


def run_parallel_chunk_lanes(
    pending: list[Any],
    *,
    workers: int,
    state_store_factory: BackfillWorkerStateStoreFactory | None,
    chunk_runner_factory: BackfillWorkerChunkRunnerFactory | None,
    run_chunk: WorkerChunkRunner,
    coordinator_tick: Callable[[], None] | None = None,
) -> list[str]:
    """Run fixed chunk lanes and close every isolated state session."""

    return _run_parallel_chunk_lanes(
        pending,
        workers=workers,
        state_store_factory=state_store_factory,
        chunk_runner_factory=chunk_runner_factory,
        run_chunk=run_chunk,
        coordinator_tick=coordinator_tick,
        admission=_ChunkAdmission(),
    )


def _run_parallel_chunk_lanes(
    pending: list[Any],
    *,
    workers: int,
    state_store_factory: BackfillWorkerStateStoreFactory | None,
    chunk_runner_factory: BackfillWorkerChunkRunnerFactory | None,
    run_chunk: WorkerChunkRunner,
    coordinator_tick: Callable[[], None] | None,
    admission: _ChunkAdmission,
) -> list[str]:
    """Inject one gate shared by lane closure and runtime lease acquisition.

    Queue removal is not admission. The runtime checks OPEN and acquires its
    durable lease under the gate; callbacks retain their four-argument contract.
    Failure closes admission before enclosing session/pool cleanup. Workers
    already admitted may finish without holding the gate.
    """

    if state_store_factory is None:
        raise RuntimeError("backfill.parallel_worker_state_store_factory_required")
    lane_count = min(workers, len(pending))
    chunk_queue: SimpleQueue[Any] = SimpleQueue()
    for chunk in pending:
        chunk_queue.put(chunk)
    with (
        ThreadPoolExecutor(max_workers=lane_count, thread_name_prefix="dpone-backfill") as pool,
        admission.on_failure(),
    ):
        futures = [
            pool.submit(
                _run_lane,
                worker_id,
                chunk_queue,
                admission=admission,
                state_store_factory=state_store_factory,
                chunk_runner_factory=chunk_runner_factory,
                run_chunk=run_chunk,
            )
            for worker_id in range(lane_count)
        ]
        errors: list[str] = []
        pending_futures = set(futures)
        coordinator_failed = False
        while pending_futures:
            if coordinator_tick is not None and not coordinator_failed:
                try:
                    coordinator_tick()
                except Exception:
                    coordinator_failed = True
                    admission.close()
                    errors.append("DPONE_BACKFILL_PROCESS_COORDINATOR_LOST")
            completed, pending_futures = wait(
                pending_futures,
                timeout=0.05,
                return_when=FIRST_COMPLETED,
            )
            for future in completed:
                errors.extend(future.result())
        return errors


def _run_lane(
    worker_id: int,
    chunk_queue: SimpleQueue[Any],
    *,
    admission: _ChunkAdmission,
    state_store_factory: BackfillWorkerStateStoreFactory,
    chunk_runner_factory: BackfillWorkerChunkRunnerFactory | None,
    run_chunk: WorkerChunkRunner,
) -> list[str]:
    errors: list[str] = []
    with admission.on_failure():
        runner_context = chunk_runner_factory(worker_id) if chunk_runner_factory is not None else nullcontext(None)
        with admission.on_failure(), state_store_factory(worker_id) as store:
            # Observe each nested context failure before an enclosing cleanup.
            with admission.on_failure(), runner_context as chunk_runner, admission.on_failure():
                while not admission.closed:
                    try:
                        chunk = chunk_queue.get_nowait()
                    except Empty:
                        break
                    if admission.closed:
                        break
                    if not run_chunk(chunk, store, errors, chunk_runner):
                        admission.close()
                        break
    return errors


__all__ = ["run_parallel_chunk_lanes", "run_process_chunk_lanes"]
