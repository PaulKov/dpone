"""Fixed-lane parallel chunk scheduling with worker-scoped state sessions."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
from queue import Empty, SimpleQueue
from threading import Event
from typing import TYPE_CHECKING, Any

from dpone.backfill.process_lane_parent import run_process_chunk_lanes
from dpone.backfill.state import FileBackfillStateStore

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

    if state_store_factory is None:
        raise RuntimeError("backfill.parallel_worker_state_store_factory_required")
    lane_count = min(workers, len(pending))
    chunk_queue: SimpleQueue[Any] = SimpleQueue()
    for chunk in pending:
        chunk_queue.put(chunk)
    stop = Event()
    with ThreadPoolExecutor(max_workers=lane_count, thread_name_prefix="dpone-backfill") as pool:
        futures = [
            pool.submit(
                _run_lane,
                worker_id,
                chunk_queue,
                stop=stop,
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
                    stop.set()
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
    stop: Event,
    state_store_factory: BackfillWorkerStateStoreFactory,
    chunk_runner_factory: BackfillWorkerChunkRunnerFactory | None,
    run_chunk: WorkerChunkRunner,
) -> list[str]:
    errors: list[str] = []
    try:
        runner_context = chunk_runner_factory(worker_id) if chunk_runner_factory is not None else nullcontext(None)
        with state_store_factory(worker_id) as store, runner_context as chunk_runner:
            while not stop.is_set():
                try:
                    chunk = chunk_queue.get_nowait()
                except Empty:
                    break
                # A peer can fail between the loop condition and the queue pop.
                # No lease has been acquired yet, so leaving this item pending in
                # the durable ledger is the exact fail-fast boundary.
                if stop.is_set():
                    break
                if not run_chunk(chunk, store, errors, chunk_runner):
                    stop.set()
                    break
    except BaseException:
        stop.set()
        raise
    return errors


__all__ = ["run_parallel_chunk_lanes", "run_process_chunk_lanes"]
