"""Spawn-safe peer barrier for deterministic parallel-retry live proofs."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from dpone.backfill.process_lane_contracts import BackfillProcessLaneBootstrap
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from dpone.runtime.etl.backfill_process_runtime import open_backfill_process_lane

REVIEWED_RETRY_BARRIER_ENTRYPOINT = (
    "tests.integration.postgres.postgres_mssql_backfill_retry_barrier_live_support:"
    "open_reviewed_retry_barrier_process_lane"
)
_BARRIER_DEADLINE_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class ReviewedRetryBarrierLanePayload:
    """First-attempt peer authority serialized into spawned process lanes."""

    production_payload: Any
    marker: str
    inject_barrier: bool


def wrap_retry_barrier_runtime(
    runtime: Any,
    *,
    marker: Path,
    inject_barrier: bool,
) -> BackfillProcessLaneRuntime:
    """Replace only the process-lane opener while retaining production DI."""

    if not isinstance(runtime, BackfillProcessLaneRuntime):
        raise AssertionError("reviewed retry barrier requires the production process-lane runtime")
    bootstrap = runtime.bootstrap
    return replace(
        runtime,
        bootstrap=BackfillProcessLaneBootstrap(
            entrypoint=REVIEWED_RETRY_BARRIER_ENTRYPOINT,
            payload=ReviewedRetryBarrierLanePayload(
                production_payload=bootstrap.payload,
                marker=str(marker),
                inject_barrier=inject_barrier,
            ),
        ),
    )


@contextmanager
def open_reviewed_retry_barrier_process_lane(
    worker_id: int,
    payload: Any,
    operation_lease_factory: Any,
):
    """Hold first-attempt peers until chunk 1 reaches its canonical CAS fault."""

    if not isinstance(payload, ReviewedRetryBarrierLanePayload):
        raise RuntimeError("reviewed retry barrier payload is invalid")
    marker = Path(payload.marker)
    with open_backfill_process_lane(
        worker_id,
        payload.production_payload,
        operation_lease_factory,
    ) as production_run:

        def run_chunk(load_config: Any) -> Any:
            chunk_index = int(load_config.options["backfill"]["chunk_context"]["index"])
            if payload.inject_barrier and chunk_index != 1:
                _wait_for_marker(marker)
                raise RuntimeError("reviewed retry peer quiesced after canonical lease fault")
            return production_run(load_config)

        yield run_chunk


def _wait_for_marker(marker: Path) -> None:
    deadline = time.monotonic() + _BARRIER_DEADLINE_SECONDS
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not marker.exists():
        raise RuntimeError("reviewed retry-barrier marker timed out")


__all__ = [
    "REVIEWED_RETRY_BARRIER_ENTRYPOINT",
    "ReviewedRetryBarrierLanePayload",
    "open_reviewed_retry_barrier_process_lane",
    "wrap_retry_barrier_runtime",
]
