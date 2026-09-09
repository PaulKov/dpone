"""Regression contracts for the PostgreSQL→MSSQL lifecycle live harness."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill.process_lane_contracts import BackfillProcessLaneBootstrap
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime
from tests.integration.postgres import postgres_mssql_backfill_lifecycle_live_support as support
from tests.integration.postgres import postgres_mssql_backfill_retry_barrier_live_support as retry_barrier


def test_reviewed_factory_forwards_portable_scope_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the injected factory aligned with the production composition root."""

    captured: dict[str, Any] = {}

    class CapturingOrchestrator:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(support, "BackfillOrchestrator", CapturingOrchestrator)
    column_resolver = object()
    history_proof = object()

    support.ReviewedBackfillOrchestratorFactory(support.ReviewedBackfillLifecycleController("success"))(
        chunk_runner=object(),
        sink_connector=object(),
        worker_state_store_factory=object(),
        portable_scope_column_resolver=column_resolver,
        portable_scope_history_proof=history_proof,
    )

    assert captured["portable_scope_column_resolver"] is column_resolver
    assert captured["portable_scope_history_proof"] is history_proof


def test_reviewed_parallel_retry_wraps_first_attempt_in_child_barrier(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install peer coordination after dispatch instead of failing a claim."""

    captured: dict[str, Any] = {}

    class CapturingOrchestrator:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(support, "BackfillOrchestrator", CapturingOrchestrator)
    runtime = BackfillProcessLaneRuntime(
        bootstrap=BackfillProcessLaneBootstrap(entrypoint="example:open_lane", payload=object()),
        renew_operation_lease=lambda *_args: True,
        validate_operation_binding=lambda *_args, **_kwargs: True,
        receipt_recovery=object(),
        parent_control_scope=lambda _store: _null_scope(),
    )

    support.ReviewedBackfillOrchestratorFactory(
        support.ReviewedBackfillLifecycleController(
            "retry_resume",
            retry_barrier_marker=tmp_path / "canonical-fault.marker",
        )
    )(
        chunk_runner=object(),
        sink_connector=object(),
        worker_state_store_factory=object(),
        worker_chunk_runner_factory=runtime,
    )

    wrapped = captured["worker_chunk_runner_factory"]
    assert isinstance(wrapped, BackfillProcessLaneRuntime)
    assert wrapped.bootstrap.entrypoint == retry_barrier.REVIEWED_RETRY_BARRIER_ENTRYPOINT
    assert isinstance(wrapped.bootstrap.payload, retry_barrier.ReviewedRetryBarrierLanePayload)
    assert wrapped.bootstrap.payload.inject_barrier is True


def test_reviewed_parallel_retry_does_not_fail_during_parent_claim(tmp_path: Any) -> None:
    """The child barrier owns peer coordination after every claim is durable."""

    controller = support.ReviewedBackfillLifecycleController(
        "retry_resume",
        retry_barrier_marker=tmp_path / "canonical-fault.marker",
    )
    controller.before_chunk_runner(
        SimpleNamespace(
            chunk_index=2,
            owner="owner-2",
        )
    )

    assert controller.events == (
        {
            "event": "retry_worker_started",
            "phase": "first_invocation",
            "chunk_index": 2,
        },
    )


def test_reviewed_retry_barrier_never_opens_source_for_peer(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A peer becomes terminal only after the parent publishes the fault marker."""

    marker = tmp_path / "canonical-fault.marker"
    marker.write_text("ready\n", encoding="utf-8")
    production_calls: list[int] = []

    @contextmanager
    def fake_production_lane(*_args: Any, **_kwargs: Any):
        def run(load_config: Any) -> dict[str, int]:
            index = int(load_config.options["backfill"]["chunk_context"]["index"])
            production_calls.append(index)
            return {"chunk_index": index}

        yield run

    monkeypatch.setattr(retry_barrier, "open_backfill_process_lane", fake_production_lane)
    payload = retry_barrier.ReviewedRetryBarrierLanePayload(
        production_payload=object(),
        marker=str(marker),
        inject_barrier=True,
    )

    with retry_barrier.open_reviewed_retry_barrier_process_lane(2, payload, object()) as run:
        with pytest.raises(RuntimeError, match="peer quiesced"):
            run(_chunk_config(2))
        assert run(_chunk_config(1)) == {"chunk_index": 1}

    assert production_calls == [1]


@contextmanager
def _null_scope():
    yield None


def _chunk_config(index: int) -> Any:
    return SimpleNamespace(options={"backfill": {"chunk_context": {"index": index}}})
