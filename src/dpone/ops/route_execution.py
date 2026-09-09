"""Route execution ledger facade over local store, fencing, and commit policy."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.execution_models import (
    RouteArtifactHash,
    RouteExecutionLedgerReport,
    RouteExecutionStage,
    RouteExecutionStatus,
    RouteExecutionStep,
)
from dpone.ops.routes.execution_policy import RouteCommitProtocolPolicy
from dpone.ops.routes.execution_store import LocalRouteExecutionLedgerStore, RouteExecutionLedgerStore
from dpone.ops.routes.models import RouteKey

_UTC = timezone.utc  # noqa: UP017


class RouteExecutionService:
    """Record idempotent route execution steps without running heavy data work."""

    def __init__(
        self,
        *,
        store: RouteExecutionLedgerStore | None = None,
        policy: RouteCommitProtocolPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store or LocalRouteExecutionLedgerStore()
        self._policy = policy or RouteCommitProtocolPolicy()
        self._clock = clock or (lambda: datetime.now(_UTC))

    def record_step(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        dataset: str,
        run_id: str,
        stage: str | RouteExecutionStage,
        status: str | RouteExecutionStatus,
        runner_id: str,
        source_boundary: str = "",
        sink_boundary: str = "",
        artifact_paths: Mapping[str, str | Path] | None = None,
        metadata: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        lease_ttl_seconds: int | None = None,
    ) -> RouteExecutionLedgerReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=_UTC)

        lease = None
        lease_blockers: tuple[str, ...] = tuple()
        if lease_ttl_seconds is not None:
            lease, lease_blockers = self._store.acquire_lease(
                output_dir=directory,
                route=route,
                dataset=dataset,
                owner=runner_id,
                now=now,
                ttl_seconds=lease_ttl_seconds,
            )

        artifact_hashes = _artifact_hashes(artifact_paths or {})
        candidate = RouteExecutionStep(
            stage=RouteExecutionStage.of(stage),
            status=RouteExecutionStatus.of(status),
            runner_id=runner_id,
            created_at=now.isoformat(),
            source_boundary=str(source_boundary),
            sink_boundary=str(sink_boundary),
            idempotency_key=idempotency_key
            or _default_idempotency_key(
                route=route,
                dataset=dataset,
                run_id=run_id,
                stage=RouteExecutionStage.of(stage),
                source_boundary=str(source_boundary),
                sink_boundary=str(sink_boundary),
                artifact_hashes=artifact_hashes,
            ),
            artifact_hashes=artifact_hashes,
            metadata=dict(metadata or {}),
        )
        existing_steps = self._store.read_steps(output_dir=directory, route=route, dataset=dataset, run_id=run_id)
        replay_warnings, conflict_blockers = _idempotency_decision(existing_steps, candidate)

        blockers = (*lease_blockers, *conflict_blockers)
        decision = self._policy.evaluate(
            existing_steps=existing_steps,
            candidate=candidate,
            blockers=blockers,
            warnings=replay_warnings,
        )
        if decision.passed and not replay_warnings:
            steps = (*existing_steps, candidate)
            ledger_path, appended = self._store.append_steps_if_version(
                output_dir=directory,
                route=route,
                dataset=dataset,
                run_id=run_id,
                expected_step_count=len(existing_steps),
                steps=steps,
            )
            if not appended:
                latest_steps = self._store.read_steps(output_dir=directory, route=route, dataset=dataset, run_id=run_id)
                replay_warnings, conflict_blockers = _idempotency_decision(latest_steps, candidate)
                if replay_warnings:
                    steps = latest_steps
                    decision = self._policy.evaluate(
                        existing_steps=latest_steps,
                        candidate=candidate,
                        warnings=replay_warnings,
                    )
                else:
                    steps = latest_steps
                    decision = self._policy.evaluate(
                        existing_steps=latest_steps,
                        candidate=candidate,
                        blockers=("route_execution.concurrent_write_conflict", *conflict_blockers),
                    )
        else:
            steps = existing_steps
            ledger_path = self._store.ledger_path(output_dir=directory, route=route, dataset=dataset, run_id=run_id)
            if not ledger_path.exists():
                self._store.write_steps(output_dir=directory, route=route, dataset=dataset, run_id=run_id, steps=steps)

        report = RouteExecutionLedgerReport(
            route=route,
            dataset=dataset,
            run_id=run_id,
            passed=decision.passed,
            level=decision.level,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            steps=steps,
            lease=lease,
            output_dir=str(directory),
            ledger_path=str(ledger_path),
            json_path=str(directory / "route_execution_ledger.json"),
            markdown_path=str(directory / "route_commit_protocol.md"),
            store_backend=self._store.backend_name,
        )
        report.write()
        return report


def _artifact_hashes(artifact_paths: Mapping[str, str | Path]) -> dict[str, RouteArtifactHash]:
    values: dict[str, RouteArtifactHash] = {}
    for name, value in sorted(artifact_paths.items()):
        path = Path(value)
        exists = path.is_file()
        values[str(name)] = RouteArtifactHash(
            name=str(name),
            path=str(path),
            sha256=sha256_file(path) if exists else "0" * 64,
            missing=not exists,
        )
    return values


def _default_idempotency_key(
    *,
    route: RouteKey,
    dataset: str,
    run_id: str,
    stage: RouteExecutionStage,
    source_boundary: str,
    sink_boundary: str,
    artifact_hashes: Mapping[str, RouteArtifactHash],
) -> str:
    payload = {
        "route": route.case_id,
        "dataset": dataset,
        "run_id": run_id,
        "stage": stage.value,
        "source_boundary": source_boundary,
        "sink_boundary": sink_boundary,
        "artifact_hashes": {name: item.sha256 for name, item in artifact_hashes.items()},
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _idempotency_decision(
    existing_steps: tuple[RouteExecutionStep, ...],
    candidate: RouteExecutionStep,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    for step in existing_steps:
        if step.idempotency_key != candidate.idempotency_key:
            continue
        if step.fingerprint() == candidate.fingerprint():
            return ("route_execution.idempotent_replay",), tuple()
        return tuple(), ("route_execution.idempotency_conflict",)
    return tuple(), tuple()
