from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from dpone.strategy_intelligence.replay import ReplayExecutionRequest


@dataclass(frozen=True, slots=True)
class ReplayBackendResult:
    passed: bool
    message: str
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReplayAdapterResult:
    status: str
    operations: tuple[str, ...]
    state_committed: bool
    diagnostics: tuple[ReplayBackendResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operations": list(self.operations),
            "state_committed": self.state_committed,
            "diagnostics": [item.to_dict() for item in self.diagnostics],
        }


class ReplayBackend(ABC):
    """Backend port used by replay adapters.

    Live implementations can call database/Kafka clients. Unit tests use fakes.
    """

    @abstractmethod
    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        raise NotImplementedError

    @abstractmethod
    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        raise NotImplementedError

    @abstractmethod
    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        raise NotImplementedError

    @abstractmethod
    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        raise NotImplementedError

    @abstractmethod
    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        raise NotImplementedError


class ArtifactOnlyReplayBackend(ReplayBackend):
    """Safe default backend that records adapter sequence without external mutation."""

    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"artifact-only staging validation for {request.run_id}")

    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"artifact-only finalizer contract for {request.sink_type}")

    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"artifact-only event replay contract for {request.sink_type}")

    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"artifact-only reconciliation contract for {request.run_id}")

    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"artifact-only state commit contract for {request.run_id}")


class ReplayAdapter(ABC):
    @abstractmethod
    def execute(self, request: ReplayExecutionRequest) -> ReplayAdapterResult:
        raise NotImplementedError


class DbReplayAdapter(ReplayAdapter):
    """Staging-first replay adapter for mutable database sinks."""

    def __init__(self, *, backend: ReplayBackend) -> None:
        self._backend = backend

    def execute(self, request: ReplayExecutionRequest) -> ReplayAdapterResult:
        return _execute_sequence(
            request,
            steps=(
                ("validate_staging", self._backend.validate_staging),
                ("execute_finalizer", self._backend.execute_finalizer),
                ("reconcile", self._backend.reconcile),
                ("commit_state", self._backend.commit_state),
            ),
        )


class KafkaReplayAdapter(ReplayAdapter):
    """Replay adapter for Kafka event-log sinks."""

    def __init__(self, *, backend: ReplayBackend) -> None:
        self._backend = backend

    def execute(self, request: ReplayExecutionRequest) -> ReplayAdapterResult:
        return _execute_sequence(
            request,
            steps=(
                ("validate_staging", self._backend.validate_staging),
                ("produce_replay_events", self._backend.produce_replay_events),
                ("reconcile", self._backend.reconcile),
                ("commit_state", self._backend.commit_state),
            ),
        )


class ReplayAdapterRegistry:
    """Resolve replay adapter by sink type."""

    def __init__(self, *, backend: ReplayBackend | None = None) -> None:
        self._backend = backend or ArtifactOnlyReplayBackend()

    def resolve(self, sink_type: str) -> ReplayAdapter:
        sink = _normalize_sink(sink_type)
        if sink in {"mssql", "postgres", "clickhouse", "bigquery"}:
            return DbReplayAdapter(backend=self._backend)
        if sink == "kafka":
            return KafkaReplayAdapter(backend=self._backend)
        raise ValueError(f"Unsupported replay sink: {sink_type}")


def _execute_sequence(
    request: ReplayExecutionRequest,
    *,
    steps: tuple[tuple[str, Callable[[ReplayExecutionRequest], ReplayBackendResult]], ...],
) -> ReplayAdapterResult:
    operations: list[str] = []
    diagnostics: list[ReplayBackendResult] = []
    state_committed = False
    for name, fn in steps:
        result = fn(request)
        diagnostics.append(result)
        operations.append(_operation_label(name, request))
        if not result.passed:
            return ReplayAdapterResult(
                status="failed",
                operations=tuple(operations),
                state_committed=state_committed,
                diagnostics=tuple(diagnostics),
            )
        if name == "commit_state":
            state_committed = True
    return ReplayAdapterResult(
        status="executed",
        operations=tuple(operations),
        state_committed=state_committed,
        diagnostics=tuple(diagnostics),
    )


def _operation_label(name: str, request: ReplayExecutionRequest) -> str:
    if name == "validate_staging":
        return f"validate_staging:{request.strategy_mode}"
    if name == "execute_finalizer":
        return f"execute_finalizer:{request.sink_type}:{request.strategy_mode}"
    if name == "produce_replay_events":
        return f"produce_replay_events:{request.sink_type}:{request.strategy_mode}"
    if name == "reconcile":
        return f"reconcile:{request.sink_type}:{request.strategy_mode}"
    if name == "commit_state":
        return f"commit_state:{request.run_id}"
    return name


def _normalize_sink(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    aliases = {"sqlserver": "mssql", "sql_server": "mssql", "bq": "bigquery"}
    return aliases.get(normalized, normalized)
