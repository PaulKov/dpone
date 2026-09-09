from __future__ import annotations

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.route_capabilities import (
    CapabilityEvidence,
    CapabilityRequirement,
    RouteCandidate,
    RuntimeCapability,
)
from dpone.runtime.route_runtime import (
    InMemoryLoadStepAuditStorage,
    RouteCapabilityBlocked,
    RouteCapabilityOrchestrator,
    RuntimeRouteDecisionPublisher,
    StaticCapabilityProbeRunner,
    StaticRouteCandidateProvider,
)
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.extract_result import ExtractResult


def test_required_blocked_route_fails_before_source_io_and_persists_decision() -> None:
    source = _CountingSource()
    sink = _Sink()
    audit = InMemoryLoadStepAuditStorage()
    processor = ETLProcessor(
        source,
        sink,
        etl_logger=_Logger(),
        route_capability_orchestrator=_orchestrator(
            _fast_candidate(),
            evidence={
                "storage.object.prefix_access": CapabilityEvidence.failure(
                    requirement_id="storage.object.prefix_access",
                    domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                    blockers=("storage.runtime_put_denied",),
                    details={"password": "super-secret"},
                )
            },
            audit_storage=audit,
        ),
    )

    with pytest.raises(RouteCapabilityBlocked, match="storage.runtime_put_denied"):
        processor.run(_cfg(runtime_capabilities={"mode": "required"}))

    assert source.state_calls == 0
    assert source.extract_calls == 0
    step = audit.records[0]
    assert step.kind == "runtime_route_decision"
    assert step.status == "blocked"
    assert step.details_json["selected_route_id"] == "blocked"
    assert step.details_json["evidence"][0]["details"]["password"] == "***REDACTED***"


def test_auto_route_fallback_records_summary_and_continues_extract() -> None:
    source = _CountingSource()
    audit = InMemoryLoadStepAuditStorage()
    processor = ETLProcessor(
        source,
        _Sink(),
        etl_logger=_Logger(),
        route_capability_orchestrator=_orchestrator(
            _fast_candidate(),
            _streaming_candidate(),
            evidence={
                "storage.object.prefix_access": CapabilityEvidence.failure(
                    requirement_id="storage.object.prefix_access",
                    domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                    blockers=("storage.runtime_put_denied",),
                ),
                "transport.push_stream": CapabilityEvidence.success(
                    requirement_id="transport.push_stream",
                    domain=RuntimeCapability.TRANSPORT,
                ),
            },
            audit_storage=audit,
        ),
    )

    result = processor.run(_cfg(runtime_capabilities={"mode": "auto"}))

    assert source.extract_calls == 1
    assert result["route_capabilities"]["summary"]["selected_route_id"] == "typed_binary_streaming"
    assert result["route_capabilities"]["summary"]["fallback_reason"] == "storage.runtime_put_denied"
    assert audit.records[0].status == "warning"


def test_warn_only_route_records_warning_and_keeps_requested_route() -> None:
    source = _CountingSource()
    audit = InMemoryLoadStepAuditStorage()
    processor = ETLProcessor(
        source,
        _Sink(),
        etl_logger=_Logger(),
        route_capability_orchestrator=_orchestrator(
            _fast_candidate(),
            evidence={
                "storage.object.prefix_access": CapabilityEvidence.failure(
                    requirement_id="storage.object.prefix_access",
                    domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                    blockers=("storage.runtime_put_denied",),
                )
            },
            audit_storage=audit,
        ),
    )

    result = processor.run(_cfg(runtime_capabilities={"mode": "warn_only"}))

    assert source.extract_calls == 1
    assert result["route_capabilities"]["summary"]["selected_route_id"] == "object_storage_pull"
    assert result["route_capabilities"]["summary"]["warnings"] == ["storage.runtime_put_denied"]
    assert audit.records[0].status == "warning"


def test_selected_route_executor_replaces_default_source_extract() -> None:
    source = _CountingSource()
    executor = _Executor(route_id="object_storage_pull")
    processor = ETLProcessor(
        source,
        _Sink(),
        etl_logger=_Logger(),
        route_capability_orchestrator=RouteCapabilityOrchestrator(
            candidate_provider=StaticRouteCandidateProvider((_fast_candidate(),)),
            probe_runner=StaticCapabilityProbeRunner(
                {
                    "storage.object.prefix_access": CapabilityEvidence.success(
                        requirement_id="storage.object.prefix_access",
                        domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                    )
                }
            ),
            publisher=RuntimeRouteDecisionPublisher(audit_storage=InMemoryLoadStepAuditStorage()),
            executors=(executor,),
        ),
    )

    result = processor.run(_cfg(runtime_capabilities={"mode": "auto"}))

    assert source.extract_calls == 0
    assert executor.calls == 1
    assert result["extracted_rows"] == 7


def _orchestrator(
    *candidates: RouteCandidate,
    evidence: dict[str, CapabilityEvidence],
    audit_storage: InMemoryLoadStepAuditStorage,
) -> RouteCapabilityOrchestrator:
    return RouteCapabilityOrchestrator(
        candidate_provider=StaticRouteCandidateProvider(candidates),
        probe_runner=StaticCapabilityProbeRunner(evidence),
        publisher=RuntimeRouteDecisionPublisher(audit_storage=audit_storage),
    )


def _fast_candidate() -> RouteCandidate:
    return RouteCandidate(
        route_id="object_storage_pull",
        requirements=(
            CapabilityRequirement(
                id="storage.object.prefix_access",
                domain=RuntimeCapability.INTERMEDIATE_STORAGE,
                required_for="object_storage_pull",
                blocker_code="storage.runtime_put_denied",
            ),
        ),
        priority=10,
    )


def _streaming_candidate() -> RouteCandidate:
    return RouteCandidate(
        route_id="typed_binary_streaming",
        requirements=(
            CapabilityRequirement(
                id="transport.push_stream",
                domain=RuntimeCapability.TRANSPORT,
                required_for="typed_binary_streaming",
            ),
        ),
        priority=20,
    )


def _cfg(*, runtime_capabilities: dict[str, object]) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"runtime": {"capabilities": runtime_capabilities}, "lineage": False},
    )


class _CountingSource:
    connector = object()

    def __init__(self) -> None:
        self.state_calls = 0
        self.extract_calls = 0

    def get_incremental_state(self, load_config):
        del load_config
        self.state_calls += 1
        return None

    def extract(self, load_config, state):
        del load_config, state
        self.extract_calls += 1
        return ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "int")])


class _Sink:
    connector = object()

    def load(self, load_config, payload):
        del load_config
        rows = getattr(payload.artifact, "estimated_rows", None) or 1
        return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows, staging_rows=rows)


class _Executor:
    def __init__(self, *, route_id: str) -> None:
        self.route_id = route_id
        self.calls = 0

    def extract(self, *, load_config, source, sink, state, load_record):
        del load_config, source, sink, state, load_record
        self.calls += 1
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": index} for index in range(7)]),
            schema=[("id", "int")],
        )


class _Logger:
    def __init__(self) -> None:
        self.progress: list[tuple[str, dict[str, object]]] = []

    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload

    def info(self, message):
        del message

    def warning(self, message):
        del message
