"""Runtime route capability orchestration and audit publishing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol

from dpone.runtime.route_capabilities import (
    CapabilityEvidence,
    RouteCandidate,
    RouteCapabilityDecision,
    RouteCapabilityPlanner,
)
from dpone.runtime.route_runtime_models import LoadStepAuditRecord

DECISION_SCHEMA_VERSION = "dpone.runtime.route_capability_decision.v1"
PROBE_SCHEMA_VERSION = "dpone.runtime.capability_probe.v1"
EXECUTION_SCHEMA_VERSION = "dpone.runtime.route_execution.v1"
SECRET_KEYS = ("password", "secret", "token", "credential", "access_key", "presigned")


class RouteCapabilityBlocked(RuntimeError):
    """Raised when route requirements block source IO."""

    def __init__(self, decision: RouteCapabilityDecision) -> None:
        self.decision = decision
        super().__init__(", ".join(decision.blockers or ("route_capability_blocked",)))


class LoadStepAuditStorage(Protocol):
    def record_load_step(self, record: LoadStepAuditRecord) -> None:
        """Persist one load-step audit record."""


class RouteCandidateProvider(Protocol):
    def candidates(self, *, load_config: Any, source: Any, sink: Any) -> Sequence[RouteCandidate]:
        """Return route candidates for the current runtime context."""


class CapabilityProbeRunner(Protocol):
    def probe(
        self,
        *,
        candidates: Sequence[RouteCandidate],
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any | None = None,
    ) -> Mapping[str, CapabilityEvidence]:
        """Return capability evidence keyed by requirement id."""


class SelectedRouteExecutor(Protocol):
    route_id: str

    def extract(self, *, load_config: Any, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        """Extract through the selected route."""


class InMemoryLoadStepAuditStorage:
    def __init__(self) -> None:
        self.records: list[LoadStepAuditRecord] = []

    def record_load_step(self, record: LoadStepAuditRecord) -> None:
        self.records.append(record)


class StaticRouteCandidateProvider:
    def __init__(self, candidates: Sequence[RouteCandidate]) -> None:
        self._candidates = tuple(candidates)

    def candidates(self, *, load_config: Any, source: Any, sink: Any) -> Sequence[RouteCandidate]:
        del load_config, source, sink
        return self._candidates


class StaticCapabilityProbeRunner:
    def __init__(self, evidence: Mapping[str, CapabilityEvidence]) -> None:
        self._evidence = dict(evidence)

    def probe(
        self,
        *,
        candidates: Sequence[RouteCandidate],
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any | None = None,
    ) -> Mapping[str, CapabilityEvidence]:
        del candidates, load_config, source, sink, load_record
        return dict(self._evidence)


class RuntimeRouteDecisionPublisher:
    def __init__(self, *, audit_storage: LoadStepAuditStorage | None = None, logger: Any | None = None) -> None:
        self.audit_storage = audit_storage
        self.logger = logger
        self.records: list[dict[str, object]] = []

    def publish(self, *, decision: RouteCapabilityDecision, load_record: Any) -> dict[str, object]:
        payload = _decision_payload(decision)
        redacted = _redact(payload)
        self.records.append(redacted)
        _log_decision(self.logger, redacted)
        if self.audit_storage is not None:
            now = _utc_now()
            self.audit_storage.record_load_step(
                LoadStepAuditRecord(
                    run_id=str(load_record.run_id),
                    load_id=str(load_record.load_id),
                    step_id="route_capability_decision",
                    phase="pre_extract",
                    kind="runtime_route_decision",
                    status=_decision_status(decision),
                    started_at=now,
                    finished_at=now,
                    details_json=redacted,
                )
            )
        return redacted

    def summary(self) -> dict[str, object] | None:
        if not self.records:
            return None
        item = self.records[-1]
        return {
            "schema_version": DECISION_SCHEMA_VERSION,
            "selected_route_id": item.get("selected_route_id"),
            "fallback_reason": item.get("fallback_reason"),
            "warnings": item.get("warnings", []),
            "blockers": item.get("blockers", []),
            "recommendations": item.get("recommendations", []),
        }


@dataclass(frozen=True, slots=True)
class RouteRuntimeContext:
    decision: RouteCapabilityDecision
    evidence: dict[str, object]
    executor: SelectedRouteExecutor | None = None

    def extract(self, *, load_config: Any, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        if self.executor is None:
            return source.extract(load_config, state)
        return self.executor.extract(
            load_config=load_config,
            source=source,
            sink=sink,
            state=state,
            load_record=load_record,
        )


class RouteCapabilityOrchestrator:
    def __init__(
        self,
        *,
        candidate_provider: RouteCandidateProvider,
        probe_runner: CapabilityProbeRunner,
        publisher: RuntimeRouteDecisionPublisher,
        planner: RouteCapabilityPlanner | None = None,
        executors: Sequence[SelectedRouteExecutor] = (),
        decision_details_provider: Any | None = None,
    ) -> None:
        self.candidate_provider = candidate_provider
        self.probe_runner = probe_runner
        self.publisher = publisher
        self.planner = planner or RouteCapabilityPlanner()
        self.executors = {executor.route_id: executor for executor in executors}
        self.decision_details_provider = decision_details_provider

    def prepare(self, *, load_config: Any, source: Any, sink: Any, load_record: Any) -> RouteRuntimeContext:
        candidates = tuple(self.candidate_provider.candidates(load_config=load_config, source=source, sink=sink))
        evidence = self.probe_runner.probe(
            candidates=candidates,
            load_config=load_config,
            source=source,
            sink=sink,
            load_record=load_record,
        )
        policy = _capability_policy(load_config)
        mode = policy["mode"] or "auto"
        requested_route_id = policy.get("requested_route_id")
        decision = self.planner.decide(
            candidates=candidates,
            evidence=evidence,
            mode=mode,
            requested_route_id=requested_route_id,
        )
        decision = _with_context_details(
            decision,
            provider=self.decision_details_provider,
            load_config=load_config,
            source=source,
            sink=sink,
            load_record=load_record,
        )
        published = self.publisher.publish(decision=decision, load_record=load_record)
        if not decision.should_start_source_io:
            raise RouteCapabilityBlocked(decision)
        return RouteRuntimeContext(
            decision=decision,
            evidence=published,
            executor=self.executors.get(decision.selected_route_id),
        )

    def summary(self) -> dict[str, object] | None:
        return self.publisher.summary()


def _capability_policy(load_config: Any) -> dict[str, str | None]:
    options = getattr(load_config, "options", {}) or {}
    runtime = options.get("runtime") if isinstance(options, dict) else {}
    capabilities = runtime.get("capabilities") if isinstance(runtime, dict) else {}
    if not isinstance(capabilities, dict):
        capabilities = {}
    requested_route_id = capabilities.get("requested_route_id")
    return {
        "mode": str(capabilities.get("mode") or "auto"),
        "requested_route_id": str(requested_route_id) if requested_route_id else None,
    }


def _with_context_details(
    decision: RouteCapabilityDecision,
    *,
    provider: Any | None,
    load_config: Any,
    source: Any,
    sink: Any,
    load_record: Any,
) -> RouteCapabilityDecision:
    if provider is None:
        return decision
    details = provider(load_config=load_config, source=source, sink=sink, load_record=load_record)
    if not details:
        return decision
    return replace(decision, details={**decision.details, **dict(details)})


def _decision_payload(decision: RouteCapabilityDecision) -> dict[str, object]:
    payload = decision.to_evidence()
    payload["schema_version"] = DECISION_SCHEMA_VERSION
    payload["probe_schema_version"] = PROBE_SCHEMA_VERSION
    payload["execution_schema_version"] = EXECUTION_SCHEMA_VERSION
    return payload


def _decision_status(decision: RouteCapabilityDecision) -> str:
    if not decision.should_start_source_io:
        return "blocked"
    if decision.fallback_reason or decision.warnings:
        return "warning"
    return "succeeded"


def _log_decision(logger: Any | None, payload: dict[str, object]) -> None:
    if logger is None or not hasattr(logger, "log_etl_progress"):
        return
    logger.log_etl_progress("ROUTE_CAPABILITY_DECISION", payload)


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "***REDACTED***" if _is_secret_key(str(key)) else _redact(item) for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_redact(item) for item in value]
    return value


def _is_secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in SECRET_KEYS)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017 - repository mypy target lacks datetime.UTC.
