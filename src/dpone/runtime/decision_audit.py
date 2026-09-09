"""Connector-neutral runtime decision audit and fallback observability."""

from __future__ import annotations

import contextlib
import contextvars
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

RUNTIME_DECISION_AUDIT_SCHEMA_VERSION = "dpone.runtime.decision_audit.v1"

_CURRENT_PUBLISHER: contextvars.ContextVar[RuntimeDecisionPublisher | None] = contextvars.ContextVar(
    "dpone_runtime_decision_publisher",
    default=None,
)
_SECRET_KEY_RE = re.compile(r"(password|passwd|pwd|token|secret|credential|authorization|api[_-]?key|pat)", re.I)
_TOP_LEVEL_KEYS = {
    "schema_version",
    "decision_id",
    "phase",
    "component",
    "category",
    "requested",
    "requested_backend",
    "requested_mode",
    "requested_transport",
    "selected",
    "selected_backend",
    "selected_provider",
    "selected_route",
    "selected_scan",
    "selected_transport",
    "transport",
    "fallback_allowed",
    "fallback_reason",
    "release_gate",
    "warnings",
    "warning_codes",
    "blockers",
    "blocker_codes",
    "route_id",
    "provider",
    "provider_version",
    "accelerator_version",
}


@dataclass(frozen=True, slots=True)
class DecisionAuditPolicy:
    """User-facing policy for runtime decision observability."""

    enabled: bool = True
    persist: bool = True
    log_level: str = "warning_on_fallback"
    include_successful_decisions: bool = True

    @classmethod
    def from_load_config(cls, load_config: Any) -> DecisionAuditPolicy:
        options = getattr(load_config, "options", {}) or {}
        governance = options.get("load_governance") if isinstance(options, Mapping) else None
        decision_audit = governance.get("decision_audit") if isinstance(governance, Mapping) else None
        raw = decision_audit if isinstance(decision_audit, Mapping) else {}
        return cls(
            enabled=_bool(raw.get("enabled"), True),
            persist=_bool(raw.get("persist"), True),
            log_level=_log_level(raw.get("log_level")),
            include_successful_decisions=_bool(raw.get("include_successful_decisions"), True),
        )


@dataclass(frozen=True, slots=True)
class RuntimeDecision:
    """Normalized runtime decision emitted by any auto/fallback mechanism."""

    decision_id: str
    phase: str
    component: str
    category: str
    requested: str | None
    selected: str | None
    fallback_allowed: bool
    fallback_reason: str | None = None
    release_gate: str = "green"
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    route_id: str | None = None
    provider: str | None = None
    provider_version: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    @property
    def should_warn(self) -> bool:
        return bool(self.fallback_reason or self.blockers or self.release_gate in {"warning", "blocked"})

    def redacted(self) -> RuntimeDecision:
        return replace(self, details=_redact(self.details))

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self.redacted())
        payload["schema_version"] = RUNTIME_DECISION_AUDIT_SCHEMA_VERSION
        payload["warnings"] = list(self.warnings)
        payload["blockers"] = list(self.blockers)
        payload["details"] = _redact(self.details)
        return payload


class RuntimeDecisionPublisher(Protocol):
    """Output port for decision audit records."""

    def publish(self, decision: RuntimeDecision) -> None: ...


@dataclass(slots=True)
class RuntimeDecisionSummary:
    """In-memory compact summary returned by ``dpone run --format json``."""

    decisions: list[dict[str, Any]] = field(default_factory=list)

    def record(self, decision: RuntimeDecision) -> None:
        payload = decision.to_jsonable()
        self.decisions.append(
            {
                "decision_id": payload["decision_id"],
                "phase": payload["phase"],
                "component": payload["component"],
                "category": payload["category"],
                "requested": payload["requested"],
                "selected": payload["selected"],
                "fallback_reason": payload["fallback_reason"],
                "release_gate": payload["release_gate"],
                "warnings": payload["warnings"],
                "blockers": payload["blockers"],
            }
        )

    def to_jsonable(self) -> dict[str, Any]:
        gates = {"green": 0, "warning": 0, "blocked": 0}
        for item in self.decisions:
            gate = str(item.get("release_gate") or "green")
            gates[gate] = gates.get(gate, 0) + 1
        return {
            "schema_version": RUNTIME_DECISION_AUDIT_SCHEMA_VERSION,
            "total": len(self.decisions),
            "by_release_gate": gates,
            "decisions": list(self.decisions),
        }


class CompositeDecisionPublisher:
    """Publish a runtime decision to logs, durable audit and run summary."""

    def __init__(
        self,
        *,
        policy: DecisionAuditPolicy,
        governance_service: Any,
        load_record: Any,
        logger: Any | None,
        summary: RuntimeDecisionSummary,
    ) -> None:
        self._policy = policy
        self._governance_service = governance_service
        self._load_record = load_record
        self._logger = logger
        self._summary = summary

    def publish(self, decision: RuntimeDecision) -> None:
        if not self._policy.enabled:
            return
        redacted = decision.redacted()
        if not self._policy.include_successful_decisions and redacted.release_gate == "green":
            return
        self._summary.record(redacted)
        self._log(redacted)
        if self._policy.persist:
            self._governance_service.record_load_step(
                load_record=self._load_record,
                step_id=redacted.decision_id,
                phase=redacted.phase,
                kind="runtime_decision",
                status="failed" if redacted.release_gate == "blocked" else "succeeded",
                details=redacted.to_jsonable(),
            )

    def _log(self, decision: RuntimeDecision) -> None:
        if self._logger is None or (self._policy.log_level == "error_only" and decision.release_gate != "blocked"):
            return
        if self._policy.log_level == "warning_on_fallback" and not decision.should_warn:
            return
        line = _log_line(decision)
        if decision.should_warn and hasattr(self._logger, "warning"):
            self._logger.warning(line)
        elif hasattr(self._logger, "info"):
            self._logger.info(line)


class RuntimeDecisionContext:
    """AOP-style active decision publisher for runtime branches."""

    @staticmethod
    @contextlib.contextmanager
    def activate(publisher: RuntimeDecisionPublisher) -> Iterator[None]:
        token = _CURRENT_PUBLISHER.set(publisher)
        try:
            yield
        finally:
            _CURRENT_PUBLISHER.reset(token)


def publish_runtime_decision(
    decision: Any,
    *,
    decision_id: str,
    phase: str,
    component: str,
    category: str,
    fallback_allowed: bool | None = None,
    details: Mapping[str, Any] | None = None,
    route_id: str | None = None,
    provider: str | None = None,
) -> RuntimeDecision | None:
    """Normalize and publish a runtime decision when a context is active."""

    normalized = normalize_runtime_decision(
        decision,
        decision_id=decision_id,
        phase=phase,
        component=component,
        category=category,
        fallback_allowed=fallback_allowed,
        details=details,
        route_id=route_id,
        provider=provider,
    )
    publisher = _CURRENT_PUBLISHER.get()
    if publisher is not None:
        publisher.publish(normalized)
    return normalized


def normalize_runtime_decision(
    decision: Any,
    *,
    decision_id: str,
    phase: str,
    component: str,
    category: str,
    fallback_allowed: bool | None = None,
    details: Mapping[str, Any] | None = None,
    route_id: str | None = None,
    provider: str | None = None,
) -> RuntimeDecision:
    payload = _decision_payload(decision)
    warnings = _strings(payload.get("warnings") or payload.get("warning_codes"))
    blockers = _strings(payload.get("blockers") or payload.get("blocker_codes"))
    fallback_reason = _optional_text(payload.get("fallback_reason"))
    return RuntimeDecision(
        decision_id=decision_id,
        phase=phase,
        component=component,
        category=category,
        requested=_optional_text(
            _first(payload, "requested", "requested_backend", "requested_mode", "requested_transport")
        ),
        selected=_optional_text(
            _first(
                payload,
                "selected",
                "selected_backend",
                "selected_provider",
                "selected_route",
                "selected_scan",
                "selected_transport",
                "transport",
            )
        ),
        fallback_allowed=_bool(fallback_allowed, bool(fallback_reason and not blockers)),
        fallback_reason=fallback_reason,
        release_gate=_release_gate(payload, warnings, blockers),
        warnings=warnings,
        blockers=blockers,
        route_id=route_id or _optional_text(payload.get("route_id") or payload.get("selected_route")),
        provider=provider or _optional_text(payload.get("provider")),
        provider_version=_optional_text(payload.get("provider_version") or payload.get("accelerator_version")),
        details={**_detail_payload(payload), **dict(details or {})},
    )


def _decision_payload(decision: Any) -> dict[str, Any]:
    if isinstance(decision, Mapping):
        return dict(decision)
    for method in ("to_evidence", "to_dict"):
        candidate = getattr(decision, method, None)
        if callable(candidate):
            raw = candidate()
            return dict(raw) if isinstance(raw, Mapping) else {}
    if hasattr(decision, "__dataclass_fields__"):
        return asdict(decision)
    return {}


def _detail_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in payload.items() if str(key) not in _TOP_LEVEL_KEYS}


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _release_gate(payload: Mapping[str, Any], warnings: Sequence[str], blockers: Sequence[str]) -> str:
    explicit = str(payload.get("release_gate") or "").strip().lower()
    if explicit:
        return explicit
    if blockers:
        return "blocked"
    if warnings or payload.get("fallback_reason"):
        return "warning"
    return "green"


def _log_line(decision: RuntimeDecision) -> str:
    return (
        "event=dpone.runtime_decision "
        f"decision_id={decision.decision_id} phase={decision.phase} component={decision.component} "
        f"category={decision.category} requested={decision.requested} selected={decision.selected} "
        f"release_gate={decision.release_gate} fallback_reason={decision.fallback_reason}"
    )


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: "***" if _SECRET_KEY_RE.search(str(key)) else _redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item) for item in value)
    if isinstance(value, str):
        return _redact_url(value)
    return value


def _redact_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or "@" not in parts.netloc:
        return value
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port is not None else ""
    return urlunsplit((parts.scheme, f"***:***@{host}{port}", parts.path, parts.query, parts.fragment))


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value if str(item))
    return (str(value),) if value else ()


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _log_level(value: Any) -> str:
    normalized = str(value or "warning_on_fallback").strip().lower()
    return normalized if normalized in {"always", "warning_on_fallback", "error_only"} else "warning_on_fallback"


__all__ = [
    "RUNTIME_DECISION_AUDIT_SCHEMA_VERSION",
    "CompositeDecisionPublisher",
    "DecisionAuditPolicy",
    "RuntimeDecision",
    "RuntimeDecisionContext",
    "RuntimeDecisionPublisher",
    "RuntimeDecisionSummary",
    "normalize_runtime_decision",
    "publish_runtime_decision",
]
