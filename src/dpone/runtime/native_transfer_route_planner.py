from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_route_models import (
    FALLBACK_CHAIN,
    RouteCertificationPolicy,
    RouteTransportCandidate,
    RouteTransportDecision,
)
from dpone.runtime.native_transfer_route_registry import NativeTransferRouteCapabilityRegistry


class NativeTransferRoutePlanner:
    """Pure route-level transport decision service."""

    def __init__(self, registry: NativeTransferRouteCapabilityRegistry | None = None) -> None:
        self._registry = registry or NativeTransferRouteCapabilityRegistry()

    def plan(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        source_options: Mapping[str, Any],
        sink_options: Mapping[str, Any],
        execution_policy: NativeTransferExecutionPolicy,
        certification_policy: RouteCertificationPolicy,
        runner_policy: str | None = None,
    ) -> RouteTransportDecision:
        requested = execution_policy.transport.mode.strip().lower() or "auto"
        effective_certification = certification_policy.effective_mode(runner_policy=runner_policy)
        matrix = self._registry.build_matrix(
            source_type=source_type,
            sink_type=sink_type,
            strategy=strategy,
            source_options=source_options,
            sink_options=sink_options,
            requested_transport=requested,
            certification_policy=certification_policy,
        )
        selected, blockers = _select_candidate(
            candidates=matrix.candidates,
            requested=requested,
            certification_mode=effective_certification,
            artifact_status=matrix.certification_artifact_status,
        )
        warnings = _warnings(selected=selected, blockers=blockers, certification_mode=effective_certification)
        return RouteTransportDecision(
            matrix=matrix,
            requested_transport=requested,
            selected_transport=selected.transport if selected else None,
            certification_mode=effective_certification,
            certification_status=_certification_status(selected, blockers),
            release_gate=_release_gate(selected=selected, blockers=blockers, warnings=warnings),
            fallback_chain=FALLBACK_CHAIN,
            blockers=blockers,
            warnings=warnings,
            reasons=_reasons(matrix.candidates, selected),
        )


def _select_candidate(
    *,
    candidates: tuple[RouteTransportCandidate, ...],
    requested: str,
    certification_mode: str,
    artifact_status: str,
) -> tuple[RouteTransportCandidate | None, tuple[str, ...]]:
    if requested != "auto":
        candidate = next((item for item in candidates if item.transport == requested), None)
        if candidate is None or not candidate.technically_eligible:
            return None, (f"native_transfer_forced_transport_unavailable:{requested}",)
        if certification_mode == "certified_only" and not candidate.certified:
            return None, _certification_blockers(artifact_status, requested)
        return candidate, tuple()

    eligible = tuple(item for item in candidates if item.technically_eligible)
    if certification_mode == "certified_only":
        certified = next((item for item in eligible if item.certified), None)
        if certified is not None:
            return certified, tuple()
        return None, _certification_blockers(artifact_status, "auto")
    return (eligible[0], tuple()) if eligible else (None, ("native_transfer_route_no_eligible_transport",))


def _certification_blockers(artifact_status: str, transport: str) -> tuple[str, ...]:
    if artifact_status in {"missing", "not_configured"}:
        return ("native_transfer_route_certification.missing",)
    if artifact_status != "present":
        return (f"native_transfer_route_certification.{artifact_status}",)
    return (f"native_transfer_route_certification.not_certified:{transport}",)


def _warnings(
    *,
    selected: RouteTransportCandidate | None,
    blockers: tuple[str, ...],
    certification_mode: str,
) -> tuple[str, ...]:
    if blockers or selected is None or certification_mode == "certified_only" or selected.certified:
        return tuple()
    return ("native_transfer_route_uncertified_advisory",)


def _certification_status(selected: RouteTransportCandidate | None, blockers: tuple[str, ...]) -> str:
    if blockers or selected is None:
        return "blocked"
    return "certified" if selected.certified else "uncertified"


def _release_gate(
    *,
    selected: RouteTransportCandidate | None,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> str:
    if blockers or selected is None:
        return "blocked"
    if warnings:
        return "warning"
    return "green"


def _reasons(
    candidates: tuple[RouteTransportCandidate, ...],
    selected: RouteTransportCandidate | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    for candidate in candidates:
        if selected is not None and candidate.transport == selected.transport and not candidate.reasons:
            break
        reasons.extend(candidate.reasons)
        if selected is not None and candidate.transport == selected.transport:
            break
    return tuple(dict.fromkeys(reason for reason in reasons if reason))


__all__ = [
    "NativeTransferRouteCapabilityRegistry",
    "NativeTransferRoutePlanner",
    "RouteCertificationPolicy",
]
