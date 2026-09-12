"""Shared R1 route-to-profile selection for planning and runtime admission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.readiness.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessActivation,
    PostgresMssqlCorrectnessProfileDecision,
    PostgresMssqlCorrectnessProfileResolver,
    PostgresMssqlCorrectnessProfileSelector,
    PostgresMssqlCorrectnessRouteRequest,
    build_postgres_mssql_correctness_route_request,
)
from dpone.readiness.resolved_process_route import resolve_process_route

if TYPE_CHECKING:
    from dpone.manifest.models import ProcessSpec


class PostgresMssqlCorrectnessRouteResolver:
    """Resolve only routes explicitly registered by the platform for R1."""

    def __init__(
        self,
        *,
        selector: PostgresMssqlCorrectnessProfileSelector,
        resolver: PostgresMssqlCorrectnessProfileResolver,
    ) -> None:
        self._selector = selector
        self._resolver = resolver

    def resolve(
        self,
        request: PostgresMssqlCorrectnessRouteRequest,
    ) -> PostgresMssqlCorrectnessProfileDecision | None:
        """Return ``None`` for legacy compatibility or one exact R1 decision."""

        profile_id = self._selector.select_profile_id(request)
        if profile_id is None:
            return None
        return self._resolver.resolve_route(profile_id=profile_id, request=request)

    def require_activatable(
        self,
        request: PostgresMssqlCorrectnessRouteRequest,
    ) -> PostgresMssqlCorrectnessProfileDecision | None:
        """Require current evidence only when the platform selected R1."""

        profile_id = self._selector.select_profile_id(request)
        if profile_id is None:
            return None
        return self._resolver.require_route_activatable(profile_id=profile_id, request=request)

    def require_runtime_activatable(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        load_config: Any,
        raw_config: Mapping[str, Any],
    ) -> PostgresMssqlCorrectnessActivation | None:
        """Build and admit runtime coordinates through the same selector snapshot."""

        request = postgres_mssql_correctness_runtime_request(
            source_type=source_type,
            sink_type=sink_type,
            strategy=strategy,
            load_config=load_config,
            raw_config=raw_config,
        )
        if request is None:
            return None
        profile_id = self._selector.select_profile_id(request)
        if profile_id is None:
            return None
        return self._resolver.require_route_activation(profile_id=profile_id, request=request)


def postgres_mssql_correctness_request(spec: ProcessSpec) -> PostgresMssqlCorrectnessRouteRequest | None:
    """Project one eligible parsed process to exact platform-selection coordinates."""

    route = resolve_process_route(spec)
    return postgres_mssql_correctness_runtime_request(
        source_type=route.source,
        sink_type=route.sink,
        strategy=route.strategy,
        load_config=spec.config.load_config,
        raw_config=spec.raw_config,
    )


def postgres_mssql_correctness_runtime_request(
    *,
    source_type: str,
    sink_type: str,
    strategy: str,
    load_config: Any,
    raw_config: Mapping[str, Any],
) -> PostgresMssqlCorrectnessRouteRequest | None:
    """Project runtime inputs through the same exact R1 selector coordinates."""

    return build_postgres_mssql_correctness_route_request(
        source_type=source_type,
        sink_type=sink_type,
        strategy=strategy,
        load_config=load_config,
        raw_config=raw_config,
    )


def postgres_mssql_correctness_projection(
    decision: PostgresMssqlCorrectnessProfileDecision | None,
) -> dict[str, object]:
    """Return the redacted deterministic plan/validation projection."""

    if decision is None:
        return {"selected": False, "execution_profile": "compatibility"}
    return {
        "selected": True,
        "mutates": False,
        "profile_id": decision.profile_id,
        "capability_id": decision.capability_id,
        "source_mode": decision.source_mode.value,
        "target_topology": "standalone_same_database",
        "authority_contract": "mssql_effect_receipt_v2",
        "implementation_status": decision.implementation_status,
        "certification_status": decision.certification_status,
        "activation_status": decision.activation_status,
        "blockers": list(decision.blockers),
        "decision_sha256": decision.canonical_sha256,
    }


def postgres_mssql_correctness_plan(
    spec: ProcessSpec,
    resolver: PostgresMssqlCorrectnessRouteResolver | None,
    *,
    source_type: str,
    sink_type: str,
) -> dict[str, object] | None:
    """Resolve the R1 plan section without adding policy to the planner."""

    if source_type != "postgres" or sink_type != "mssql":
        return None
    request = postgres_mssql_correctness_request(spec)
    if request is None or resolver is None:
        return postgres_mssql_correctness_projection(None)
    return postgres_mssql_correctness_projection(resolver.resolve(request))


__all__ = [
    "PostgresMssqlCorrectnessRouteResolver",
    "postgres_mssql_correctness_plan",
    "postgres_mssql_correctness_projection",
    "postgres_mssql_correctness_request",
    "postgres_mssql_correctness_runtime_request",
]
