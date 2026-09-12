"""Application-level resolver for the PostgreSQL to MSSQL R1 profile.

The resolver composes pure contracts with injected profile/evidence readers.
It deliberately has no runtime, connector, or vendor-SDK dependency so plan
and manifest validation can share the exact same decision.
"""

from __future__ import annotations

from dataclasses import fields

from dpone.contracts import ETLConfigurationError
from dpone.contracts.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessActivation,
    PostgresMssqlCorrectnessProfile,
    PostgresMssqlCorrectnessProfileDecision,
    PostgresMssqlCorrectnessRequirements,
    PostgresMssqlCorrectnessRouteRequest,
    build_postgres_mssql_correctness_route_request,
    decide_postgres_mssql_correctness_profile,
)
from dpone.ports.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessEvidencePort,
    PostgresMssqlCorrectnessProfileProvider,
    PostgresMssqlCorrectnessProfileSelector,
)

_EVIDENCE_FIELDS = frozenset({"implementation_status", "certification_status", "activation_status"})


class PostgresMssqlCorrectnessProfileResolutionError(ETLConfigurationError):
    """Stable fail-closed result for profile resolution or activation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class PostgresMssqlCorrectnessProfileResolver:
    """Resolve one semantic request against current environment authority."""

    def __init__(
        self,
        profiles: PostgresMssqlCorrectnessProfileProvider,
        evidence: PostgresMssqlCorrectnessEvidencePort,
    ) -> None:
        self._profiles = profiles
        self._evidence = evidence

    def resolve(
        self,
        *,
        profile_id: str,
        requirements: PostgresMssqlCorrectnessRequirements,
    ) -> PostgresMssqlCorrectnessProfileDecision:
        """Return an explainable non-mutating decision for validate and plan."""

        _configured, current = self._load_current(profile_id)
        return decide_postgres_mssql_correctness_profile(requirements, current)

    def resolve_route(
        self,
        *,
        profile_id: str,
        request: PostgresMssqlCorrectnessRouteRequest,
    ) -> PostgresMssqlCorrectnessProfileDecision:
        """Resolve one profile and verify its route binding from one snapshot."""

        configured, current = self._load_current(profile_id)
        if (
            configured.source_connection_ref != request.source_connection_ref
            or configured.sink_connection_ref != request.sink_connection_ref
            or request.source_mode not in configured.allowed_source_modes
        ):
            raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
        return decide_postgres_mssql_correctness_profile(request.requirements(), current)

    def require_activatable(
        self,
        *,
        profile_id: str,
        requirements: PostgresMssqlCorrectnessRequirements,
    ) -> PostgresMssqlCorrectnessProfileDecision:
        """Return the current exact tuple or raise before business-source I/O."""

        decision = self.resolve(profile_id=profile_id, requirements=requirements)
        _require_activatable_decision(decision)
        return decision

    def require_route_activatable(
        self,
        *,
        profile_id: str,
        request: PostgresMssqlCorrectnessRouteRequest,
    ) -> PostgresMssqlCorrectnessProfileDecision:
        """Require an exact route binding and current vendor-live evidence."""

        return self.require_route_activation(profile_id=profile_id, request=request).decision

    def require_route_activation(
        self,
        *,
        profile_id: str,
        request: PostgresMssqlCorrectnessRouteRequest,
    ) -> PostgresMssqlCorrectnessActivation:
        """Bind the trusted physical profile to its exact semantic decision."""

        configured, current = self._load_current(profile_id)
        if (
            configured.source_connection_ref != request.source_connection_ref
            or configured.sink_connection_ref != request.sink_connection_ref
            or request.source_mode not in configured.allowed_source_modes
        ):
            raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
        decision = decide_postgres_mssql_correctness_profile(request.requirements(), current)
        _require_activatable_decision(decision)
        return PostgresMssqlCorrectnessActivation(profile=current, decision=decision)

    def _load_current(
        self,
        profile_id: str,
    ) -> tuple[PostgresMssqlCorrectnessProfile, PostgresMssqlCorrectnessProfile]:
        canonical_profile_id = _profile_id(profile_id)
        configured = self._profiles.load(canonical_profile_id)
        if configured.profile_id != canonical_profile_id:
            raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
        current = self._evidence.bind_current_evidence(configured)
        _require_same_static_profile(configured, current)
        return configured, current


def _require_activatable_decision(decision: PostgresMssqlCorrectnessProfileDecision) -> None:
    if decision.blockers:
        raise PostgresMssqlCorrectnessProfileResolutionError(decision.blockers[0])
    if (
        decision.implementation_status != "implemented"
        or decision.certification_status != "vendor_pass"
        or decision.activation_status not in {"explicit_opt_in", "default"}
    ):
        raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")


def _profile_id(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_REQUIRED")
    if value != value.strip():
        raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_REQUIRED")
    return value


def _require_same_static_profile(
    configured: PostgresMssqlCorrectnessProfile,
    current: PostgresMssqlCorrectnessProfile,
) -> None:
    for item in fields(configured):
        if item.name in _EVIDENCE_FIELDS:
            continue
        if getattr(configured, item.name) != getattr(current, item.name):
            raise PostgresMssqlCorrectnessProfileResolutionError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")


__all__ = [
    "PostgresMssqlCorrectnessActivation",
    "PostgresMssqlCorrectnessProfileDecision",
    "PostgresMssqlCorrectnessProfileResolutionError",
    "PostgresMssqlCorrectnessProfileResolver",
    "PostgresMssqlCorrectnessProfileSelector",
    "PostgresMssqlCorrectnessRouteRequest",
    "build_postgres_mssql_correctness_route_request",
]
