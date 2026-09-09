"""Capability boundary for authorizing physical Semantic Refresh activation."""

from __future__ import annotations

from typing import NoReturn, Protocol


class SemanticRefreshProductionActivationGuardPort(Protocol):
    """Prove that every prerequisite for physical production activation exists."""

    def authorize(
        self,
        *,
        subject: object,
        plan_bundle: object,
    ) -> None: ...


class SemanticRefreshProductionActivationUnavailableError(RuntimeError):
    """Stable fail-closed result for the 0.74 local diagnostic preview."""

    code = "DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE"


class UnavailableSemanticRefreshProductionActivationGuard:
    """Default guard while no certified exact-UUID retention controller ships."""

    def authorize(self, **_: object) -> NoReturn:
        raise SemanticRefreshProductionActivationUnavailableError(
            f"{SemanticRefreshProductionActivationUnavailableError.code}: "
            "dpone 0.74 is a local diagnostic preview and has no certified "
            "exact-UUID predecessor-retention controller"
        )


__all__ = [
    "SemanticRefreshProductionActivationGuardPort",
    "SemanticRefreshProductionActivationUnavailableError",
    "UnavailableSemanticRefreshProductionActivationGuard",
]
