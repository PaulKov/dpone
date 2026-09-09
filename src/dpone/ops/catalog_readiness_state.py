"""Route execution ledger and state promotion service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.route_state_promotion import RouteStatePromotionService
from dpone.ops.routes.execution_store_factory import LOCAL_JSON_BACKEND, RouteExecutionLedgerStoreFactory
from dpone.ops.routes.state_store_factory import LOCAL_JSON_BACKEND as STATE_LOCAL_JSON_BACKEND
from dpone.ops.routes.state_store_factory import RouteStateStoreFactory


@dataclass(frozen=True, slots=True)
class ReadinessStateCatalog:
    """Factories for route execution ledger and state promotion services."""

    @classmethod
    def default(cls) -> ReadinessStateCatalog:
        return cls()

    def route_execution_ledger(
        self,
        *,
        store_backend: str = LOCAL_JSON_BACKEND,
        store_uri: str | None = None,
    ) -> RouteExecutionService:
        return RouteExecutionService(
            store=RouteExecutionLedgerStoreFactory.build(backend=store_backend, store_uri=store_uri)
        )

    def route_state_promotion(
        self,
        *,
        state_backend: str = STATE_LOCAL_JSON_BACKEND,
        state_uri: str | None = None,
    ) -> RouteStatePromotionService:
        return RouteStatePromotionService(
            state_store=RouteStateStoreFactory.build(backend=state_backend, store_uri=state_uri)
        )


__all__ = ["ReadinessStateCatalog"]
