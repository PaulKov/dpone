"""Adapter from canonical self-service capabilities to dbt publish decisions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.readiness.capability_discovery_protocols import (
    RouteCapabilityProtocol,
)


class DbtCapabilitySnapshotPort(Protocol):
    """Minimal capability view required by dbt route selection."""

    @property
    def snapshot_id(self) -> str: ...

    @property
    def routes(self) -> Sequence[RouteCapabilityProtocol]: ...


class DbtRouteCapabilityPolicy:
    """Resolve routes through the same capability projection as CLI and Studio."""

    def __init__(
        self,
        snapshot: DbtCapabilitySnapshotPort,
        *,
        require_certified: bool,
    ) -> None:
        self._snapshot = snapshot
        self._require_certified = require_certified

    def resolve(
        self,
        *,
        source: str,
        sink: str,
        strategy: str,
        transport: str | None,
        schema_evolution: str | None,
        airflow_runtime_mode: str | None,
        path: str,
    ) -> tuple[dict[str, Any] | None, tuple[DbtPublishIssue, ...]]:
        route_id = f"{source.lower()}:{sink.lower()}:{strategy.lower()}"
        route = next((item for item in self._snapshot.routes if item.id == route_id), None)
        if route is None or route.support.status == "not_supported":
            return None, (
                DbtPublishIssue(
                    code="DPONE_DBT_ROUTE_NOT_SUPPORTED",
                    message=f"Route {route_id} is not supported by the canonical capability catalog",
                    path=path,
                    remediation="Choose a supported strategy or install/certify the required connector route.",
                ),
            )
        coordinates = (transport, schema_evolution, airflow_runtime_mode)
        configured = all(isinstance(item, str) and item for item in coordinates)
        variants = (
            tuple(
                item
                for item in route.certification.variants
                if (
                    item.transport,
                    item.schema_evolution,
                    item.airflow_runtime_mode,
                )
                == coordinates
            )
            if configured
            else ()
        )
        if len(variants) > 1:
            return None, (
                DbtPublishIssue(
                    code="DPONE_DBT_ROUTE_CERTIFICATION_AMBIGUOUS",
                    message=f"Route {route_id} has multiple certification variants for the requested dimensions",
                    path=path,
                    remediation="Remove duplicate route-certification variants from the platform evidence catalog.",
                ),
            )
        variant = variants[0] if variants else None
        requested_variant_id = (
            "|".join((route.id, *coordinates))
            if configured and all(isinstance(item, str) for item in coordinates)
            else None
        )
        capability: dict[str, Any] = {
            "snapshot_id": self._snapshot.snapshot_id,
            "route_id": route.id,
            "support": route.support.status,
            "variant_id": variant.id if variant else requested_variant_id,
            "transport": transport,
            "schema_evolution": schema_evolution,
            "airflow_runtime_mode": airflow_runtime_mode,
            "certification_level": variant.level if variant else "experimental",
            "evidence_status": variant.evidence_status if variant else "UNVERIFIED",
            "evidence_refs": list(variant.evidence_refs) if variant else [],
            "evidence_reason_codes": (
                list(variant.reason_codes)
                if variant
                else [
                    (
                        "dbt_route_certification_variant_unavailable"
                        if configured
                        else "dbt_route_certification_dimensions_missing"
                    )
                ]
            ),
        }
        production_certified = (
            variant is not None
            and variant.evidence_status == "PASS"
            and bool(variant.evidence_refs)
            and variant.level
            in {
                "production-certified",
                "enterprise-certified",
            }
        )
        if self._require_certified and not production_certified:
            detail = (
                "does not define exact certification dimensions"
                if not configured
                else "lacks current production-certified evidence for the requested variant"
            )
            return None, (
                DbtPublishIssue(
                    code="DPONE_DBT_ROUTE_NOT_CERTIFIED",
                    message=f"Route {route_id} {detail}",
                    path=path,
                    remediation=(
                        "Configure transport, schema_evolution and airflow_runtime_mode in the "
                        "platform-owned profile and attach current evidence for that exact variant."
                    ),
                ),
            )
        return capability, ()


__all__ = ["DbtRouteCapabilityPolicy"]
