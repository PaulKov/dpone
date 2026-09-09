"""Static route candidates for self-service sample planning.

Catalog membership is credential-free metadata, not production authorization.
Production policy also requires trusted external evidence to be verified by the
composition root and injected into ``SourceSamplingCapabilityDetector``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CertifiedSamplingRoute:
    certification_id: str
    status: str
    source: str
    sink: str
    strategy: str
    transport: str
    schema_evolution: str
    airflow_runtime_mode: str
    sampling_mode: str
    estimated_read_bytes: int

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.source, self.sink, self.strategy)

    @property
    def proof(self) -> str:
        return f"route_certification:{self.certification_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "certification_id": self.certification_id,
            "status": self.status,
            "source": self.source,
            "sink": self.sink,
            "strategy": self.strategy,
            "transport": self.transport,
            "schema_evolution": self.schema_evolution,
            "airflow_runtime_mode": self.airflow_runtime_mode,
            "sampling_mode": self.sampling_mode,
            "estimated_read_bytes": self.estimated_read_bytes,
            "proof": self.proof,
        }


_CATALOG: tuple[CertifiedSamplingRoute, ...] = (
    CertifiedSamplingRoute(
        certification_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        status="experimental",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        sampling_mode="pushdown",
        estimated_read_bytes=1024 * 1024,
    ),
)


def certified_sampling_route_catalog() -> tuple[CertifiedSamplingRoute, ...]:
    return _CATALOG


def certified_sampling_route_index() -> dict[tuple[str, str, str], CertifiedSamplingRoute]:
    return {route.key: route for route in _CATALOG}


def certified_sampling_routes() -> tuple[dict[str, Any], ...]:
    return tuple(route.to_dict() for route in _CATALOG)


__all__ = [
    "CertifiedSamplingRoute",
    "certified_sampling_route_catalog",
    "certified_sampling_route_index",
    "certified_sampling_routes",
]
