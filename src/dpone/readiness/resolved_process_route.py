"""Canonical route identity for an already parsed manifest process."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.manifest.models import ProcessSpec


@dataclass(frozen=True, slots=True)
class ResolvedProcessRoute:
    source: str
    sink: str
    strategy: str


def resolve_process_route(spec: ProcessSpec) -> ResolvedProcessRoute:
    """Return the connector defaults and parsed strategy used by execution planning."""

    raw = spec.raw_config
    source = _mapping(raw.get("source"))
    sink = _mapping(raw.get("sink"))
    strategy = spec.config.load_config.load_strategy
    return ResolvedProcessRoute(
        source=canonical_endpoint_type(_connector_type(source, default="postgres")),
        sink=canonical_endpoint_type(_connector_type(sink, default="bigquery")),
        strategy=str(getattr(strategy, "value", strategy)).strip(),
    )


def _connector_type(config: Mapping[str, object], *, default: str) -> str:
    value = config["type"] if "type" in config else default
    return value.strip() if isinstance(value, str) else ""


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["ResolvedProcessRoute", "resolve_process_route"]
