"""Exact capability coordinates used by semantic-refresh route evidence."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.semantic_refresh_core import require_closed_mapping, require_digest, require_text

_FIELDS = frozenset(
    {
        "source_connector",
        "source_connector_version",
        "sink_connector",
        "sink_connector_version",
        "load_strategy",
        "environment",
        "runtime_image_digest",
        "toolchain_sha256",
        "capability_policy_sha256",
        "source_capability_sha256",
        "sink_capability_sha256",
        "route_policy_sha256",
    }
)


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshRouteCapabilityCoordinates:
    """The exact source/sink/runtime/policy tuple certified by one receipt."""

    source_connector: str
    source_connector_version: str
    sink_connector: str
    sink_connector_version: str
    load_strategy: str
    environment: str
    runtime_image_digest: str
    toolchain_sha256: str
    capability_policy_sha256: str
    source_capability_sha256: str
    sink_capability_sha256: str
    route_policy_sha256: str

    def __post_init__(self) -> None:
        for field in (
            "source_connector",
            "source_connector_version",
            "sink_connector",
            "sink_connector_version",
            "load_strategy",
            "environment",
        ):
            require_text(getattr(self, field), f"coordinates.{field}")
        for field in _FIELDS - {
            "source_connector",
            "source_connector_version",
            "sink_connector",
            "sink_connector_version",
            "load_strategy",
            "environment",
        }:
            require_digest(getattr(self, field), f"coordinates.{field}")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshRouteCapabilityCoordinates:
        """Parse exact coordinates without extension fields."""

        raw = require_closed_mapping(value, "coordinates", required=_FIELDS)
        return cls(
            source_connector=require_text(raw.get("source_connector"), "coordinates.source_connector"),
            source_connector_version=require_text(
                raw.get("source_connector_version"), "coordinates.source_connector_version"
            ),
            sink_connector=require_text(raw.get("sink_connector"), "coordinates.sink_connector"),
            sink_connector_version=require_text(
                raw.get("sink_connector_version"), "coordinates.sink_connector_version"
            ),
            load_strategy=require_text(raw.get("load_strategy"), "coordinates.load_strategy"),
            environment=require_text(raw.get("environment"), "coordinates.environment"),
            runtime_image_digest=require_digest(raw.get("runtime_image_digest"), "coordinates.runtime_image_digest"),
            toolchain_sha256=require_digest(raw.get("toolchain_sha256"), "coordinates.toolchain_sha256"),
            capability_policy_sha256=require_digest(
                raw.get("capability_policy_sha256"), "coordinates.capability_policy_sha256"
            ),
            source_capability_sha256=require_digest(
                raw.get("source_capability_sha256"), "coordinates.source_capability_sha256"
            ),
            sink_capability_sha256=require_digest(
                raw.get("sink_capability_sha256"), "coordinates.sink_capability_sha256"
            ),
            route_policy_sha256=require_digest(raw.get("route_policy_sha256"), "coordinates.route_policy_sha256"),
        )

    def to_dict(self) -> dict[str, str]:
        """Return exact coordinates in canonical field form."""

        return {field: getattr(self, field) for field in sorted(_FIELDS)}


__all__ = ["SemanticRefreshRouteCapabilityCoordinates"]
