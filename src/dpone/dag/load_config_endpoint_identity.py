"""Canonical runtime endpoint options with v1 identity compatibility."""

from __future__ import annotations

from typing import Any

from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION


def inject_endpoint_identity_options(
    options: dict[str, Any],
    source_type: object,
    sink_type: object,
    canonical_source_type: str,
    canonical_sink_type: str,
) -> None:
    """Install canonical runtime types and the minimal legacy identity projection."""

    authored_source = str(source_type)
    authored_sink = str(sink_type)
    options.pop(ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION, None)
    options["source_type"] = canonical_source_type
    options["sink_type"] = canonical_sink_type
    if authored_source != canonical_source_type or authored_sink != canonical_sink_type:
        options[ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION] = {
            "source_type": authored_source,
            "sink_type": authored_sink,
        }


__all__ = ["inject_endpoint_identity_options"]
