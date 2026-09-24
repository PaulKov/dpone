"""Derive environment credential transport from verified workload packs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.init_fetch_connection_bridge import require_closed_init_fetch_connection_bridge
from dpone_airflow_pack.pack_identity import verify_pack_fingerprint


def environment_connection_projection(packs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any] | None:
    """Preserve legacy snapshots unless all selected packs explicitly choose env.

    These are already confined, digest-verified pack payloads; verify their own
    fingerprints as well before deriving a credential authority. Never guess
    transport from mutable process environment or a source registry default.
    """
    projections = []
    for pack in packs.values():
        verify_pack_fingerprint(pack)
        projection = pack.get("connection_projection")
        projections.append(projection if isinstance(projection, Mapping) else {})
    if not any(projection.get("mode") == "env" for projection in projections):
        return None
    if any(projection.get("mode") != "env" for projection in projections):
        raise ValueError("environment credential transport cannot mix with other pack transports")
    entries: dict[str, dict[str, Any]] = {}
    for projection in projections:
        closed = require_closed_init_fetch_connection_bridge(projection)
        for item in closed["connections"]:
            ref = item["registry_connection_ref"]
            entry = dict(item)
            if ref in entries and entries[ref]["connection_id"] != entry["connection_id"]:
                raise ValueError("conflicting environment credential mapping for registry ref")
            entries[ref] = entry
    return dict(
        require_closed_init_fetch_connection_bridge(
            {
                "mode": "env",
                "payload_format": "airflow_connection_uri",
                "secret_values": False,
                "connections": [entries[ref] for ref in sorted(entries)],
            }
        )
    )
