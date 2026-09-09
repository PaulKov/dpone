"""Command-layer factory for readiness services."""

from __future__ import annotations

from pathlib import Path

from dpone.readiness.capability_discovery_composition import (
    build_capability_discovery_service,
)
from dpone.services.readiness import ReadinessService


def build_readiness_service() -> ReadinessService:
    """Return the readiness service used by CLI commands."""

    root = Path.cwd()
    return ReadinessService(
        capability_snapshot_factory=lambda: build_capability_discovery_service(
            root=root,
        ).snapshot()
    )
