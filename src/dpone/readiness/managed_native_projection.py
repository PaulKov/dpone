"""Native-transfer projections used by the managed execution plan."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness import managed_native_transfer_plan as native_transfer_plan
from dpone.readiness import managed_planning_snapshot as snapshot_planning


def native_transfer_transport(raw: Mapping[str, Any], source_type: str, sink_type: str) -> dict[str, Any]:
    return native_transfer_plan.native_transfer_transport(raw, source_type=source_type, sink_type=sink_type)


def native_transfer_bulk_wire(raw: Mapping[str, Any], source_type: str, sink_type: str) -> dict[str, Any]:
    override = snapshot_planning.bulk_wire_override(raw, source_type, sink_type)
    if override is not None:
        return override
    return native_transfer_plan.native_transfer_bulk_wire(raw, source_type=source_type, sink_type=sink_type)


def native_transfer_snapshot_optimization(raw: Mapping[str, Any], source_type: str, sink_type: str) -> dict[str, Any]:
    return native_transfer_plan.native_transfer_snapshot_optimization(raw, source_type=source_type, sink_type=sink_type)


def native_transfer_route_decision(
    raw: Mapping[str, Any],
    source_type: str,
    sink_type: str,
    *,
    manifest_dir: Path,
    certification_mode_override: str | None = None,
) -> dict[str, Any]:
    return native_transfer_plan.native_transfer_route_decision(
        raw,
        source_type=source_type,
        sink_type=sink_type,
        manifest_dir=manifest_dir,
        certification_mode_override=certification_mode_override,
    )
