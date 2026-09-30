"""Target allocation observations shared by native staging coordinators."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.ports.mssql_native import NativeChunkImporter, NativeChunkLimits, WindowContractError


def require_native_stage_allocation(
    limits: NativeChunkLimits,
    importer: NativeChunkImporter | None,
    external_observer: Callable[[], int] | None,
    observation: dict[str, Any] | None = None,
    key: str = "allocated",
) -> int:
    """Read one authoritative allocation value and enforce the configured ceiling."""
    if external_observer is not None:
        allocated = external_observer()
    elif importer is not None:
        allocated = importer.allocated_bytes()
    else:
        raise WindowContractError("mssql_native.allocation_observer_unavailable")
    if type(allocated) is not int or allocated < 0:
        raise WindowContractError("mssql_native.invalid_allocation_observation")
    if observation is not None:
        observation[key] = allocated
    if allocated > limits.stage_allocated_bytes_stop_threshold:
        raise WindowContractError(f"mssql_native.stage_allocation_threshold_exceeded:{allocated}")
    return allocated


__all__ = ["require_native_stage_allocation"]
