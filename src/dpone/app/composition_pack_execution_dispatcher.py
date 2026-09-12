"""Route one admitted pack-exec command to the installed v3 cell factories.

Native-v2 dispatch stays in ``composition_native_dbt_dispatch``. This cell only
selects the v3 factory after the verified command and manifest are classified.
Ordinary argv remains ``dpone run``; the concrete cell is a manifest decision.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.app.composition_execution_cells import InstalledCompositionExecutionCapabilities
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_execution import (
    composition_generated_transfer_cell,
    composition_transfer_cell,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.runtime.composition_native_dbt_dispatch import (
    ORDINARY_WORKER_UNAVAILABLE,
    CompositionNativeDbtDispatcher,
)
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionDispatchRequest,
)

_MAX_MANIFEST_BYTES = 8 * 1024 * 1024


class CompositionPackExecutionDispatcher:
    """Dispatch native dbt or ordinary transfer through the installed factories."""

    def __init__(
        self,
        *,
        supervisor: Any,
        capabilities: InstalledCompositionExecutionCapabilities,
        native_executor: Any | None,
        ordinary_root: Callable[[CompositionDispatchRequest, str, Mapping[str, Any]], Any | None],
    ) -> None:
        self._native = CompositionNativeDbtDispatcher(native_executor, supervisor=supervisor)
        self._capabilities = capabilities
        self._ordinary_root = ordinary_root

    def run(self, request: CompositionDispatchRequest) -> int:
        if request.kind == "native_dbt":
            return self._native.run(request)
        if request.kind != "ordinary_transfer":
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        return self._run_ordinary(request)

    def _run_ordinary(self, request: CompositionDispatchRequest) -> int:
        try:
            manifest = ordinary_manifest(request)
            cell = ordinary_cell(manifest)
            factory = self._capabilities.factory(cell)
        except (CompositionAdmissionError, CompositionDispatchRejection, OSError, TypeError, ValueError):
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE) from None
        if not callable(factory):
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        root = self._ordinary_root(request, cell, manifest)
        if root is None:
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        result = root.execute(request)
        return int(getattr(result, "exit_code", 5))


def ordinary_manifest(request: CompositionDispatchRequest) -> Mapping[str, Any]:
    """Read the launcher-verified ordinary manifest from the worktree."""

    path = request.working_directory / request.verified_input
    payload = path.read_bytes()
    if len(payload) > _MAX_MANIFEST_BYTES:
        raise CompositionAdmissionError("transfer_manifest_capability")
    return strict_json_object(payload)


def ordinary_cell(manifest: Mapping[str, Any]) -> str:
    """Classify PostgreSQL→MSSQL vs MSSQL→ClickHouse from original bytes."""

    try:
        return composition_transfer_cell(manifest)
    except CompositionAdmissionError:
        return composition_generated_transfer_cell(manifest)


__all__ = [
    "CompositionPackExecutionDispatcher",
    "ordinary_cell",
    "ordinary_manifest",
]
