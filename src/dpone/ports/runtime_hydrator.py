"""Runtime hydration port for ETL process configs.

This module is intentionally lightweight and free of imports from
``dpone.runtime``. The DAG/manifest layers depend on this port to obtain
runtime bindings only when real execution is requested.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config import LoadConfig


import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.contracts import RuntimeConfigurationError


@dataclass(slots=True)
class RuntimeBindings:
    """Runtime-only objects attached to a parsed ETL config."""

    source_obj: Any = None
    sink_obj: Any = None
    etl_logger: Any = None
    run_state_storage: Any = None
    xmin_handoff_state_storage: Any = None
    partition_checkpoint_store: Any = None
    load_identity_service: Any = None
    credential_resolution_receipts: tuple[Mapping[str, Any], ...] = ()

    postgres_mssql_correctness_activation: Any = None

    postgres_mssql_correctness_runtime: Any = None


class RuntimeHydrator(Protocol):
    """Builds runtime objects for a parsed ETL config."""

    def build(
        self,
        *,
        config: Mapping[str, Any],
        load_config: LoadConfig,
    ) -> RuntimeBindings:  # pragma: no cover - protocol
        ...


_RUNTIME_HYDRATOR: RuntimeHydrator | None = None
_DEFAULT_RUNTIME_BOOTSTRAP = "dpone.app.runtime_bootstrap"


def register_runtime_hydrator(hydrator: RuntimeHydrator) -> None:
    """Registers the default runtime hydrator implementation."""

    global _RUNTIME_HYDRATOR
    _RUNTIME_HYDRATOR = hydrator


def get_runtime_hydrator() -> RuntimeHydrator | None:
    return _RUNTIME_HYDRATOR


def ensure_runtime_hydrator() -> RuntimeHydrator:
    """Returns a registered hydrator, importing the default bootstrap lazily.

    The lazy bootstrap keeps metadata-only CLI/documentation workflows free of
    heavy optional runtime dependencies. Execution paths can still obtain the
    default runtime implementation on demand.
    """

    hydrator = get_runtime_hydrator()
    if hydrator is not None:
        return hydrator

    importlib.import_module(_DEFAULT_RUNTIME_BOOTSTRAP)
    hydrator = get_runtime_hydrator()
    if hydrator is None:
        raise RuntimeConfigurationError(
            "Runtime hydrator is not registered. Import dpone.app.runtime_bootstrap "
            "or register a custom RuntimeHydrator via dpone.ports.runtime_hydrator."
        )
    return hydrator
