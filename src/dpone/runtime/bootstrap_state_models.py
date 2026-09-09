"""Immutable values produced by runtime state bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.config.state import ResolvedMssqlStateConfig


@dataclass(frozen=True, slots=True)
class RuntimeStateBindings:
    """Resolved state services shared by runtime hydration components."""

    state_type: str
    proxy_config: Mapping[str, Any]
    xmin_state_storage: Any
    xmin_handoff_state_storage: Any = None
    kafka_offset_state_storage: Any = None
    partition_checkpoint_store: Any = None
    shared_bq_connector: Any = None
    shared_mssql_state_connector: Any = None
    shared_postgres_state_connector: Any = None
    mssql_state_location: ResolvedMssqlStateConfig | None = None
    load_audit_storage: Any = None


__all__ = ["RuntimeStateBindings"]
