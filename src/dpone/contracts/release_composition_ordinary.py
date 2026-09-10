"""Detached evidence returned by the ordinary workload inventory capability."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_relation_writes import DbtRelationWrite


class OrdinaryReleaseInventoryError(ValueError):
    """Ordinary sources cannot establish complete declarative execution authority."""

    code = "DPONE_RELEASE_COMPOSITION_ORDINARY_INVALID"


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in {str, int, bool, bytes}:
        return value
    raise TypeError("ordinary capture contains an unsupported value type")


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class OrdinaryReleaseCapture:
    """Original source inventory, verified transport projection and logical writes.

    Every mapping and sequence is recursively copied and frozen. ``files`` binds
    the original source bytes; ``dag_files`` and ``pack_files`` contain only the
    deterministic strict transport projection. Inventory identity excludes the
    sidecar choice and mutable local filesystem locations.
    """

    inventory: Mapping[str, Any]
    files: Mapping[str, bytes]
    dag_files: Mapping[str, bytes]
    pack_files: Mapping[str, bytes]
    relation_writes: tuple[DbtRelationWrite, ...]

    def __post_init__(self) -> None:
        for field in ("inventory", "files", "dag_files", "pack_files"):
            object.__setattr__(self, field, _freeze(getattr(self, field)))
        object.__setattr__(self, "relation_writes", tuple(self.relation_writes))

    @property
    def inventory_sha256(self) -> str:
        """Canonical source identity independent of the final transport rewrite."""
        return canonical_fingerprint(self.inventory_dict())

    def inventory_dict(self) -> dict[str, Any]:
        """Return a detached JSON-serializable inventory with list arrays."""
        return _json_value(self.inventory)
