"""Supply-chain evidence compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["SupplyChainAttestationReport", "SupplyChainAttestationService"]

_EXPORTS: dict[str, str] = {
    "SupplyChainAttestationReport": "dpone.supply_chain.attestation:SupplyChainAttestationReport",
    "SupplyChainAttestationService": "dpone.supply_chain.attestation:SupplyChainAttestationService",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
