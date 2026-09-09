"""Connector SDK scaffold and certification compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ConnectorCapabilityCertificationService",
    "ConnectorCertificationTemplateService",
    "ConnectorSdkScaffoldResult",
    "ConnectorSdkScaffoldService",
    "NativeTransferCapability",
    "TransportCapabilityMatrix",
    "TransportEvidenceRenderer",
]

_EXPORTS: dict[str, str] = {
    "ConnectorCapabilityCertificationService": (
        "dpone.connector_sdk.native_transfer_certification:ConnectorCapabilityCertificationService"
    ),
    "ConnectorCertificationTemplateService": "dpone.connector_sdk.certification:ConnectorCertificationTemplateService",
    "ConnectorSdkScaffoldResult": "dpone.connector_sdk.models:ConnectorSdkScaffoldResult",
    "ConnectorSdkScaffoldService": "dpone.connector_sdk.scaffold:ConnectorSdkScaffoldService",
    "NativeTransferCapability": "dpone.connector_sdk.native_transfer_certification:NativeTransferCapability",
    "TransportCapabilityMatrix": "dpone.connector_sdk.native_transfer_certification:TransportCapabilityMatrix",
    "TransportEvidenceRenderer": "dpone.connector_sdk.native_transfer_rendering:TransportEvidenceRenderer",
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
