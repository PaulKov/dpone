"""Immutable, backend-neutral runtime connection contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.connector_declarations import canonical_endpoint_type

if TYPE_CHECKING:
    from dpone.runtime.credentials.config import CredentialsConfig


class RuntimeConnectionAuthorityError(RuntimeError):
    """Fail-closed error for invalid or conflicting runtime connection authority."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def canonical_runtime_endpoint_type(value: object) -> str:
    """Project a manifest/registry endpoint token onto runtime identity."""

    return canonical_endpoint_type(str(value or ""))


def runtime_connection_authority_subject(
    *,
    environment: str,
    release_id: str,
    deployment_id: str,
    release_sha256: str,
    deployment_sha256: str,
    binding_set_sha256: str,
    connection_registry_sha256: str,
    credential_runtime_sha256: str,
) -> str:
    """Bind loader-verified shared connection artifacts to one deployment."""

    return canonical_fingerprint(
        {
            "schema": "dpone.runtime-connection-authority-subject.v1",
            "environment": environment,
            "release_id": release_id,
            "deployment_id": deployment_id,
            "release_sha256": release_sha256,
            "deployment_sha256": deployment_sha256,
            "binding_set_sha256": binding_set_sha256,
            "connection_registry_sha256": connection_registry_sha256,
            "credential_runtime_sha256": credential_runtime_sha256,
        }
    )


@dataclass(frozen=True, slots=True)
class ResolvedConnectionDescriptor:
    """A non-secret connection type and detached immutable registry metadata."""

    connection_type: str
    properties: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.properties, Mapping):
            raise TypeError("properties must be a mapping")
        object.__setattr__(self, "connection_type", canonical_runtime_endpoint_type(self.connection_type))
        object.__setattr__(self, "properties", _freeze_mapping(self.properties))


@dataclass(frozen=True)
class ResolvedBindingConnection:
    """Resolved credentials, safe evidence metadata, and optional connection metadata."""

    credentials: CredentialsConfig = field(repr=False)
    safe_metadata: dict[str, Any]
    descriptor: ResolvedConnectionDescriptor | None = None


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_value(item) for item in value)
    if isinstance(value, bytearray):
        return bytes(value)
    return value


__all__ = [
    "ResolvedBindingConnection",
    "ResolvedConnectionDescriptor",
    "RuntimeConnectionAuthorityError",
    "canonical_runtime_endpoint_type",
    "runtime_connection_authority_subject",
]
