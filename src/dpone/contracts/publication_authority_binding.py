"""Immutable, non-secret identity for an explicitly selected publication store.

This module performs no connection or schema admission I/O. A deployment must
authenticate service ownership and supply the observed SQL endpoint identity;
pure digest calculation does not establish those facts.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

from dpone.contracts.clickhouse_cluster_publication import canonical_json, digest_payload
from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.mssql_object_name import safe_mssql_identifier
from dpone.contracts.runtime_connection import ResolvedConnectionDescriptor

_DIGEST = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class PublicationAuthorityBinding:
    """One deployment-owned service namespace and exact SQL storage location.

    Logical aliases and secrets are not coordination identities. Storage changes
    preserve the logical target key but change its binding digest, requiring an
    explicit admitted cutover instead of treating an empty store as a new load.
    """

    backend: str
    connection_ref: str
    database: str
    schema: str
    service_id: str
    environment: str

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if (
                not isinstance(value, str)
                or not value
                or value != value.strip()
                or not value.isprintable()
                or len(value) > 128
            ):
                raise ValueError(f"publication_authority.{field.name}: invalid identity")
        if self.backend != "mssql":
            raise ValueError("publication_authority.backend: unsupported backend")
        if not is_valid_connection_ref(self.connection_ref):
            raise ValueError("publication_authority.connection_ref: canonical alias required")
        if not safe_mssql_identifier(self.database) or not safe_mssql_identifier(self.schema):
            raise ValueError("publication_authority: unsafe database/schema identifier")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PublicationAuthorityBinding:
        """Require the complete closed contract; never coerce or echo inputs."""
        names = {field.name for field in fields(cls)}
        if not isinstance(value, Mapping) or set(value) != names:
            raise ValueError("publication_authority: exact binding fields required")
        return cls(**dict(value))

    def require_descriptor(self, descriptor: ResolvedConnectionDescriptor | None) -> None:
        """Check non-secret registry coordinates, not live endpoint authority."""
        if not isinstance(descriptor, ResolvedConnectionDescriptor) or descriptor.connection_type != "mssql":
            raise ValueError("publication_authority: resolved MSSQL descriptor required")
        if descriptor.properties.get("database") != self.database or descriptor.properties.get("schema") != self.schema:
            raise ValueError("publication_authority: registry storage location mismatch")


def publication_slot_key(binding: PublicationAuthorityBinding, target_key: str) -> str:
    """Partition the existing target digest by stable service and environment."""
    _require_digest(target_key)
    return digest_payload(
        {
            "contract": "dpone.publication-slot.v1",
            "service_id": binding.service_id,
            "environment": binding.environment,
            "target_key": target_key,
        }
    )


def publication_binding_digest(binding: PublicationAuthorityBinding, *, endpoint_identity: str) -> str:
    """Bind observed storage endpoint and location, excluding alias/credentials.

    ``endpoint_identity`` is an independently admitted non-secret SHA-256, not
    a raw hostname and not a digest manufactured from an alias by this module.
    """
    _require_digest(endpoint_identity)
    return digest_payload(
        {
            "contract": "dpone.publication-binding.v1",
            "backend": binding.backend,
            "service_id": binding.service_id,
            "environment": binding.environment,
            "endpoint_identity": endpoint_identity,
            "database": binding.database,
            "schema": binding.schema,
        }
    )


def _require_digest(value: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError("publication_authority: canonical SHA-256 required")


def native_publication_provenance(binding_digest: str) -> bytes:
    """One closed origin contract shared by native SQL writes and readback."""
    _require_digest(binding_digest)
    return canonical_json(
        {"contract": "dpone.publication-origin.v1", "origin": "native", "binding_digest": binding_digest}
    ).encode()
