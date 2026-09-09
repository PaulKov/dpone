"""Immutable, route-selected source identity for retry-safe governance."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SourcePhysicalIdentity:
    """Signed physical identity; endpoint coordinates are diagnostics only."""

    dialect: str
    cluster_identifier: str | None
    database: str
    effective_principal: str
    session_principal: str
    server_address: str | None = None
    server_port: int | None = None
    topology_role: str | None = None
    version: int = 1
    authority_sha256: str | None = None
    timeline_id: int | None = None
    database_oid: int | None = None
    effective_principal_oid: int | None = None
    session_principal_oid: int | None = None
    schema: str | None = None
    schema_oid: int | None = None
    relation: str | None = None
    relation_oid: int | None = None
    verification_profile: str | None = None

    def __post_init__(self) -> None:
        required = (self.dialect, self.database, self.effective_principal, self.session_principal)
        if any(not str(value).strip() for value in required):
            raise ValueError("mssql_transaction.source_physical_identity_incomplete")
        if self.server_port is not None and (
            isinstance(self.server_port, bool) or not isinstance(self.server_port, int) or self.server_port <= 0
        ):
            raise ValueError("mssql_transaction.source_physical_identity_port_invalid")
        if self.version not in {1, 2, 3}:
            raise ValueError("mssql_transaction.source_physical_identity_version_unsupported")
        if self.version == 1:
            if not str(self.cluster_identifier or "").strip():
                raise ValueError("mssql_transaction.source_physical_identity_incomplete")
            if self.authority_sha256 is not None:
                raise ValueError("mssql_transaction.source_physical_identity_version_invalid")
            return
        if re.fullmatch(r"sha256:[0-9a-f]{64}", str(self.authority_sha256 or "")) is None:
            raise ValueError("mssql_transaction.source_authority_digest_invalid")
        extended_names = (self.schema, self.relation, self.topology_role)
        if any(not str(value or "").strip() for value in extended_names):
            raise ValueError("mssql_transaction.source_physical_identity_incomplete")
        extended_numbers = (
            self.database_oid,
            self.effective_principal_oid,
            self.session_principal_oid,
            self.schema_oid,
            self.relation_oid,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value >= 2**32
            for value in extended_numbers
        ):
            raise ValueError("mssql_transaction.source_physical_identity_oid_invalid")
        if self.version == 2:
            if (
                not str(self.cluster_identifier or "").strip()
                or self.verification_profile not in {None, "physical_cluster"}
                or isinstance(self.timeline_id, bool)
                or not isinstance(self.timeline_id, int)
                or not 0 < self.timeline_id < 2**32
            ):
                raise ValueError("mssql_transaction.source_physical_identity_version_invalid")
            return
        if (
            self.cluster_identifier is not None
            or self.timeline_id is not None
            or self.verification_profile != "catalog_identity"
        ):
            raise ValueError("mssql_transaction.source_physical_identity_version_invalid")

    def to_dict(self) -> dict[str, Any]:
        """Return only route identity, excluding DNS/LB endpoint diagnostics."""

        contract: dict[str, Any] = {
            "version": self.version,
            "dialect": self.dialect,
            "cluster_identifier": self.cluster_identifier,
            "database": self.database,
            "effective_principal": self.effective_principal,
            "session_principal": self.session_principal,
            "topology_role": self.topology_role,
        }
        if self.version in {2, 3}:
            contract.update(
                {
                    "authority_sha256": self.authority_sha256,
                    "timeline_id": self.timeline_id,
                    "database_oid": self.database_oid,
                    "effective_principal_oid": self.effective_principal_oid,
                    "session_principal_oid": self.session_principal_oid,
                    "schema": self.schema,
                    "schema_oid": self.schema_oid,
                    "relation": self.relation,
                    "relation_oid": self.relation_oid,
                }
            )
        if self.version == 3:
            contract["verification_profile"] = self.verification_profile
        return contract

    def diagnostics(self) -> dict[str, Any]:
        """Return non-authoritative endpoint facts for safe observability."""

        return {
            "server_address": self.server_address,
            "server_port": self.server_port,
        }


__all__ = ["SourcePhysicalIdentity"]
