"""Immutable values for the MSSQL physical-target registry boundary."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MssqlPhysicalTargetIdentity:
    """Opaque binding returned by the target database under its own collation."""

    server_name: str
    machine_name: str
    instance_name: str
    replica_name: str
    database_name: str
    database_create_token: str
    binding_id: uuid.UUID
    schema_name: str
    table_name: str
    object_id: int | None

    @property
    def digest(self) -> bytes:
        """Return the one key used for state, locking, authority and receipts."""

        payload = {
            "version": "mssql_physical_target_registry_v1",
            "server": {
                "server_name": self.server_name.casefold(),
                "machine_name": self.machine_name.casefold(),
                "instance_name": self.instance_name.casefold(),
                "replica_name": self.replica_name.casefold(),
            },
            "database": {
                "name": self.database_name,
                "create_token": self.database_create_token,
            },
            "binding_id": str(self.binding_id),
        }
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).digest()


__all__ = ["MssqlPhysicalTargetIdentity"]
