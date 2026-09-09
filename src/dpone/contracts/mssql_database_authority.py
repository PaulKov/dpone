"""Immutable SQL Server database identity pinned by a connection registry.

The registry artifact is deployment authority.  Runtime code may verify these
facts against SQL Server, but it must never populate or rotate them from the
currently reachable database.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_AUTHORITY_FIELDS = frozenset({"database_id", "create_token", "database_guid"})
_CREATE_TOKEN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?$")


class MssqlDatabaseAuthorityContractError(RuntimeError):
    """A strict registry omitted or malformed physical database authority."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class MssqlDatabaseAuthorityPin:
    """One canonical SQL Server database identity from a signed registry."""

    database_name: str
    database_id: int
    create_token: str
    database_guid: uuid.UUID

    @classmethod
    def from_raw(
        cls,
        database_name: Any,
        raw: Any,
        *,
        capability: str,
    ) -> MssqlDatabaseAuthorityPin:
        """Parse one keyed, exact, closed authority document."""

        prefix = _capability_prefix(capability)
        resolved_name = _text(database_name)
        if not resolved_name or len(resolved_name) > 128:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_name_invalid")
        if not isinstance(raw, Mapping) or set(raw) != _AUTHORITY_FIELDS:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_fields_invalid")
        database_id = raw.get("database_id")
        if isinstance(database_id, bool) or not isinstance(database_id, int) or database_id <= 0:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_id_invalid")
        create_token = _text(raw.get("create_token"))
        if _CREATE_TOKEN.fullmatch(create_token) is None:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_create_token_invalid")
        raw_guid = _text(raw.get("database_guid"))
        try:
            database_guid = uuid.UUID(raw_guid)
        except (ValueError, AttributeError) as exc:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_guid_invalid") from exc
        if str(database_guid) != raw_guid:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_guid_not_canonical")
        return cls(resolved_name, database_id, create_token, database_guid)


@dataclass(frozen=True, slots=True)
class MssqlDatabaseAuthoritySet:
    """Finite signed identities addressable by canonical or case-alias name."""

    pins: tuple[MssqlDatabaseAuthorityPin, ...]

    @classmethod
    def from_connection_properties(
        cls,
        properties: Any,
        *,
        capability: str,
    ) -> MssqlDatabaseAuthoritySet:
        """Parse every authored pin and require the connection's default DB."""

        prefix = _capability_prefix(capability)
        if not isinstance(properties, Mapping):
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_connection_properties_invalid")
        raw = properties.get("database_authorities")
        if not isinstance(raw, Mapping) or not raw:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authorities_required")
        pins = tuple(
            MssqlDatabaseAuthorityPin.from_raw(name, value, capability=capability)
            for name, value in sorted(raw.items(), key=lambda item: str(item[0]))
        )
        folded = [pin.database_name.casefold() for pin in pins]
        if len(folded) != len(set(folded)):
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_alias_ambiguous")
        result = cls(pins)
        result.require(_text(properties.get("database")), capability=capability)
        return result

    def require(self, database_name: str, *, capability: str) -> MssqlDatabaseAuthorityPin:
        """Resolve exactly one pin while allowing only a case spelling alias."""

        prefix = _capability_prefix(capability)
        requested = _text(database_name)
        matches = tuple(pin for pin in self.pins if pin.database_name.casefold() == requested.casefold())
        if len(matches) != 1:
            raise MssqlDatabaseAuthorityContractError(f"{prefix}_database_authority_required")
        return matches[0]


def _capability_prefix(capability: str) -> str:
    if capability not in {"target", "staging", "state"}:
        raise ValueError("MSSQL database authority capability must be target, staging, or state")
    return f"mssql_transaction.{capability}"


def _text(value: Any) -> str:
    return str(value or "").strip()


__all__ = [
    "MssqlDatabaseAuthorityContractError",
    "MssqlDatabaseAuthorityPin",
    "MssqlDatabaseAuthoritySet",
]
