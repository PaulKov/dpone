"""Closed, deployment-signed PostgreSQL source identity authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

POSTGRES_SOURCE_AUTHORITY_PROPERTY = "postgres_source_authority"
_PHYSICAL_AUTHORITY_FIELDS = frozenset(
    {
        "version",
        "system_identifier",
        "timeline_id",
        "topology_role",
        "database",
        "principals",
        "relations",
    }
)
_CATALOG_AUTHORITY_FIELDS = frozenset(
    {
        "version",
        "verification_profile",
        "topology_role",
        "database",
        "principals",
        "relations",
    }
)
_NAMED_OID_FIELDS = frozenset({"canonical_name", "oid"})
_PRINCIPAL_FIELDS = frozenset({"effective", "session"})
_RELATION_FIELDS = frozenset({"schema", "relation", "namespace_oid", "relation_oid"})
_SYSTEM_IDENTIFIER = re.compile(r"^[1-9][0-9]{0,19}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class PostgresSourceAuthorityContractError(ValueError):
    """A registry source-authority document is incomplete or ambiguous."""


@dataclass(frozen=True, slots=True)
class PostgresNamedOidPin:
    """One exact PostgreSQL catalog name and OID."""

    canonical_name: str
    oid: int

    @classmethod
    def from_document(cls, raw: Any, *, role: str) -> PostgresNamedOidPin:
        document = _closed_mapping(raw, _NAMED_OID_FIELDS, f"{role}_fields_invalid")
        return cls(
            canonical_name=_required_name(
                document.get("canonical_name"),
                f"{role}_name_invalid",
            ),
            oid=_required_oid(document.get("oid"), f"{role}_oid_invalid"),
        )

    def to_document(self) -> dict[str, Any]:
        return {"canonical_name": self.canonical_name, "oid": self.oid}


@dataclass(frozen=True, slots=True)
class PostgresRelationAuthorityPin:
    """Exact canonical relation identity inside one PostgreSQL database."""

    schema: str
    relation: str
    namespace_oid: int
    relation_oid: int

    @classmethod
    def from_document(
        cls,
        key: str,
        raw: Any,
    ) -> PostgresRelationAuthorityPin:
        document = _closed_mapping(
            raw,
            _RELATION_FIELDS,
            "source_relation_authority_fields_invalid",
        )
        pin = cls(
            schema=_required_name(
                document.get("schema"),
                "source_relation_schema_invalid",
            ),
            relation=_required_name(
                document.get("relation"),
                "source_relation_name_invalid",
            ),
            namespace_oid=_required_oid(
                document.get("namespace_oid"),
                "source_relation_namespace_oid_invalid",
            ),
            relation_oid=_required_oid(
                document.get("relation_oid"),
                "source_relation_oid_invalid",
            ),
        )
        if key != pin.canonical_key:
            raise PostgresSourceAuthorityContractError("source_relation_authority_key_not_canonical")
        return pin

    @property
    def canonical_key(self) -> str:
        return f"{self.schema}.{self.relation}"

    def to_document(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "relation": self.relation,
            "namespace_oid": self.namespace_oid,
            "relation_oid": self.relation_oid,
        }


@dataclass(frozen=True, slots=True)
class SelectedPostgresSourceAuthority:
    """The one signed relation authority selected for a route."""

    system_identifier: str | None
    timeline_id: int | None
    verification_profile: str
    topology_role: str
    database: PostgresNamedOidPin
    effective_principal: PostgresNamedOidPin
    session_principal: PostgresNamedOidPin
    relation: PostgresRelationAuthorityPin
    authored_schema: str
    authored_relation: str
    version: int = 1

    @property
    def authority_document_utf8(self) -> bytes:
        """Return the byte-exact signed-authority preimage for this relation."""

        return json.dumps(
            self.to_document(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @property
    def authority_sha256(self) -> str:
        """Hash only this route's signed authority, not unrelated pins."""

        return "sha256:" + hashlib.sha256(self.authority_document_utf8).hexdigest()

    def to_document(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "version": self.version,
            "dialect": "postgres",
            "role": "source",
            "topology_role": self.topology_role,
            "database": self.database.to_document(),
            "principals": {
                "effective": self.effective_principal.to_document(),
                "session": self.session_principal.to_document(),
            },
            "relation": self.relation.to_document(),
        }
        if self.version == 1:
            document.update(
                system_identifier=self.system_identifier,
                timeline_id=self.timeline_id,
            )
        else:
            document["verification_profile"] = self.verification_profile
        return document

    @property
    def authored_uses_case_alias(self) -> bool:
        return self.authored_schema != self.relation.schema or self.authored_relation != self.relation.relation


@dataclass(frozen=True, slots=True)
class PostgresSourceAuthority:
    """Globally validated finite source registry with per-route selection."""

    system_identifier: str | None
    timeline_id: int | None
    verification_profile: str
    topology_role: str
    database: PostgresNamedOidPin
    effective_principal: PostgresNamedOidPin
    session_principal: PostgresNamedOidPin
    relations: tuple[PostgresRelationAuthorityPin, ...]
    version: int = 1

    @classmethod
    def from_connection_properties(
        cls,
        properties: Mapping[str, Any],
    ) -> PostgresSourceAuthority:
        raw = properties.get(POSTGRES_SOURCE_AUTHORITY_PROPERTY)
        if not isinstance(raw, Mapping):
            raise PostgresSourceAuthorityContractError("postgres_source_authority_fields_invalid")
        version = raw.get("version")
        if isinstance(version, bool) or not isinstance(version, int):
            raise PostgresSourceAuthorityContractError("postgres_source_authority_version_unsupported")
        if version == 1:
            document = _closed_mapping(
                raw,
                _PHYSICAL_AUTHORITY_FIELDS,
                "postgres_source_authority_fields_invalid",
            )
            verification_profile = "physical_cluster"
            system_identifier = str(document.get("system_identifier") or "")
            if _SYSTEM_IDENTIFIER.fullmatch(system_identifier) is None or int(system_identifier) >= 2**64:
                raise PostgresSourceAuthorityContractError("postgres_source_system_identifier_invalid")
            timeline_id = _required_uint32(
                document.get("timeline_id"),
                "postgres_source_timeline_id_invalid",
            )
        elif version == 2:
            document = _closed_mapping(
                raw,
                _CATALOG_AUTHORITY_FIELDS,
                "postgres_source_authority_fields_invalid",
            )
            verification_profile = str(document.get("verification_profile") or "")
            if verification_profile != "catalog_identity":
                raise PostgresSourceAuthorityContractError("postgres_source_verification_profile_invalid")
            system_identifier = None
            timeline_id = None
        else:
            raise PostgresSourceAuthorityContractError("postgres_source_authority_version_unsupported")
        topology_role = str(document.get("topology_role") or "").strip()
        if topology_role not in {"primary", "standby"}:
            raise PostgresSourceAuthorityContractError("postgres_source_topology_role_invalid")
        principals = _closed_mapping(
            document.get("principals"),
            _PRINCIPAL_FIELDS,
            "postgres_source_principals_fields_invalid",
        )
        raw_relations = document.get("relations")
        if not isinstance(raw_relations, Mapping) or not raw_relations:
            raise PostgresSourceAuthorityContractError("postgres_source_relations_required")
        relations = tuple(
            PostgresRelationAuthorityPin.from_document(str(key), value) for key, value in raw_relations.items()
        )
        _require_unambiguous_ascii_relation_keys(relations)
        return cls(
            system_identifier=system_identifier,
            timeline_id=timeline_id,
            verification_profile=verification_profile,
            topology_role=topology_role,
            database=PostgresNamedOidPin.from_document(
                document.get("database"),
                role="postgres_source_database",
            ),
            effective_principal=PostgresNamedOidPin.from_document(
                principals.get("effective"),
                role="postgres_source_effective_principal",
            ),
            session_principal=PostgresNamedOidPin.from_document(
                principals.get("session"),
                role="postgres_source_session_principal",
            ),
            relations=relations,
            version=version,
        )

    def select(
        self,
        *,
        authored_schema: str,
        authored_relation: str,
    ) -> SelectedPostgresSourceAuthority:
        schema = _required_name(
            authored_schema,
            "postgres_source_authored_schema_invalid",
        )
        relation = _required_name(
            authored_relation,
            "postgres_source_authored_relation_invalid",
        )
        exact = [pin for pin in self.relations if pin.schema == schema and pin.relation == relation]
        candidates = exact or [
            pin
            for pin in self.relations
            if ascii_case_alias(schema, pin.schema) and ascii_case_alias(relation, pin.relation)
        ]
        if len(candidates) != 1:
            code = (
                "postgres_source_relation_authority_missing"
                if not candidates
                else "postgres_source_relation_authority_ambiguous"
            )
            raise PostgresSourceAuthorityContractError(code)
        return SelectedPostgresSourceAuthority(
            system_identifier=self.system_identifier,
            timeline_id=self.timeline_id,
            verification_profile=self.verification_profile,
            topology_role=self.topology_role,
            database=self.database,
            effective_principal=self.effective_principal,
            session_principal=self.session_principal,
            relation=candidates[0],
            authored_schema=schema,
            authored_relation=relation,
            version=self.version,
        )


def ascii_case_alias(authored: str, canonical: str) -> bool:
    """Allow only ASCII A-Z case differences; Unicode stays exact."""

    if len(authored) != len(canonical):
        return False
    return all(
        left == right
        or (left.isascii() and right.isascii() and left.isalpha() and right.isalpha() and left.lower() == right.lower())
        for left, right in zip(authored, canonical, strict=True)
    )


def require_authority_sha256(value: Any) -> str:
    """Validate a runtime-owned selected-authority digest."""

    digest = str(value or "")
    if _SHA256.fullmatch(digest) is None:
        raise PostgresSourceAuthorityContractError("postgres_source_authority_digest_invalid")
    return digest


def _require_unambiguous_ascii_relation_keys(
    relations: tuple[PostgresRelationAuthorityPin, ...],
) -> None:
    folded: dict[tuple[str, str], PostgresRelationAuthorityPin] = {}
    for pin in relations:
        key = (_ascii_fold(pin.schema), _ascii_fold(pin.relation))
        if key in folded:
            raise PostgresSourceAuthorityContractError("postgres_source_relation_authority_ascii_case_ambiguous")
        folded[key] = pin


def _ascii_fold(value: str) -> str:
    return value.translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"))


def _closed_mapping(
    raw: Any,
    fields: frozenset[str],
    code: str,
) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != fields:
        raise PostgresSourceAuthorityContractError(code)
    return raw


def _required_name(value: Any, code: str) -> str:
    name = str(value or "")
    if not name or "\x00" in name or len(name.encode("utf-8")) > 63:
        raise PostgresSourceAuthorityContractError(code)
    return name


def _required_oid(value: Any, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**32:
        raise PostgresSourceAuthorityContractError(code)
    return value


def _required_uint32(value: Any, code: str) -> int:
    return _required_oid(value, code)


__all__ = [
    "POSTGRES_SOURCE_AUTHORITY_PROPERTY",
    "PostgresNamedOidPin",
    "PostgresRelationAuthorityPin",
    "PostgresSourceAuthority",
    "PostgresSourceAuthorityContractError",
    "SelectedPostgresSourceAuthority",
    "ascii_case_alias",
    "require_authority_sha256",
]
