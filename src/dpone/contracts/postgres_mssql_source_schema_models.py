"""Closed models used by PostgreSQL selected-relation schema authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import NoReturn

from dpone.contracts.mssql_r1_v3_codec import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_int,
    expect_text,
)
from dpone.contracts.postgres_mssql_source_schema_primitives import (
    SourceSchemaPrimitiveError,
    closed_mapping,
    exact_bool,
    exact_digest,
    exact_int,
    exact_text,
)
from dpone.contracts.postgres_mssql_type_authority import (
    POSTGRES_PG_CATALOG_NAMESPACE_OID_V1,
    PostgresMssqlSourceColumnRefV1,
    PostgresMssqlTypePolicyAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1
from dpone.contracts.postgres_source_authority import PostgresNamedOidPin, PostgresRelationAuthorityPin

_COLUMN_DOMAIN = b"dpone-postgres-mssql-observed-source-column-v1\0"
_COLUMN_VERSION = "dpone-postgres-mssql-observed-source-column-1"
_UINT32_MAX = 2**32 - 1
_SYSTEM_IDENTIFIER = re.compile(r"[1-9][0-9]{0,19}")


class PostgresMssqlSourceSchemaModelErrorV1(ValueError):
    """One closed model rejection with no dependency exception leakage."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)

    @property
    def __cause__(self) -> None:
        return None

    @__cause__.setter
    def __cause__(self, value: object) -> None:
        del value

    @property
    def __context__(self) -> None:
        return None

    @__context__.setter
    def __context__(self, value: object) -> None:
        del value


def _fail(reason: str) -> NoReturn:
    raise PostgresMssqlSourceSchemaModelErrorV1(reason)


@dataclass(frozen=True, slots=True)
class PostgresSelectedRelationAuthorityDocumentV1:
    """Strict decoder for the existing selected-source JSON preimage."""

    version: int
    dialect: str
    role: str
    topology_role: str
    database: PostgresNamedOidPin
    effective_principal: PostgresNamedOidPin
    session_principal: PostgresNamedOidPin
    relation: PostgresRelationAuthorityPin
    system_identifier: str | None
    timeline_id: int | None
    verification_profile: str | None

    @classmethod
    def from_authority_document_utf8(cls, payload: bytes) -> PostgresSelectedRelationAuthorityDocumentV1:
        try:
            if type(payload) is not bytes:
                _fail("malformed_canonical_bytes")
            document = json.loads(payload)
            value = cls.from_document(document)
            canonical = json.dumps(
                document,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if canonical != payload:
                _fail("malformed_canonical_bytes")
            return value
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaModelErrorV1:
            raise
        except (UnicodeError, TypeError, ValueError, KeyError, SourceSchemaPrimitiveError):
            _fail("malformed_canonical_bytes")

    @classmethod
    def from_document(cls, raw: object) -> PostgresSelectedRelationAuthorityDocumentV1:
        try:
            if type(raw) is not dict:
                _fail("malformed_canonical_bytes")
            version = exact_int(raw.get("version"), minimum=1, maximum=2)
            common = {
                "version",
                "dialect",
                "role",
                "topology_role",
                "database",
                "principals",
                "relation",
            }
            expected = common | ({"system_identifier", "timeline_id"} if version == 1 else {"verification_profile"})
            document = closed_mapping(raw, frozenset(expected))
            if document["dialect"] != "postgres" or document["role"] != "source":
                _fail("malformed_canonical_bytes")
            topology = document["topology_role"]
            if type(topology) is not str or topology not in {"primary", "standby"}:
                _fail("malformed_canonical_bytes")
            database = _named_pin(document["database"])
            principals = closed_mapping(document["principals"], frozenset({"effective", "session"}))
            relation = _relation_pin(document["relation"])
            system_identifier: str | None = None
            timeline_id: int | None = None
            verification_profile: str | None = None
            if version == 1:
                system_identifier = document["system_identifier"]
                if (
                    type(system_identifier) is not str
                    or _SYSTEM_IDENTIFIER.fullmatch(system_identifier) is None
                    or int(system_identifier) >= 2**64
                ):
                    _fail("malformed_canonical_bytes")
                timeline_id = exact_int(document["timeline_id"], minimum=1, maximum=_UINT32_MAX)
            else:
                verification_profile = document["verification_profile"]
                if verification_profile != "catalog_identity":
                    _fail("malformed_canonical_bytes")
            return cls(
                version,
                "postgres",
                "source",
                topology,
                database,
                _named_pin(principals["effective"]),
                _named_pin(principals["session"]),
                relation,
                system_identifier,
                timeline_id,
                verification_profile,
            )
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaModelErrorV1:
            raise
        except (TypeError, ValueError, KeyError, SourceSchemaPrimitiveError):
            _fail("malformed_canonical_bytes")


def _named_pin(raw: object) -> PostgresNamedOidPin:
    item = closed_mapping(raw, frozenset({"canonical_name", "oid"}))
    return PostgresNamedOidPin(
        exact_text(item["canonical_name"]),
        exact_int(item["oid"], minimum=1, maximum=_UINT32_MAX),
    )


def _relation_pin(raw: object) -> PostgresRelationAuthorityPin:
    item = closed_mapping(raw, frozenset({"schema", "relation", "namespace_oid", "relation_oid"}))
    return PostgresRelationAuthorityPin(
        exact_text(item["schema"]),
        exact_text(item["relation"]),
        exact_int(item["namespace_oid"], minimum=1, maximum=_UINT32_MAX),
        exact_int(item["relation_oid"], minimum=1, maximum=_UINT32_MAX),
    )


def require_type_policy(value: object) -> PostgresMssqlTypePolicyAuthorityV1:
    """Return an exact V1 type-policy authority or fail closed."""

    if type(value) is not PostgresMssqlTypePolicyAuthorityV1:
        _fail("type_policy_exact_type_required")
    return value


@dataclass(frozen=True, slots=True)
class PostgresMssqlObservedSourceColumnV1:
    """One immutable column observation bound to one selected relation."""

    contract_version: str
    selected_source_authority_sha256: bytes
    namespace_oid: int
    relation_oid: int
    projection_ordinal: int
    attribute_number: int
    name: str
    type_oid: int
    type_namespace_oid: int
    type_namespace_name: str
    type_name: str
    type_kind: str
    type_modifier: int
    nullable: bool
    collation_oid: int
    generated_kind: str
    identity_kind: str
    source_shape: PostgresMssqlSourceScalarShapeV1
    source_column_ref: PostgresMssqlSourceColumnRefV1

    def __post_init__(self) -> None:
        try:
            if self.contract_version != _COLUMN_VERSION:
                _fail("wrong_version")
            exact_digest(self.selected_source_authority_sha256)
            exact_int(self.namespace_oid, minimum=1, maximum=_UINT32_MAX)
            exact_int(self.relation_oid, minimum=1, maximum=_UINT32_MAX)
            exact_int(self.projection_ordinal, minimum=1, maximum=1024)
            exact_int(self.attribute_number, minimum=1, maximum=32767)
            exact_text(self.name, nfc=True)
            exact_int(self.type_oid, minimum=1, maximum=_UINT32_MAX)
            if self.type_namespace_oid != POSTGRES_PG_CATALOG_NAMESPACE_OID_V1:
                _fail("column_type_identity_invalid")
            if self.type_namespace_name != "pg_catalog" or self.type_kind != "b":
                _fail("column_type_identity_invalid")
            exact_text(self.type_name)
            exact_int(self.type_modifier, minimum=-(2**31), maximum=2**31 - 1)
            exact_bool(self.nullable)
            exact_int(self.collation_oid, minimum=0, maximum=_UINT32_MAX)
            if self.generated_kind != "" or self.identity_kind not in {"", "a", "d"}:
                _fail("source_column_unsupported")
            if type(self.source_shape) is not PostgresMssqlSourceScalarShapeV1:
                _fail("exact_type_violation")
            if type(self.source_column_ref) is not PostgresMssqlSourceColumnRefV1:
                _fail("exact_type_violation")
            if (
                self.type_oid != self.source_shape.source_type_oid
                or self.type_modifier != self.source_shape.source_typmod
                or self.type_name != self.source_shape.family.value
                or self.source_column_ref.ordinal != self.projection_ordinal
                or self.source_column_ref.name != self.name
                or self.source_column_ref.nullable is not self.nullable
                or self.source_column_ref.source_shape.canonical_bytes != self.source_shape.canonical_bytes
            ):
                _fail("column_type_identity_invalid")
        except PostgresMssqlSourceSchemaModelErrorV1:
            raise
        except (TypeError, ValueError, SourceSchemaPrimitiveError):
            _fail("exact_type_violation")

    @property
    def canonical_bytes(self) -> bytes:
        payload = canonical_bytes(
            _COLUMN_DOMAIN,
            (
                self.contract_version,
                self.selected_source_authority_sha256,
                self.namespace_oid,
                self.relation_oid,
                self.projection_ordinal,
                self.attribute_number,
                self.name,
                self.type_oid,
                self.type_namespace_oid,
                self.type_namespace_name,
                self.type_name,
                self.type_kind,
                self.type_modifier,
                self.nullable,
                self.collation_oid,
                self.generated_kind,
                self.identity_kind,
                self.source_shape.canonical_bytes,
                self.source_column_ref.canonical_bytes,
            ),
        )
        return payload + hashlib.sha256(payload).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlObservedSourceColumnV1:
        try:
            if type(payload) is not bytes or len(payload) < 33:
                _fail("malformed_canonical_bytes")
            encoded, checksum = payload[:-32], payload[-32:]
            values = decode_canonical_bytes(encoded, _COLUMN_DOMAIN, field_count=19)
            if hashlib.sha256(encoded).digest() != checksum:
                _fail("malformed_canonical_bytes")
            nullable = values[13]
            if type(nullable) is not bool:
                _fail("exact_type_violation")
            return cls(
                expect_text(values[0], "version"),
                exact_digest(expect_bytes(values[1], "selected digest")),
                expect_int(values[2], "namespace OID"),
                expect_int(values[3], "relation OID"),
                expect_int(values[4], "projection ordinal"),
                expect_int(values[5], "attribute number"),
                expect_text(values[6], "name"),
                expect_int(values[7], "type OID"),
                expect_int(values[8], "type namespace OID"),
                expect_text(values[9], "type namespace name"),
                expect_text(values[10], "type name"),
                expect_text(values[11], "type kind"),
                expect_int(values[12], "type modifier"),
                nullable,
                expect_int(values[14], "collation OID"),
                expect_text(values[15], "generated kind"),
                expect_text(values[16], "identity kind"),
                PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(expect_bytes(values[17], "source shape")),
                PostgresMssqlSourceColumnRefV1.from_canonical_bytes(expect_bytes(values[18], "source column ref")),
            )
        except PostgresMssqlSourceSchemaModelErrorV1:
            raise
        except Exception as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            _fail("wrong_domain" if "domain" in str(exc).lower() else "malformed_canonical_bytes")


__all__ = [
    "PostgresMssqlObservedSourceColumnV1",
    "PostgresMssqlSourceSchemaModelErrorV1",
    "PostgresSelectedRelationAuthorityDocumentV1",
    "require_type_policy",
]
