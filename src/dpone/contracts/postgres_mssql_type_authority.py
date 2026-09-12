"""Type-policy coverage, catalog resolution and source-column reference issuance."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_int,
    expect_text,
    expect_tuple,
    require_digest,
)
from dpone.contracts.postgres_mssql_type_derivation import (
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlTypeDecisionAuthorityV1,
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)

POSTGRES_PG_CATALOG_NAMESPACE_OID_V1 = 11

_POLICY = b"dpone-postgres-mssql-type-policy-authority-v1\0"
_SOURCE_REF = b"dpone-postgres-mssql-source-column-ref-v1\0"
_POLICY_VERSION = "dpone-postgres-mssql-type-policy-1"


class _CatalogShapeResolutionError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class PostgresMssqlTypePolicyAuthorityV1:
    policy_version: str = _POLICY_VERSION
    ordered_decisions: tuple[PostgresMssqlTypeDecisionAuthorityV1, ...] = ()

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.policy_version) is not str or self.policy_version != _POLICY_VERSION:
            reject("wrong_version")
        if (
            type(self.ordered_decisions) is not tuple
            or not self.ordered_decisions
            or len(self.ordered_decisions) > 1024
        ):
            reject("policy_coverage_invalid")
        if not all(type(item) is PostgresMssqlTypeDecisionAuthorityV1 for item in self.ordered_decisions):
            reject("invalid_facet")
        expected = tuple(sorted(self.ordered_decisions, key=lambda item: item.source_shape.sort_key))
        shapes = tuple(item.source_shape.canonical_bytes for item in self.ordered_decisions)
        if self.ordered_decisions != expected or len(set(shapes)) != len(shapes):
            reject("decision_order_invalid")

    @classmethod
    def create(
        cls,
        decisions: tuple[PostgresMssqlTypeDecisionAuthorityV1, ...],
        referenced_shapes: tuple[PostgresMssqlSourceScalarShapeV1, ...],
    ) -> PostgresMssqlTypePolicyAuthorityV1:
        if (
            type(decisions) is not tuple
            or type(referenced_shapes) is not tuple
            or not decisions
            or not referenced_shapes
            or len(referenced_shapes) > 1024
            or not all(type(item) is PostgresMssqlTypeDecisionAuthorityV1 for item in decisions)
            or not all(type(item) is PostgresMssqlSourceScalarShapeV1 for item in referenced_shapes)
        ):
            reject("policy_coverage_invalid")
        by_shape: dict[bytes, PostgresMssqlTypeDecisionAuthorityV1] = {}
        for decision in decisions:
            existing = by_shape.setdefault(decision.source_shape.canonical_bytes, decision)
            if existing != decision:
                reject("decision_duplicate")
        references = {shape.canonical_bytes for shape in referenced_shapes}
        if references != set(by_shape):
            reject("policy_coverage_invalid")
        return cls(_POLICY_VERSION, tuple(sorted(by_shape.values(), key=lambda item: item.source_shape.sort_key)))

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _POLICY, (self.policy_version, tuple(item.canonical_bytes for item in self.ordered_decisions))
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlTypePolicyAuthorityV1:
        version, decisions = decode_canonical_bytes(payload, _POLICY, field_count=2)
        return cls(
            expect_text(version, "policy version"),
            tuple(
                PostgresMssqlTypeDecisionAuthorityV1.from_canonical_bytes(expect_bytes(item, "decision"))
                for item in expect_tuple(decisions, "decisions")
            ),
        )

    def source_column_ref(
        self,
        *,
        ordinal: int,
        name: str,
        nullable: bool,
        source_shape: PostgresMssqlSourceScalarShapeV1,
    ) -> PostgresMssqlSourceColumnRefV1:
        """Issue a reference only when this policy owns the exact source shape."""

        if type(source_shape) is not PostgresMssqlSourceScalarShapeV1:
            reject("authority_splice")
        if source_shape.canonical_bytes not in {
            decision.source_shape.canonical_bytes for decision in self.ordered_decisions
        }:
            reject("policy_coverage_invalid")
        return PostgresMssqlSourceColumnRefV1._from_policy(
            ordinal,
            name,
            nullable,
            source_shape,
            self.digest,
        )

    def resolve_catalog_shape(
        self,
        type_namespace_oid: int,
        type_oid: int,
        type_modifier: int,
    ) -> PostgresMssqlSourceScalarShapeV1:
        """Resolve one exact built-in PostgreSQL catalog shape owned by this policy."""

        if (
            type(type_namespace_oid) is not int
            or type_namespace_oid != POSTGRES_PG_CATALOG_NAMESPACE_OID_V1
            or type(type_oid) is not int
            or not 0 < type_oid < 2**32
            or type(type_modifier) is not int
            or not -(2**31) <= type_modifier < 2**31
        ):
            reject("invalid_facet")
        matches = tuple(
            decision.source_shape
            for decision in self.ordered_decisions
            if decision.source_shape.source_type_oid == type_oid
            and decision.source_shape.source_typmod == type_modifier
        )
        if len(matches) != 1:
            raise _CatalogShapeResolutionError("policy_coverage_invalid")
        return matches[0]


@dataclass(frozen=True, slots=True, init=False)
class PostgresMssqlSourceColumnRefV1:
    ordinal: int
    name: str
    nullable: bool
    source_shape: PostgresMssqlSourceScalarShapeV1
    type_policy_digest: bytes

    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        reject("authority_splice")

    @classmethod
    def _from_policy(
        cls,
        ordinal: int,
        name: str,
        nullable: bool,
        source_shape: PostgresMssqlSourceScalarShapeV1,
        type_policy_digest: bytes,
    ) -> PostgresMssqlSourceColumnRefV1:
        value = object.__new__(cls)
        for field, item in zip(
            cls.__dataclass_fields__,
            (ordinal, name, nullable, source_shape, type_policy_digest),
            strict=True,
        ):
            object.__setattr__(value, field, item)
        value.__post_init__()
        return value

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or not 1 <= self.ordinal <= 1024 or type(self.nullable) is not bool:
            reject("ordinal_invalid")
        if type(self.name) is not str:
            reject("identifier_invalid")
        require_identifier_v1(self.name)
        if type(self.source_shape) is not PostgresMssqlSourceScalarShapeV1:
            reject("authority_splice")
        if type(self.type_policy_digest) is not bytes:
            reject("invalid_facet")
        require_digest(self.type_policy_digest, "type policy digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SOURCE_REF,
            (
                self.ordinal,
                self.name,
                self.nullable,
                self.source_shape.canonical_bytes,
                self.type_policy_digest,
            ),
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlSourceColumnRefV1:
        values = decode_canonical_bytes(payload, _SOURCE_REF, field_count=5)
        nullable = values[2]
        if type(nullable) is not bool:
            reject("invalid_facet")
        return cls._from_policy(
            expect_int(values[0], "ordinal"),
            expect_text(values[1], "name"),
            nullable,
            PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(expect_bytes(values[3], "source shape")),
            require_digest(values[4], "type policy digest"),
        )

    def validate_against_policy(self, policy: PostgresMssqlTypePolicyAuthorityV1) -> None:
        """Prove this decoded reference still belongs to the supplied policy."""

        if (
            type(policy) is not PostgresMssqlTypePolicyAuthorityV1
            or self.type_policy_digest != policy.digest
            or self.source_shape.canonical_bytes
            not in {decision.source_shape.canonical_bytes for decision in policy.ordered_decisions}
        ):
            reject("authority_splice")


__all__ = [
    "POSTGRES_PG_CATALOG_NAMESPACE_OID_V1",
    "PostgresMssqlTypePolicyAuthorityV1",
    "PostgresMssqlSourceColumnRefV1",
]
