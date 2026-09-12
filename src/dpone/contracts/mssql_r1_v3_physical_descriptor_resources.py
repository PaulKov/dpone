"""Typed resource, access and comparison-coordinate contracts."""

from __future__ import annotations

from dataclasses import dataclass

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_schema_primitives import require_schema_identifier


class MssqlR1ResourceKindV1(StrEnum):
    STATIC_OBJECT = "static_object"
    DYNAMIC_STAGE = "dynamic_stage"
    REGISTERED_TARGET = "registered_target"
    CATALOG = "catalog"
    SESSION = "session"
    PERMISSION = "permission"
    SIGNATURE = "signature"
    EXTENDED_PROPERTY = "extended_property"


class MssqlR1AccessKindV1(StrEnum):
    READ = "read"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    DDL = "ddl"
    EXECUTE = "execute"
    GRANT = "grant"
    REVOKE = "revoke"
    SIGN = "sign"
    ADD_PROPERTY = "add_property"
    DROP_PROPERTY = "drop_property"


class MssqlR1LockKindV1(StrEnum):
    SCHEMA = "schema"
    PHYSICAL = "physical"
    BINDING = "binding"
    OPERATION = "operation"
    ARTIFACT = "artifact"
    REGISTRATION = "registration"
    AUTHORITY = "authority"


class MssqlR1LockCardinalityV1(StrEnum):
    ONE = "one"
    EXACT_REQUEST_SET = "exact_request_set"


class MssqlR1ProjectionScalarKindV1(StrEnum):
    TEXT = "text"
    BINARY = "binary"
    DIGEST = "digest"
    UUID = "uuid"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    UTC = "utc"


class MssqlR1ValueCardinalityV1(StrEnum):
    SCALAR = "scalar"
    ORDERED_SET = "ordered_set"


_REF = b"dpone-r1-physical-resource-ref-v1\0"
_FIELD = b"dpone-r1-physical-resource-field-v1\0"
_ACCESS = b"dpone-r1-physical-resource-access-v1\0"
_DECLARATION = b"dpone-r1-physical-resource-declaration-v1\0"
_COORDINATE = b"dpone-r1-physical-comparison-coordinate-v1\0"
_LITERAL = b"dpone-r1-physical-comparison-literal-v1\0"
_SELECTOR = b"dpone-r1-physical-resource-instance-selector-v1\0"

ACCESS_MATRIX = {
    MssqlR1ResourceKindV1.STATIC_OBJECT: {
        "read",
        "insert",
        "update",
        "delete",
        "ddl",
        "execute",
        "add_property",
        "drop_property",
    },
    MssqlR1ResourceKindV1.DYNAMIC_STAGE: {
        "read",
        "insert",
        "update",
        "delete",
        "ddl",
        "grant",
        "revoke",
        "add_property",
        "drop_property",
    },
    MssqlR1ResourceKindV1.REGISTERED_TARGET: {"read", "insert", "update", "delete"},
    MssqlR1ResourceKindV1.CATALOG: {"read"},
    MssqlR1ResourceKindV1.SESSION: {"read", "update"},
    MssqlR1ResourceKindV1.PERMISSION: {"read", "grant", "revoke"},
    MssqlR1ResourceKindV1.SIGNATURE: {"read", "sign"},
    MssqlR1ResourceKindV1.EXTENDED_PROPERTY: {"read", "add_property", "drop_property"},
}

_SQL_SCALARS = {
    "char": MssqlR1ProjectionScalarKindV1.TEXT,
    "varchar": MssqlR1ProjectionScalarKindV1.TEXT,
    "nchar": MssqlR1ProjectionScalarKindV1.TEXT,
    "nvarchar": MssqlR1ProjectionScalarKindV1.TEXT,
    "binary": MssqlR1ProjectionScalarKindV1.BINARY,
    "varbinary": MssqlR1ProjectionScalarKindV1.BINARY,
    "uniqueidentifier": MssqlR1ProjectionScalarKindV1.UUID,
    "tinyint": MssqlR1ProjectionScalarKindV1.INTEGER,
    "smallint": MssqlR1ProjectionScalarKindV1.INTEGER,
    "int": MssqlR1ProjectionScalarKindV1.INTEGER,
    "bigint": MssqlR1ProjectionScalarKindV1.INTEGER,
    "bit": MssqlR1ProjectionScalarKindV1.BOOLEAN,
    "datetime2": MssqlR1ProjectionScalarKindV1.UTC,
    "datetimeoffset": MssqlR1ProjectionScalarKindV1.UTC,
}


def compatible_sql_scalar_kinds(sql_type: str, maximum_length: int) -> frozenset[MssqlR1ProjectionScalarKindV1]:
    """Return the closed physical scalar projection for one portable SQL value."""

    expected = _SQL_SCALARS.get(sql_type)
    if expected is None:
        return frozenset()
    if sql_type == "binary" and maximum_length == 32:
        return frozenset({MssqlR1ProjectionScalarKindV1.BINARY, MssqlR1ProjectionScalarKindV1.DIGEST})
    return frozenset({expected})


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalResourceRefV1:
    resource_kind: MssqlR1ResourceKindV1
    schema_name: str | None
    object_name: str | None
    coordinate_name: str | None

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.resource_kind, MssqlR1ResourceKindV1, "resource kind")
        is_static = self.resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT
        if is_static != (self.schema_name is not None and self.object_name is not None):
            raise MssqlR1V3ContractError("resource coordinate shape differs from its kind")
        if is_static:
            require_schema_identifier(self.schema_name, "resource schema")
            require_schema_identifier(self.object_name, "resource object")
            if self.coordinate_name is not None:
                raise MssqlR1V3ContractError("static resource cannot carry a coordinate name")
        else:
            if self.schema_name is not None or self.object_name is not None:
                raise MssqlR1V3ContractError("template resource cannot carry schema/object")
            _VALIDATE.require_text(self.coordinate_name, "resource coordinate")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_REF, (self.resource_kind, self.schema_name, self.object_name, self.coordinate_name))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalResourceRefV1:
        values = list(decode_canonical_bytes(payload, _REF, field_count=4))
        values[0] = expect_enum(MssqlR1ResourceKindV1, values[0], "resource kind")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ResourceFieldV1:
    ordinal: int
    name: str
    scalar_kind: MssqlR1ProjectionScalarKindV1
    value_cardinality: MssqlR1ValueCardinalityV1
    nullable: bool
    instance_key_ordinal: int | None

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "resource field ordinal")
        require_schema_identifier(self.name, "resource field name")
        _VALIDATE.require_exact_enum(self.scalar_kind, MssqlR1ProjectionScalarKindV1, "resource scalar kind")
        _VALIDATE.require_exact_enum(self.value_cardinality, MssqlR1ValueCardinalityV1, "resource value cardinality")
        _VALIDATE.require_bool(self.nullable, "resource field nullability")
        if self.instance_key_ordinal is not None:
            _VALIDATE.require_ordinal(self.instance_key_ordinal, "instance-key ordinal")
            if self.nullable or self.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR:
                raise MssqlR1V3ContractError("instance-key field must be non-null scalar")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _FIELD,
            (
                self.ordinal,
                self.name,
                self.scalar_kind,
                self.value_cardinality,
                self.nullable,
                self.instance_key_ordinal,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ResourceFieldV1:
        values = list(decode_canonical_bytes(payload, _FIELD, field_count=6))
        values[2] = expect_enum(MssqlR1ProjectionScalarKindV1, values[2], "resource scalar kind")
        values[3] = expect_enum(MssqlR1ValueCardinalityV1, values[3], "resource value cardinality")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalResourceAccessV1:
    resource: MssqlR1PhysicalResourceRefV1
    access_kind: MssqlR1AccessKindV1

    def __post_init__(self) -> None:
        if type(self.resource) is not MssqlR1PhysicalResourceRefV1:
            raise MssqlR1V3ContractError("resource access requires an exact resource")
        _VALIDATE.require_exact_enum(self.access_kind, MssqlR1AccessKindV1, "access kind")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_ACCESS, (self.resource.canonical_bytes, self.access_kind))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalResourceAccessV1:
        resource, access = decode_canonical_bytes(payload, _ACCESS, field_count=2)
        return cls(
            MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(resource, "resource")),
            expect_enum(MssqlR1AccessKindV1, access, "access kind"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalResourceDeclarationV1:
    resource: MssqlR1PhysicalResourceRefV1
    associated_static_object: MssqlR1PhysicalResourceRefV1 | None
    ordered_fields: tuple[MssqlR1ResourceFieldV1, ...]
    ordered_allowed_access_kinds: tuple[MssqlR1AccessKindV1, ...]
    allowed_lock_kind: MssqlR1LockKindV1 | None
    lock_subrank: int
    lock_cardinality: MssqlR1LockCardinalityV1

    def __post_init__(self) -> None:
        if type(self.resource) is not MssqlR1PhysicalResourceRefV1:
            raise MssqlR1V3ContractError("resource declaration requires an exact resource")
        if self.associated_static_object is not None and (
            type(self.associated_static_object) is not MssqlR1PhysicalResourceRefV1
            or self.associated_static_object.resource_kind is not MssqlR1ResourceKindV1.STATIC_OBJECT
        ):
            raise MssqlR1V3ContractError("associated object must be an exact static resource")
        if (
            self.resource.resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT
            and self.associated_static_object != self.resource
        ):
            raise MssqlR1V3ContractError("static resource must associate to itself")
        _VALIDATE.require_tuple(self.ordered_fields, MssqlR1ResourceFieldV1, "resource fields")
        _VALIDATE.require_contiguous(self.ordered_fields, "resource fields")
        if len({field.name.casefold() for field in self.ordered_fields}) != len(self.ordered_fields):
            raise MssqlR1V3ContractError("resource fields contain a case-fold collision")
        key_ordinals = tuple(field.instance_key_ordinal for field in self.ordered_fields if field.instance_key_ordinal)
        if key_ordinals != tuple(range(1, len(key_ordinals) + 1)):
            raise MssqlR1V3ContractError("instance-key ordinals must be contiguous")
        if type(self.ordered_allowed_access_kinds) is not tuple or not self.ordered_allowed_access_kinds:
            raise MssqlR1V3ContractError("allowed access kinds must be a nonempty tuple")
        if any(type(item) is not MssqlR1AccessKindV1 for item in self.ordered_allowed_access_kinds):
            raise MssqlR1V3ContractError("allowed access kind uses a raw discriminator")
        if self.ordered_allowed_access_kinds != tuple(
            sorted(self.ordered_allowed_access_kinds, key=lambda item: item.value)
        ) or len(set(self.ordered_allowed_access_kinds)) != len(self.ordered_allowed_access_kinds):
            raise MssqlR1V3ContractError("allowed access kinds must use canonical order")
        if not {item.value for item in self.ordered_allowed_access_kinds} <= ACCESS_MATRIX[self.resource.resource_kind]:
            raise MssqlR1V3ContractError("resource declaration widens the closed access matrix")
        _VALIDATE.require_int(self.lock_subrank, "lock subrank", maximum=3)
        _VALIDATE.require_exact_enum(self.lock_cardinality, MssqlR1LockCardinalityV1, "lock cardinality")
        if self.allowed_lock_kind is None:
            if self.lock_subrank != 0 or self.lock_cardinality is not MssqlR1LockCardinalityV1.ONE:
                raise MssqlR1V3ContractError("unlocked resource has a noncanonical lock shape")
        else:
            _VALIDATE.require_exact_enum(self.allowed_lock_kind, MssqlR1LockKindV1, "allowed lock kind")
            if (self.allowed_lock_kind in {MssqlR1LockKindV1.ARTIFACT, MssqlR1LockKindV1.AUTHORITY}) != (
                self.lock_subrank in {1, 2, 3}
            ):
                raise MssqlR1V3ContractError("lock subrank differs from its lock kind")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DECLARATION,
            (
                self.resource.canonical_bytes,
                None if self.associated_static_object is None else self.associated_static_object.canonical_bytes,
                tuple(field.canonical_bytes for field in self.ordered_fields),
                self.ordered_allowed_access_kinds,
                self.allowed_lock_kind,
                self.lock_subrank,
                self.lock_cardinality,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalResourceDeclarationV1:
        values = list(decode_canonical_bytes(payload, _DECLARATION, field_count=7))
        values[0] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[0], "resource"))
        if values[1] is not None:
            values[1] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[1], "association"))
        values[2] = tuple(
            MssqlR1ResourceFieldV1.from_canonical_bytes(expect_bytes(item, "resource field"))
            for item in expect_tuple(values[2], "resource fields")
        )
        values[3] = tuple(
            expect_enum(MssqlR1AccessKindV1, item, "access kind")
            for item in expect_tuple(values[3], "allowed access kinds")
        )
        if values[4] is not None:
            values[4] = expect_enum(MssqlR1LockKindV1, values[4], "lock kind")
        values[6] = expect_enum(MssqlR1LockCardinalityV1, values[6], "lock cardinality")
        return cls(*values)  # type: ignore[arg-type]


__all__ = (
    "ACCESS_MATRIX",
    "MssqlR1PhysicalResourceAccessV1",
    "MssqlR1PhysicalResourceDeclarationV1",
    "MssqlR1PhysicalResourceRefV1",
    "MssqlR1ResourceFieldV1",
    "compatible_sql_scalar_kinds",
)


def validate_resource_inventory(values: tuple[MssqlR1PhysicalResourceDeclarationV1, ...]) -> None:
    """Require a nonempty, exact and canonically ordered resource inventory."""
    _VALIDATE.require_tuple(values, MssqlR1PhysicalResourceDeclarationV1, "resource declarations", nonempty=True)
    _VALIDATE.require_canonical(values, "resource declarations")
