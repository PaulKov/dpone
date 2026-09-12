"""Column, index and closed-feature leaves for the registered R1 target."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bool,
    expect_bytes,
    expect_int,
    expect_text,
    expect_tuple,
    require_digest,
    require_positive,
)
from dpone.contracts.postgres_mssql_type_target_enums import MssqlR1TargetScalarFamilyV1 as Target
from dpone.contracts.postgres_mssql_type_target_enums import (
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import MssqlR1CanonicalTargetScalarShapeV1

_COLUMN = b"dpone-mssql-r1-registered-target-column-v1\0"
_INDEX = b"dpone-mssql-r1-registered-target-index-v1\0"
_FEATURE = b"dpone-mssql-r1-closed-target-feature-observation-v1\0"


def _optional_bytes(value: object, field: str) -> bytes | None:
    if value is None:
        return None
    if type(value) is not bytes:
        reject("invalid_facet")
    return require_digest(value, field)


def _int_tuple(value: object, field: str) -> tuple[int, ...]:
    return tuple(expect_int(item, field) for item in expect_tuple(value, field))


@dataclass(frozen=True, slots=True)
class MssqlR1RegisteredTargetColumnV1:
    ordinal: int
    name: str
    system_type_schema: str
    system_type_name: str
    user_type_schema: str
    user_type_name: str
    scalar_shape: MssqlR1CanonicalTargetScalarShapeV1
    max_length: int
    precision: int
    scale: int
    nullable: bool
    identity: bool
    computed: bool
    sparse: bool
    rowguidcol: bool
    generated_always_type: int
    default_definition_digest: bytes | None
    computed_definition_digest: bytes | None

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or not 1 <= self.ordinal <= 1024:
            reject("ordinal_invalid")
        require_positive(self.ordinal, "column ordinal")
        if type(self.scalar_shape) is not MssqlR1CanonicalTargetScalarShapeV1:
            reject("catalog_behavior_mismatch")
        if type(self.name) is not str:
            reject("identifier_invalid")
        require_identifier_v1(self.name)
        if any(
            type(value) is not str
            for value in (self.system_type_schema, self.system_type_name, self.user_type_schema, self.user_type_name)
        ):
            reject("invalid_facet")
        if self.system_type_schema != "sys" or self.user_type_schema != "sys":
            reject("catalog_behavior_mismatch", operator=True)
        expected_name = "float" if self.scalar_shape.family is Target.FLOAT_53 else self.scalar_shape.family.value
        if self.system_type_name != expected_name or self.user_type_name != expected_name:
            reject("catalog_behavior_mismatch", operator=True)
        if (self.max_length, self.precision, self.scale) != _catalog_facets(self.scalar_shape):
            reject("catalog_behavior_mismatch", operator=True)
        if any(
            type(value) is not int
            for value in (self.max_length, self.precision, self.scale, self.generated_always_type)
        ):
            reject("invalid_facet")
        flags = (self.nullable, self.identity, self.computed, self.sparse, self.rowguidcol)
        if not all(type(value) is bool for value in flags):
            reject("invalid_facet")
        if any(flags[1:]) or self.generated_always_type != 0:
            reject("target_feature_unsupported", operator=True)
        if self.default_definition_digest is not None or self.computed_definition_digest is not None:
            reject("target_feature_unsupported", operator=True)

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(
            self.scalar_shape.canonical_bytes if name == "scalar_shape" else getattr(self, name)
            for name in self.__dataclass_fields__
        )
        return canonical_bytes(_COLUMN, values)

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegisteredTargetColumnV1:
        values = list(decode_canonical_bytes(payload, _COLUMN, field_count=18))
        values[6] = MssqlR1CanonicalTargetScalarShapeV1.from_canonical_bytes(expect_bytes(values[6], "shape"))
        for index in (0, 7, 8, 9, 15):
            values[index] = expect_int(values[index], "column integer")
        for index in (1, 2, 3, 4, 5):
            values[index] = expect_text(values[index], "column text")
        for index in (10, 11, 12, 13, 14):
            values[index] = expect_bool(values[index], "column flag")
        values[16] = _optional_bytes(values[16], "default digest")
        values[17] = _optional_bytes(values[17], "computed digest")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RegisteredTargetIndexV1:
    ordinal: int
    name: str
    index_kind: str
    unique: bool
    primary_key: bool
    unique_constraint: bool
    disabled: bool
    hypothetical: bool
    ignore_dup_key: bool
    filter_definition_digest: bytes | None
    ordered_key_column_ordinals: tuple[int, ...]
    ordered_descending: tuple[bool, ...]
    ordered_included_column_ordinals: tuple[int, ...]

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or not 1 <= self.ordinal <= 1000:
            reject("ordinal_invalid")
        require_positive(self.ordinal, "index ordinal")
        if type(self.name) is not str or type(self.index_kind) is not str:
            reject("identifier_invalid")
        require_identifier_v1(self.name)
        if self.index_kind not in {"clustered", "nonclustered"}:
            reject("catalog_behavior_mismatch", operator=True)
        flags = (
            self.unique,
            self.primary_key,
            self.unique_constraint,
            self.disabled,
            self.hypothetical,
            self.ignore_dup_key,
        )
        if not all(type(value) is bool for value in flags):
            reject("invalid_facet")
        if self.disabled or self.hypothetical or self.ignore_dup_key or self.filter_definition_digest is not None:
            reject("target_feature_unsupported", operator=True)
        keys, descending, included = (
            self.ordered_key_column_ordinals,
            self.ordered_descending,
            self.ordered_included_column_ordinals,
        )
        if (
            type(keys) is not tuple
            or not keys
            or not all(type(item) is int and item > 0 for item in keys)
            or len(set(keys)) != len(keys)
            or type(descending) is not tuple
            or len(descending) != len(keys)
            or not all(type(item) is bool for item in descending)
            or type(included) is not tuple
            or not all(type(item) is int and item > 0 for item in included)
            or len(set(included)) != len(included)
            or set(keys) & set(included)
            or (self.unique_constraint and not self.unique)
        ):
            reject("catalog_order_invalid")
        for ordinal in (*keys, *included):
            require_positive(ordinal, "index column ordinal")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_INDEX, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegisteredTargetIndexV1:
        values = list(decode_canonical_bytes(payload, _INDEX, field_count=13))
        values[0], values[1], values[2] = (
            expect_int(values[0], "ordinal"),
            expect_text(values[1], "name"),
            expect_text(values[2], "kind"),
        )
        for index in range(3, 9):
            values[index] = expect_bool(values[index], "index flag")
        values[9] = _optional_bytes(values[9], "filter digest")
        values[10] = _int_tuple(values[10], "key ordinal")
        values[11] = tuple(expect_bool(item, "descending") for item in expect_tuple(values[11], "descending"))
        values[12] = _int_tuple(values[12], "included ordinal")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ClosedTargetFeatureObservationV1:
    temporal_type: int
    ledger_type: int
    memory_optimized: bool
    durability_desc: str
    filetable: bool
    graph_node: bool
    graph_edge: bool
    ordered_trigger_digests: tuple[bytes, ...]
    ordered_inbound_foreign_key_digests: tuple[bytes, ...]
    ordered_outbound_foreign_key_digests: tuple[bytes, ...]
    ordered_check_constraint_digests: tuple[bytes, ...]
    ordered_indexed_view_dependency_digests: tuple[bytes, ...]
    ordered_encrypted_column_ordinals: tuple[int, ...]

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.temporal_type) is not int or type(self.ledger_type) is not int:
            reject("invalid_facet")
        if type(self.durability_desc) is not str:
            reject("invalid_facet")
        if not all(
            type(value) is bool for value in (self.memory_optimized, self.filetable, self.graph_node, self.graph_edge)
        ):
            reject("invalid_facet")
        head = (
            self.temporal_type,
            self.ledger_type,
            self.memory_optimized,
            self.durability_desc,
            self.filetable,
            self.graph_node,
            self.graph_edge,
        )
        if head != (0, 0, False, "SCHEMA_AND_DATA", False, False, False):
            reject("target_feature_unsupported", operator=True)
        if any(
            type(value) is not tuple or value
            for value in tuple(getattr(self, name) for name in tuple(self.__dataclass_fields__)[7:])
        ):
            reject("target_feature_unsupported", operator=True)

    @classmethod
    def empty(cls) -> MssqlR1ClosedTargetFeatureObservationV1:
        return cls(0, 0, False, "SCHEMA_AND_DATA", False, False, False, (), (), (), (), (), ())

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_FEATURE, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ClosedTargetFeatureObservationV1:
        values = list(decode_canonical_bytes(payload, _FEATURE, field_count=13))
        values[0], values[1] = expect_int(values[0], "temporal type"), expect_int(values[1], "ledger type")
        values[2], values[3] = expect_bool(values[2], "memory optimized"), expect_text(values[3], "durability")
        for index in (4, 5, 6):
            values[index] = expect_bool(values[index], "feature flag")
        for index in range(7, 12):
            values[index] = tuple(
                require_digest(item, "feature digest") for item in expect_tuple(values[index], "digests")
            )
        values[12] = _int_tuple(values[12], "encrypted ordinal")
        return cls(*values)  # type: ignore[arg-type]


def _catalog_facets(shape: MssqlR1CanonicalTargetScalarShapeV1) -> tuple[int, int, int]:
    precision = {
        Target.BIT: 1,
        Target.SMALLINT: 5,
        Target.INT: 10,
        Target.BIGINT: 19,
        Target.REAL: 24,
        Target.FLOAT_53: 53,
        Target.UNIQUEIDENTIFIER: 0,
        Target.DATE: 10,
        Target.NVARCHAR: 0,
        Target.VARBINARY: 0,
        Target.TIME: 16,
        Target.DATETIME2: 27,
        Target.DATETIMEOFFSET: 34,
    }.get(shape.family, shape.precision)
    scale = (
        shape.scale
        if shape.family is Target.DECIMAL
        else shape.precision
        if shape.family in {Target.TIME, Target.DATETIME2, Target.DATETIMEOFFSET}
        else 0
    )
    assert precision is not None and scale is not None and shape.maximum_bytes is not None
    return shape.maximum_bytes, precision, scale


__all__ = [
    "MssqlR1ClosedTargetFeatureObservationV1",
    "MssqlR1RegisteredTargetColumnV1",
    "MssqlR1RegisteredTargetIndexV1",
]
