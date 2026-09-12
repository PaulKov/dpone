"""Portable schema-2 constraint and index relations."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1ConstraintKindV3,
    MssqlR1ExtendedPropertyV3,
    MssqlR1IndexDirectionV3,
    MssqlR1ResultCardinalityV3,
    MssqlR1ResultColumnV3,
    MssqlR1SchemaColumnV3,
    MssqlR1SchemaObjectKindV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SignerProfileKindV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    decode_members,
    expect_bytes,
    expect_enum,
    field_values,
    require_canonical_named_set,
    require_canonical_set,
    require_contiguous,
    require_digest,
    require_identifiers,
    require_module_options,
    require_schema_identifier,
)


@dataclass(frozen=True, slots=True)
class MssqlR1ReferencedObjectV3:
    schema_name: str
    object_name: str
    ordered_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        require_schema_identifier(self.schema_name, "referenced schema")
        require_schema_identifier(self.object_name, "referenced object")
        require_identifiers(self.ordered_columns, "referenced columns", required=True)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-referenced-object-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ReferencedObjectV3:
        schema, object_name, columns = decode_canonical_bytes(
            payload,
            b"dpone-r1-schema-referenced-object-v3-schema-2\0",
            field_count=3,
        )
        return cls(schema, object_name, columns)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaConstraintV3:
    name: str
    kind: MssqlR1ConstraintKindV3
    ordered_columns: tuple[str, ...]
    referenced_object: MssqlR1ReferencedObjectV3 | None
    definition_digest: bytes
    trusted: bool
    enabled: bool

    def __post_init__(self) -> None:
        require_schema_identifier(self.name, "constraint name")
        require_identifiers(self.ordered_columns, "constraint columns", required=True)
        if not isinstance(self.kind, MssqlR1ConstraintKindV3):
            raise MssqlR1V3ContractError("constraint kind is unsupported")
        if self.kind is MssqlR1ConstraintKindV3.FOREIGN_KEY:
            reference_is_valid = isinstance(self.referenced_object, MssqlR1ReferencedObjectV3)
        else:
            reference_is_valid = self.referenced_object is None
        if not reference_is_valid:
            raise MssqlR1V3ContractError("constraint referenced object is inconsistent")
        require_digest(self.definition_digest, "constraint definition digest")
        if not isinstance(self.trusted, bool) or not isinstance(self.enabled, bool):
            raise MssqlR1V3ContractError("constraint flags must be boolean")

    @property
    def canonical_bytes(self) -> bytes:
        reference = None if self.referenced_object is None else self.referenced_object.canonical_bytes
        return canonical_bytes(
            b"dpone-r1-schema-constraint-v3-schema-2\0",
            (
                self.name,
                self.kind,
                self.ordered_columns,
                reference,
                self.definition_digest,
                self.trusted,
                self.enabled,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaConstraintV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-schema-constraint-v3-schema-2\0", field_count=7))
        values[1] = expect_enum(MssqlR1ConstraintKindV3, values[1], "constraint kind")
        if values[3] is not None:
            values[3] = MssqlR1ReferencedObjectV3.from_canonical_bytes(expect_bytes(values[3], "reference"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaIndexKeyV3:
    column_name: str
    direction: MssqlR1IndexDirectionV3

    def __post_init__(self) -> None:
        require_schema_identifier(self.column_name, "index key column")
        if not isinstance(self.direction, MssqlR1IndexDirectionV3):
            raise MssqlR1V3ContractError("index direction is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-index-key-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaIndexKeyV3:
        column, direction = decode_canonical_bytes(
            payload,
            b"dpone-r1-schema-index-key-v3-schema-2\0",
            field_count=2,
        )
        return cls(column, expect_enum(MssqlR1IndexDirectionV3, direction, "index direction"))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaIndexV3:
    name: str
    unique: bool
    clustered: bool
    ordered_keys: tuple[MssqlR1SchemaIndexKeyV3, ...]
    ordered_include_columns: tuple[str, ...]
    filter_definition_digest: bytes | None
    enabled: bool

    def __post_init__(self) -> None:
        require_schema_identifier(self.name, "index name")
        if not all(isinstance(value, bool) for value in (self.unique, self.clustered, self.enabled)):
            raise MssqlR1V3ContractError("index flags must be boolean")
        if (
            not isinstance(self.ordered_keys, tuple)
            or not self.ordered_keys
            or not all(isinstance(item, MssqlR1SchemaIndexKeyV3) for item in self.ordered_keys)
        ):
            raise MssqlR1V3ContractError("index keys must be a nonempty typed tuple")
        keys = tuple(item.column_name for item in self.ordered_keys)
        includes = require_identifiers(self.ordered_include_columns, "index include columns")
        if len(set(keys)) != len(keys) or set(keys) & set(includes):
            raise MssqlR1V3ContractError("index key/include columns are invalid")
        if self.filter_definition_digest is not None:
            require_digest(self.filter_definition_digest, "filter definition digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-index-v3-schema-2\0",
            (
                self.name,
                self.unique,
                self.clustered,
                tuple(item.canonical_bytes for item in self.ordered_keys),
                self.ordered_include_columns,
                self.filter_definition_digest,
                self.enabled,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaIndexV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-schema-index-v3-schema-2\0", field_count=7))
        values[3] = decode_members(values[3], MssqlR1SchemaIndexKeyV3, "index key")
        return cls(*values)  # type: ignore[arg-type]


__all__ = [
    "MssqlR1ExtendedPropertyV3",
    "MssqlR1ReferencedObjectV3",
    "MssqlR1ResultCardinalityV3",
    "MssqlR1ResultColumnV3",
    "MssqlR1SchemaColumnV3",
    "MssqlR1SchemaConstraintV3",
    "MssqlR1SchemaIndexKeyV3",
    "MssqlR1SchemaIndexV3",
    "MssqlR1SchemaObjectKindV3",
    "MssqlR1SchemaProcedureParameterV3",
    "MssqlR1SignerProfileKindV3",
    "require_canonical_set",
    "require_canonical_named_set",
    "require_contiguous",
    "require_module_options",
]
