"""Typed comparison coordinates, literals, existence operands, and selectors."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    MAX_SQL_BIGINT,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
    parse_canonical_utc_text,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1ComparisonSourceV1,
    MssqlR1ResourceInstanceSelectorKindV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ValueCardinalityV1,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import require_schema_identifier

_COORDINATE = b"dpone-r1-physical-comparison-coordinate-v1\0"
_LITERAL = b"dpone-r1-physical-comparison-literal-v1\0"
_SELECTOR = b"dpone-r1-physical-resource-instance-selector-v1\0"
_EXISTENCE = b"dpone-r1-physical-resource-existence-operand-v1\0"


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1ComparisonCoordinateV1:
    source: MssqlR1ComparisonSourceV1
    resource: MssqlR1PhysicalResourceRefV1 | None
    field_name: str
    scalar_kind: MssqlR1ProjectionScalarKindV1
    value_cardinality: MssqlR1ValueCardinalityV1
    nullable: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.source, MssqlR1ComparisonSourceV1, "comparison source")
        require_schema_identifier(self.field_name, "comparison field")
        _VALIDATE.require_exact_enum(self.scalar_kind, MssqlR1ProjectionScalarKindV1, "comparison scalar kind")
        _VALIDATE.require_exact_enum(self.value_cardinality, MssqlR1ValueCardinalityV1, "comparison cardinality")
        _VALIDATE.require_bool(self.nullable, "comparison nullability")
        needs_resource = self.source in {
            MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.DESCENDANT_RECEIPT,
            MssqlR1ComparisonSourceV1.SESSION_BINDING,
        }
        if (needs_resource and type(self.resource) is not MssqlR1PhysicalResourceRefV1) or (
            not needs_resource and self.resource is not None
        ):
            raise MssqlR1V3ContractError("comparison source/resource shape is invalid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _COORDINATE,
            (
                self.source,
                None if self.resource is None else self.resource.canonical_bytes,
                self.field_name,
                self.scalar_kind,
                self.value_cardinality,
                self.nullable,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ComparisonCoordinateV1:
        values = list(decode_canonical_bytes(payload, _COORDINATE, field_count=6))
        values[0] = expect_enum(MssqlR1ComparisonSourceV1, values[0], "comparison source")
        if values[1] is not None:
            values[1] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[1], "resource"))
        values[3] = expect_enum(MssqlR1ProjectionScalarKindV1, values[3], "scalar kind")
        values[4] = expect_enum(MssqlR1ValueCardinalityV1, values[4], "value cardinality")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ComparisonLiteralV1:
    scalar_kind: MssqlR1ProjectionScalarKindV1
    value: str | bytes | int | bool

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.scalar_kind, MssqlR1ProjectionScalarKindV1, "literal scalar kind")
        if self.scalar_kind in {
            MssqlR1ProjectionScalarKindV1.TEXT,
            MssqlR1ProjectionScalarKindV1.UUID,
            MssqlR1ProjectionScalarKindV1.UTC,
        }:
            _VALIDATE.require_text(self.value, "literal value")
        if self.scalar_kind is MssqlR1ProjectionScalarKindV1.UUID:
            try:
                if str(UUID(self.value)) != self.value:  # type: ignore[arg-type]
                    raise ValueError
            except (ValueError, AttributeError) as exc:
                raise MssqlR1V3ContractError("UUID literal is not canonical") from exc
        elif self.scalar_kind is MssqlR1ProjectionScalarKindV1.UTC:
            parse_canonical_utc_text(self.value, "UTC literal")
        elif self.scalar_kind is MssqlR1ProjectionScalarKindV1.DIGEST:
            if type(self.value) is not bytes or len(self.value) != 32:
                raise MssqlR1V3ContractError("digest literal must be 32 bytes")
        elif self.scalar_kind is MssqlR1ProjectionScalarKindV1.BINARY and type(self.value) is not bytes:
            raise MssqlR1V3ContractError("binary literal must be bytes")
        elif self.scalar_kind is MssqlR1ProjectionScalarKindV1.INTEGER:
            _VALIDATE.require_int(self.value, "integer literal", minimum=-MAX_SQL_BIGINT - 1)
        elif self.scalar_kind is MssqlR1ProjectionScalarKindV1.BOOLEAN:
            _VALIDATE.require_bool(self.value, "boolean literal")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_LITERAL, (self.scalar_kind, self.value))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ComparisonLiteralV1:
        kind, value = decode_canonical_bytes(payload, _LITERAL, field_count=2)
        return cls(expect_enum(MssqlR1ProjectionScalarKindV1, kind, "literal kind"), value)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ResourceInstanceSelectorV1:
    selector_kind: MssqlR1ResourceInstanceSelectorKindV1
    ordered_coordinates: tuple[MssqlR1ComparisonCoordinateV1, ...]
    allow_empty: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.selector_kind, MssqlR1ResourceInstanceSelectorKindV1, "selector kind")
        _VALIDATE.require_tuple(self.ordered_coordinates, MssqlR1ComparisonCoordinateV1, "selector coordinates")
        _VALIDATE.require_bool(self.allow_empty, "selector allow-empty")
        count = len(self.ordered_coordinates)
        if self.selector_kind is MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON:
            if count or self.allow_empty:
                raise MssqlR1V3ContractError("declared-singleton selector has a noncanonical shape")
        elif self.selector_kind is MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE:
            if count == 0 or any(
                item.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR for item in self.ordered_coordinates
            ):
                raise MssqlR1V3ContractError("single-value selector requires scalar coordinates")
        elif count == 0 or any(
            item.value_cardinality is not MssqlR1ValueCardinalityV1.ORDERED_SET for item in self.ordered_coordinates
        ):
            raise MssqlR1V3ContractError("ordered-values selector requires ordered-set coordinates")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SELECTOR,
            (self.selector_kind, tuple(item.canonical_bytes for item in self.ordered_coordinates), self.allow_empty),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ResourceInstanceSelectorV1:
        kind, coordinates, allow_empty = decode_canonical_bytes(payload, _SELECTOR, field_count=3)
        return cls(
            expect_enum(MssqlR1ResourceInstanceSelectorKindV1, kind, "selector kind"),
            tuple(
                MssqlR1ComparisonCoordinateV1.from_canonical_bytes(expect_bytes(item, "selector coordinate"))
                for item in expect_tuple(coordinates, "selector coordinates")
            ),
            allow_empty,  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ResourceExistenceOperandV1:
    resource: MssqlR1PhysicalResourceRefV1
    instance_selector: MssqlR1ResourceInstanceSelectorV1

    def __post_init__(self) -> None:
        if (
            type(self.resource) is not MssqlR1PhysicalResourceRefV1
            or type(self.instance_selector) is not MssqlR1ResourceInstanceSelectorV1
        ):
            raise MssqlR1V3ContractError("existence operand requires exact resource and selector")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_EXISTENCE, (self.resource.canonical_bytes, self.instance_selector.canonical_bytes))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ResourceExistenceOperandV1:
        resource, selector = decode_canonical_bytes(payload, _EXISTENCE, field_count=2)
        return cls(
            MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(resource, "resource")),
            MssqlR1ResourceInstanceSelectorV1.from_canonical_bytes(expect_bytes(selector, "selector")),
        )


__all__ = (
    "MssqlR1ComparisonCoordinateV1",
    "MssqlR1ComparisonLiteralV1",
    "MssqlR1ResourceExistenceOperandV1",
    "MssqlR1ResourceInstanceSelectorV1",
)
