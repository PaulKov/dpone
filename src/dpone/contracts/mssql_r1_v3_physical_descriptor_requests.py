"""Request, projection and outcome contracts for physical modules."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ComparisonCoordinateV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1ComparisonSourceV1,
    MssqlR1ProjectionRoleV1,
    MssqlR1RequestBindingKindV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceDeclarationV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ResourceKindV1,
    MssqlR1ValueCardinalityV1,
    compatible_sql_scalar_kinds,
)
from dpone.contracts.mssql_r1_v3_schema_modules import (
    MssqlR1FixedResultV3,
    MssqlR1ProcedureResultContractV3,
    MssqlR1StageScanTemplateV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1ResultColumnV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SupportedCodecEntryV3,
    require_schema_identifier,
)

_FIELD = b"dpone-r1-physical-projection-field-v1\0"
_GRAMMAR = b"dpone-r1-physical-projection-grammar-v1\0"
_BINDING = b"dpone-r1-physical-scalar-parameter-binding-v1\0"
_REQUEST = b"dpone-r1-physical-request-authority-v1\0"
_OUTCOME = b"dpone-r1-physical-outcome-variant-v1\0"


def _require_casefold_unique(values: tuple[str, ...], field: str) -> None:
    if len({value.casefold() for value in values}) != len(values):
        raise MssqlR1V3ContractError(f"{field} contain a case-fold collision")


def exact_result_prefix() -> tuple[MssqlR1ResultColumnV3, ...]:
    """Return the five-column physical result prefix frozen by the parent contract."""

    return (
        MssqlR1ResultColumnV3(1, "result_contract_version", "varchar", 64, 0, 0, False, "Latin1_General_100_BIN2"),
        MssqlR1ResultColumnV3(2, "outcome", "varchar", 32, 0, 0, False, "Latin1_General_100_BIN2"),
        MssqlR1ResultColumnV3(3, "request_digest", "binary", 32, 0, 0, False, None),
        MssqlR1ResultColumnV3(4, "projection_revision", "bigint", 8, 19, 0, False, None),
        MssqlR1ResultColumnV3(5, "server_observed_at", "datetime2", 8, 27, 7, False, None),
    )


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1ProjectionFieldV1:
    ordinal: int
    name: str
    scalar_kind: MssqlR1ProjectionScalarKindV1
    value_cardinality: MssqlR1ValueCardinalityV1
    nullable: bool

    def __post_init__(self) -> None:
        _VALIDATE.require_ordinal(self.ordinal, "projection field ordinal")
        require_schema_identifier(self.name, "projection field name")
        _VALIDATE.require_exact_enum(self.scalar_kind, MssqlR1ProjectionScalarKindV1, "projection scalar kind")
        _VALIDATE.require_exact_enum(self.value_cardinality, MssqlR1ValueCardinalityV1, "projection cardinality")
        _VALIDATE.require_bool(self.nullable, "projection field nullability")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _FIELD, (self.ordinal, self.name, self.scalar_kind, self.value_cardinality, self.nullable)
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProjectionFieldV1:
        values = list(decode_canonical_bytes(payload, _FIELD, field_count=5))
        values[2] = expect_enum(MssqlR1ProjectionScalarKindV1, values[2], "scalar kind")
        values[3] = expect_enum(MssqlR1ValueCardinalityV1, values[3], "cardinality")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ProjectionGrammarV1:
    grammar_version: str
    projection_role: MssqlR1ProjectionRoleV1
    ordered_fields: tuple[MssqlR1ProjectionFieldV1, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_text(self.grammar_version, "projection grammar version")
        _VALIDATE.require_exact_enum(self.projection_role, MssqlR1ProjectionRoleV1, "projection role")
        _VALIDATE.require_tuple(self.ordered_fields, MssqlR1ProjectionFieldV1, "projection fields", nonempty=True)
        _VALIDATE.require_contiguous(self.ordered_fields, "projection fields")
        if len({field.name.casefold() for field in self.ordered_fields}) != len(self.ordered_fields):
            raise MssqlR1V3ContractError("projection fields contain a case-fold collision")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _GRAMMAR,
            (self.grammar_version, self.projection_role, tuple(item.canonical_bytes for item in self.ordered_fields)),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProjectionGrammarV1:
        version, role, fields = decode_canonical_bytes(payload, _GRAMMAR, field_count=3)
        return cls(
            version,  # type: ignore[arg-type]
            expect_enum(MssqlR1ProjectionRoleV1, role, "projection role"),
            tuple(
                MssqlR1ProjectionFieldV1.from_canonical_bytes(expect_bytes(item, "field"))
                for item in expect_tuple(fields, "projection fields")
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ScalarParameterBindingV1:
    parameter_name: str
    value_source: MssqlR1ComparisonCoordinateV1

    def __post_init__(self) -> None:
        require_schema_identifier(self.parameter_name, "bound parameter")
        if type(self.value_source) is not MssqlR1ComparisonCoordinateV1:
            raise MssqlR1V3ContractError("parameter binding requires an exact coordinate")
        if self.value_source.source not in {
            MssqlR1ComparisonSourceV1.REQUEST_FIELD,
            MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.SESSION_BINDING,
        }:
            raise MssqlR1V3ContractError("scalar parameter binding source is unsupported")
        if self.value_source.source is MssqlR1ComparisonSourceV1.SESSION_BINDING and (
            self.value_source.resource is None
            or self.value_source.resource.resource_kind is not MssqlR1ResourceKindV1.SESSION
        ):
            raise MssqlR1V3ContractError("session binding requires an exact session resource")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_BINDING, (self.parameter_name, self.value_source.canonical_bytes))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ScalarParameterBindingV1:
        name, source = decode_canonical_bytes(payload, _BINDING, field_count=2)
        return cls(name, MssqlR1ComparisonCoordinateV1.from_canonical_bytes(expect_bytes(source, "source")))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RequestAuthorityV1:
    binding_kind: MssqlR1RequestBindingKindV1
    codec: MssqlR1SupportedCodecEntryV3
    request_payload_parameter: str | None
    request_digest_parameter: str
    projection_parameter: str | None
    projection_grammar: MssqlR1ProjectionGrammarV1 | None
    ordered_scalar_parameter_bindings: tuple[MssqlR1ScalarParameterBindingV1, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.binding_kind, MssqlR1RequestBindingKindV1, "request binding kind")
        if type(self.codec) is not MssqlR1SupportedCodecEntryV3:
            raise MssqlR1V3ContractError("request codec must be an exact schema-2 codec")
        require_schema_identifier(self.request_digest_parameter, "request digest parameter")
        _VALIDATE.require_tuple(
            self.ordered_scalar_parameter_bindings, MssqlR1ScalarParameterBindingV1, "scalar bindings"
        )
        if self.ordered_scalar_parameter_bindings:
            _VALIDATE.require_canonical(self.ordered_scalar_parameter_bindings, "scalar bindings")
        payload_mode = self.binding_kind is MssqlR1RequestBindingKindV1.PAYLOAD_PROJECTION
        present = (
            self.request_payload_parameter is not None,
            self.projection_parameter is not None,
            self.projection_grammar is not None,
        )
        if present != ((True, True, True) if payload_mode else (False, False, False)):
            raise MssqlR1V3ContractError("request authority has an inconsistent payload/projection shape")
        if self.request_payload_parameter is not None:
            require_schema_identifier(self.request_payload_parameter, "request payload parameter")
        if self.projection_parameter is not None:
            require_schema_identifier(self.projection_parameter, "projection parameter")
        if self.projection_grammar is not None and type(self.projection_grammar) is not MssqlR1ProjectionGrammarV1:
            raise MssqlR1V3ContractError("projection grammar type is invalid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _REQUEST,
            (
                self.binding_kind,
                self.codec.canonical_bytes,
                self.request_payload_parameter,
                self.request_digest_parameter,
                self.projection_parameter,
                None if self.projection_grammar is None else self.projection_grammar.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_scalar_parameter_bindings),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RequestAuthorityV1:
        values = list(decode_canonical_bytes(payload, _REQUEST, field_count=7))
        values[0] = expect_enum(MssqlR1RequestBindingKindV1, values[0], "binding kind")
        values[1] = MssqlR1SupportedCodecEntryV3.from_canonical_bytes(expect_bytes(values[1], "codec"))
        if values[5] is not None:
            values[5] = MssqlR1ProjectionGrammarV1.from_canonical_bytes(expect_bytes(values[5], "grammar"))
        values[6] = tuple(
            MssqlR1ScalarParameterBindingV1.from_canonical_bytes(expect_bytes(item, "binding"))
            for item in expect_tuple(values[6], "scalar bindings")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1OutcomeVariantV1:
    outcome_literal: str
    ordered_null_columns: tuple[str, ...]
    ordered_present_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        _VALIDATE.require_lower_ascii(self.outcome_literal, "outcome literal")
        for values in (self.ordered_null_columns, self.ordered_present_columns):
            if type(values) is not tuple:
                raise MssqlR1V3ContractError("outcome column shape must be a tuple")
            for value in values:
                require_schema_identifier(value, "outcome column")
        if set(self.ordered_null_columns) & set(self.ordered_present_columns):
            raise MssqlR1V3ContractError("outcome NULL/present columns overlap")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OUTCOME, (self.outcome_literal, self.ordered_null_columns, self.ordered_present_columns)
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1OutcomeVariantV1:
        return cls(*decode_canonical_bytes(payload, _OUTCOME, field_count=3))  # type: ignore[arg-type]


def validate_request_authority(
    authority: MssqlR1RequestAuthorityV1,
    parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
    codecs: tuple[MssqlR1SupportedCodecEntryV3, ...],
) -> None:
    """Bind request-envelope and scalar sources to the exact portable signature."""

    if authority.codec.canonical_bytes not in {item.canonical_bytes for item in codecs}:
        raise MssqlR1V3ContractError("request codec is outside schema-2 authority")
    parameter_names = tuple(item.name for item in parameters)
    _require_casefold_unique(parameter_names, "portable parameter coordinates")
    by_name = {item.name: item for item in parameters}
    role_names = tuple(
        name
        for name in (
            authority.request_payload_parameter,
            authority.request_digest_parameter,
            authority.projection_parameter,
        )
        if name is not None
    )
    if len({name.casefold() for name in role_names}) != len(role_names):
        raise MssqlR1V3ContractError("request roles require distinct parameter authorities")
    envelope = {
        authority.request_digest_parameter,
        authority.request_payload_parameter,
        authority.projection_parameter,
    } - {None}
    bound = {item.parameter_name for item in authority.ordered_scalar_parameter_bindings}
    if (
        len(by_name) != len(parameters)
        or envelope | bound != set(by_name)
        or envelope & bound
        or len(bound) != len(authority.ordered_scalar_parameter_bindings)
    ):
        raise MssqlR1V3ContractError("request authority does not bind every input parameter exactly once")
    required = [(authority.request_digest_parameter, MssqlR1ProjectionScalarKindV1.DIGEST)]
    if authority.request_payload_parameter is not None:
        required.append((authority.request_payload_parameter, MssqlR1ProjectionScalarKindV1.BINARY))
    if authority.projection_parameter is not None:
        required.append((authority.projection_parameter, MssqlR1ProjectionScalarKindV1.TEXT))
    for name, scalar_kind in required:
        parameter = by_name[name]
        if scalar_kind not in compatible_sql_scalar_kinds(parameter.sql_type, parameter.maximum_length):
            raise MssqlR1V3ContractError("request envelope parameter has an incompatible SQL scalar type")
    for binding in authority.ordered_scalar_parameter_bindings:
        parameter = by_name[binding.parameter_name]
        source = binding.value_source
        if (
            source.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
            or source.nullable
            or source.scalar_kind not in compatible_sql_scalar_kinds(parameter.sql_type, parameter.maximum_length)
        ):
            raise MssqlR1V3ContractError("scalar parameter source is not type-compatible")


def validate_outcome_variants(
    variants: tuple[MssqlR1OutcomeVariantV1, ...],
    result: MssqlR1ProcedureResultContractV3,
) -> None:
    """Close each fixed-result outcome over its exact nullable-column shape."""

    if type(result) is MssqlR1StageScanTemplateV3:
        _require_casefold_unique(
            tuple(item.name for item in result.ordered_fixed_suffix_columns), "stage-scan result columns"
        )
    if type(result) is not MssqlR1FixedResultV3:
        if variants:
            raise MssqlR1V3ContractError("stage-scan result cannot carry outcome variants")
        return
    assert isinstance(result, MssqlR1FixedResultV3)
    result_names = tuple(item.name for item in result.ordered_columns)
    _require_casefold_unique(result_names, "portable result column coordinates")
    if not variants or len({item.outcome_literal.casefold() for item in variants}) != len(variants):
        raise MssqlR1V3ContractError("fixed result requires nonempty unique outcome variants")
    nullable = tuple(item.name for item in result.ordered_columns if item.nullable)
    for outcome in variants:
        if (
            tuple(name for name in nullable if name in outcome.ordered_null_columns) != outcome.ordered_null_columns
            or tuple(name for name in nullable if name in outcome.ordered_present_columns)
            != outcome.ordered_present_columns
            or set(nullable) != set(outcome.ordered_null_columns) | set(outcome.ordered_present_columns)
        ):
            raise MssqlR1V3ContractError("outcome variant does not close nullable result columns")


def require_coordinate_authority(
    coordinate: MssqlR1ComparisonCoordinateV1,
    authority: MssqlR1RequestAuthorityV1,
    parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...],
    result_columns: tuple[MssqlR1ResultColumnV3, ...],
    declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1],
) -> None:
    """Resolve one repeated comparison coordinate to its sole typed authority."""

    if coordinate.resource is not None:
        declaration = declarations.get(coordinate.resource.canonical_bytes)
        field = (
            None
            if declaration is None
            else next((item for item in declaration.ordered_fields if item.name == coordinate.field_name), None)
        )
        if field is None or (
            coordinate.scalar_kind,
            coordinate.value_cardinality,
            coordinate.nullable,
        ) != (field.scalar_kind, field.value_cardinality, field.nullable):
            raise MssqlR1V3ContractError("comparison coordinate differs from declared field metadata")
        return
    if coordinate.source.value == "request_field":
        fields = () if authority.projection_grammar is None else authority.projection_grammar.ordered_fields
        matches = [item for item in fields if item.name == coordinate.field_name]
        if not matches or (
            coordinate.scalar_kind,
            coordinate.value_cardinality,
            coordinate.nullable,
        ) != (matches[0].scalar_kind, matches[0].value_cardinality, matches[0].nullable):
            raise MssqlR1V3ContractError("request-field coordinate differs from projection grammar")
        return
    if coordinate.source.value == "procedure_parameter":
        parameter_matches = [item for item in parameters if item.name == coordinate.field_name]
        if not parameter_matches:
            raise MssqlR1V3ContractError("comparison coordinate does not resolve to its portable authority")
        sql_type = parameter_matches[0].sql_type
        maximum_length = parameter_matches[0].maximum_length
        nullable = False
    else:
        result_matches = [item for item in result_columns if item.name == coordinate.field_name]
        if not result_matches:
            raise MssqlR1V3ContractError("comparison coordinate does not resolve to its portable authority")
        sql_type = result_matches[0].sql_type
        maximum_length = result_matches[0].maximum_length
        nullable = result_matches[0].nullable
    if (
        coordinate.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
        or coordinate.nullable != nullable
        or coordinate.scalar_kind not in compatible_sql_scalar_kinds(sql_type, maximum_length)
    ):
        raise MssqlR1V3ContractError("comparison coordinate differs from portable scalar metadata")
