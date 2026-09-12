"""Portable procedure, trigger and object contracts for schema-2."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_schema_relations import (
    MssqlR1ExtendedPropertyV3,
    MssqlR1ResultCardinalityV3,
    MssqlR1ResultColumnV3,
    MssqlR1SchemaColumnV3,
    MssqlR1SchemaConstraintV3,
    MssqlR1SchemaIndexV3,
    MssqlR1SchemaObjectKindV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SignerProfileKindV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    decode_members,
    expect_bytes,
    expect_enum,
    require_canonical_named_set,
    require_contiguous,
    require_digest,
    require_module_options,
    require_schema_identifier,
)


@dataclass(frozen=True, slots=True)
class MssqlR1NoResultV3:
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-result-none-v3-schema-2\0", ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1NoResultV3:
        decode_canonical_bytes(payload, b"dpone-r1-schema-result-none-v3-schema-2\0", field_count=0)
        return cls()


@dataclass(frozen=True, slots=True)
class MssqlR1FixedResultV3:
    cardinality: MssqlR1ResultCardinalityV3
    ordered_columns: tuple[MssqlR1ResultColumnV3, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.cardinality, MssqlR1ResultCardinalityV3) or not isinstance(self.ordered_columns, tuple):
            raise MssqlR1V3ContractError("fixed result contract is invalid")
        if not self.ordered_columns or not all(
            isinstance(item, MssqlR1ResultColumnV3) for item in self.ordered_columns
        ):
            raise MssqlR1V3ContractError("fixed result contract is invalid")
        require_contiguous(self.ordered_columns, "result columns")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-result-fixed-v3-schema-2\0",
            (self.cardinality, tuple(item.canonical_bytes for item in self.ordered_columns)),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1FixedResultV3:
        cardinality, columns = decode_canonical_bytes(
            payload,
            b"dpone-r1-schema-result-fixed-v3-schema-2\0",
            field_count=2,
        )
        return cls(
            expect_enum(MssqlR1ResultCardinalityV3, cardinality, "result cardinality"),
            decode_members(columns, MssqlR1ResultColumnV3, "result column"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1StageScanTemplateV3:
    cardinality: MssqlR1ResultCardinalityV3
    business_projection_policy_digest: bytes
    ordered_fixed_suffix_columns: tuple[MssqlR1ResultColumnV3, ...]

    def __post_init__(self) -> None:
        if self.cardinality is not MssqlR1ResultCardinalityV3.ZERO_OR_MANY:
            raise MssqlR1V3ContractError("stage scan cardinality must be zero_or_many")
        require_digest(self.business_projection_policy_digest, "business projection policy digest")
        if not isinstance(self.ordered_fixed_suffix_columns, tuple) or not all(
            isinstance(item, MssqlR1ResultColumnV3) for item in self.ordered_fixed_suffix_columns
        ):
            raise MssqlR1V3ContractError("stage suffix columns must be a typed tuple")
        if not self.ordered_fixed_suffix_columns:
            raise MssqlR1V3ContractError("stage suffix columns must be nonempty")
        require_contiguous(self.ordered_fixed_suffix_columns, "stage suffix columns")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-result-template-v3-schema-2\0",
            (
                self.cardinality,
                self.business_projection_policy_digest,
                tuple(item.canonical_bytes for item in self.ordered_fixed_suffix_columns),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StageScanTemplateV3:
        cardinality, policy, columns = decode_canonical_bytes(
            payload,
            b"dpone-r1-schema-result-template-v3-schema-2\0",
            field_count=3,
        )
        return cls(
            expect_enum(MssqlR1ResultCardinalityV3, cardinality, "result cardinality"),
            policy,  # type: ignore[arg-type]
            decode_members(columns, MssqlR1ResultColumnV3, "stage suffix column"),
        )

    def instantiated_digest(
        self,
        business_columns: tuple[MssqlR1ResultColumnV3, ...],
        open_stage_plan_digest: bytes,
    ) -> bytes:
        if not isinstance(business_columns, tuple) or not all(
            isinstance(item, MssqlR1ResultColumnV3) for item in business_columns
        ):
            raise MssqlR1V3ContractError("business columns must be a typed tuple")
        require_contiguous(business_columns, "business columns")
        require_digest(open_stage_plan_digest, "open stage plan digest")
        suffix = tuple(
            MssqlR1ResultColumnV3(
                len(business_columns) + item.ordinal,
                item.name,
                item.sql_type,
                item.maximum_length,
                item.precision,
                item.scale,
                item.nullable,
                item.collation,
            )
            for item in self.ordered_fixed_suffix_columns
        )
        complete = business_columns + suffix
        require_contiguous(complete, "instantiated result columns")
        return hashlib.sha256(
            canonical_bytes(
                b"dpone-r1-stage-scan-instance-v3-schema-2\0",
                (
                    self.canonical_bytes,
                    open_stage_plan_digest,
                    tuple(item.canonical_bytes for item in complete),
                ),
            )
        ).digest()


MssqlR1ProcedureResultContractV3 = MssqlR1NoResultV3 | MssqlR1FixedResultV3 | MssqlR1StageScanTemplateV3


def _decode_result(value: object) -> MssqlR1ProcedureResultContractV3:
    payload = expect_bytes(value, "procedure result contract")
    variants = (
        (b"dpone-r1-schema-result-none-v3-schema-2\0", MssqlR1NoResultV3),
        (b"dpone-r1-schema-result-fixed-v3-schema-2\0", MssqlR1FixedResultV3),
        (b"dpone-r1-schema-result-template-v3-schema-2\0", MssqlR1StageScanTemplateV3),
    )
    for domain, contract in variants:
        if payload.startswith(domain):
            return contract.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("procedure result union is unsupported")


@dataclass(frozen=True, slots=True)
class MssqlR1ModuleOptionsV3:
    execute_as: str
    uses_ansi_nulls: bool
    uses_quoted_identifier: bool
    schema_bound: bool
    native_compilation: bool
    signer_profile: MssqlR1SignerProfileKindV3

    def __post_init__(self) -> None:
        require_module_options(
            self.execute_as,
            (self.uses_ansi_nulls, self.uses_quoted_identifier, self.schema_bound, self.native_compilation),
            self.signer_profile,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-module-options-v3-schema-2\0",
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ModuleOptionsV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-schema-module-options-v3-schema-2\0", field_count=6))
        values[5] = expect_enum(MssqlR1SignerProfileKindV3, values[5], "signer profile")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PortableTriggerV3:
    schema_name: str
    trigger_name: str
    parent_schema_name: str
    parent_object_name: str
    timing: str
    ordered_events: tuple[str, ...]
    enabled: bool
    definition_digest: bytes
    module_options: MssqlR1ModuleOptionsV3

    def __post_init__(self) -> None:
        for name in ("schema_name", "trigger_name", "parent_schema_name", "parent_object_name"):
            require_schema_identifier(getattr(self, name), name)
        if self.timing != "instead_of" or self.ordered_events != ("update", "delete") or self.enabled is not True:
            raise MssqlR1V3ContractError("trigger profile is outside the closed R1 profile")
        require_digest(self.definition_digest, "trigger definition digest")
        if not isinstance(self.module_options, MssqlR1ModuleOptionsV3) or (
            self.module_options.signer_profile is not MssqlR1SignerProfileKindV3.NONE
        ):
            raise MssqlR1V3ContractError("trigger module must be unsigned")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-trigger-v3-schema-2\0",
            (
                self.schema_name,
                self.trigger_name,
                self.parent_schema_name,
                self.parent_object_name,
                self.timing,
                self.ordered_events,
                self.enabled,
                self.definition_digest,
                self.module_options.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PortableTriggerV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-schema-trigger-v3-schema-2\0", field_count=9))
        values[8] = MssqlR1ModuleOptionsV3.from_canonical_bytes(expect_bytes(values[8], "module options"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PortableSchemaObjectV3:
    kind: MssqlR1SchemaObjectKindV3
    schema_name: str
    object_name: str
    module_definition_digest: bytes | None
    ordered_columns: tuple[MssqlR1SchemaColumnV3, ...]
    ordered_constraints: tuple[MssqlR1SchemaConstraintV3, ...]
    ordered_indexes: tuple[MssqlR1SchemaIndexV3, ...]
    ordered_parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...]
    result_contract: MssqlR1ProcedureResultContractV3 | None
    module_options: MssqlR1ModuleOptionsV3 | None
    ordered_triggers: tuple[MssqlR1PortableTriggerV3, ...]
    ordered_extended_properties: tuple[MssqlR1ExtendedPropertyV3, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.kind, MssqlR1SchemaObjectKindV3):
            raise MssqlR1V3ContractError("schema object kind is unsupported")
        require_schema_identifier(self.schema_name, "object schema")
        require_schema_identifier(self.object_name, "object name")
        for values, contract, field in (
            (self.ordered_columns, MssqlR1SchemaColumnV3, "columns"),
            (self.ordered_parameters, MssqlR1SchemaProcedureParameterV3, "parameters"),
        ):
            if not isinstance(values, tuple) or not all(isinstance(item, contract) for item in values):
                raise MssqlR1V3ContractError(f"{field} must be a typed tuple")
            require_contiguous(values, field)
        require_canonical_named_set(self.ordered_constraints, MssqlR1SchemaConstraintV3, "constraints", "name")
        require_canonical_named_set(self.ordered_indexes, MssqlR1SchemaIndexV3, "indexes", "name")
        require_canonical_named_set(self.ordered_triggers, MssqlR1PortableTriggerV3, "triggers", "trigger_name")
        require_canonical_named_set(
            self.ordered_extended_properties,
            MssqlR1ExtendedPropertyV3,
            "extended properties",
            "name",
        )
        if self.kind is MssqlR1SchemaObjectKindV3.TABLE:
            if (
                not self.ordered_columns
                or self.module_definition_digest is not None
                or self.ordered_parameters
                or self.result_contract is not None
                or self.module_options is not None
            ):
                raise MssqlR1V3ContractError("table schema object has procedure members")
            if any(
                trigger.schema_name != self.schema_name
                or trigger.parent_schema_name != self.schema_name
                or trigger.parent_object_name != self.object_name
                for trigger in self.ordered_triggers
            ):
                raise MssqlR1V3ContractError("trigger coordinates differ from their portable table")
        elif (
            self.module_definition_digest is None
            or self.ordered_columns
            or self.ordered_constraints
            or self.ordered_indexes
            or self.ordered_triggers
            or not isinstance(
                self.result_contract,
                (MssqlR1NoResultV3, MssqlR1FixedResultV3, MssqlR1StageScanTemplateV3),
            )
            or not isinstance(self.module_options, MssqlR1ModuleOptionsV3)
        ):
            raise MssqlR1V3ContractError("procedure schema object has inconsistent members")
        if self.module_definition_digest is not None:
            require_digest(self.module_definition_digest, "module definition digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-schema-object-v3-schema-2\0",
            (
                self.kind,
                self.schema_name,
                self.object_name,
                self.module_definition_digest,
                tuple(item.canonical_bytes for item in self.ordered_columns),
                tuple(item.canonical_bytes for item in self.ordered_constraints),
                tuple(item.canonical_bytes for item in self.ordered_indexes),
                tuple(item.canonical_bytes for item in self.ordered_parameters),
                None if self.result_contract is None else self.result_contract.canonical_bytes,
                None if self.module_options is None else self.module_options.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_triggers),
                tuple(item.canonical_bytes for item in self.ordered_extended_properties),
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PortableSchemaObjectV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-schema-object-v3-schema-2\0", field_count=12))
        values[0] = expect_enum(MssqlR1SchemaObjectKindV3, values[0], "object kind")
        contracts = (
            (4, MssqlR1SchemaColumnV3, "column"),
            (5, MssqlR1SchemaConstraintV3, "constraint"),
            (6, MssqlR1SchemaIndexV3, "index"),
            (7, MssqlR1SchemaProcedureParameterV3, "parameter"),
            (10, MssqlR1PortableTriggerV3, "trigger"),
            (11, MssqlR1ExtendedPropertyV3, "extended property"),
        )
        for index, contract, field in contracts:
            values[index] = decode_members(values[index], contract, field)
        if values[8] is not None:
            values[8] = _decode_result(values[8])
        if values[9] is not None:
            values[9] = MssqlR1ModuleOptionsV3.from_canonical_bytes(expect_bytes(values[9], "module options"))
        return cls(*values)  # type: ignore[arg-type]


def module_definition_digest(sql_text: str) -> bytes:
    """Hash canonical module text without retaining SQL in the contract."""
    if not isinstance(sql_text, str) or "\0" in sql_text:
        raise MssqlR1V3ContractError("module text must be NUL-free text")
    normalized = unicodedata.normalize("NFC", sql_text.replace("\r\n", "\n").replace("\r", "\n"))
    if normalized.endswith("\n\n"):
        raise MssqlR1V3ContractError("module text has more than one final line feed")
    if normalized.endswith("\n"):
        normalized = normalized[:-1]
    try:
        encoded = normalized.encode()
    except UnicodeEncodeError as exc:
        raise MssqlR1V3ContractError("module text must be valid UTF-8") from exc
    return hashlib.sha256(canonical_bytes(b"dpone-r1-schema-module-text-v3-schema-2\0", (encoded,))).digest()


__all__ = [name for name in tuple(globals()) if name.startswith("MssqlR1")] + ["module_definition_digest"]
