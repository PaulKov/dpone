"""Execution semantics and module wrappers for the physical descriptor."""

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
from dpone.contracts.mssql_r1_v3_physical_descriptor_definitions import MssqlR1DefinitionPayloadV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1BindingModuleKindV1,
    MssqlR1BindingSignerKindV1,
    MssqlR1DefinitionKindV1,
    MssqlR1PrincipalKindV1,
    MssqlR1TransitionAuthorityV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_execution import MssqlR1ExecutionSemanticsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_requests import exact_result_prefix
from dpone.contracts.mssql_r1_v3_schema_modules import (
    MssqlR1FixedResultV3,
    MssqlR1NoResultV3,
    MssqlR1PortableSchemaObjectV3,
    MssqlR1ProcedureResultContractV3,
    MssqlR1StageScanTemplateV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1ResultCardinalityV3,
    MssqlR1SchemaObjectKindV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SignerProfileKindV3,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionRuleV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1SubjectRoleV3,
)

_PROCEDURE = b"dpone-r1-physical-procedure-descriptor-v1\0"
_BINDING = b"dpone-r1-physical-binding-module-template-v1\0"

PROCEDURE_NAMES = tuple(
    "dpone_provision_registration_v3 dpone_rotate_registration_v3 dpone_import_generation_authority_set_v3 "
    "dpone_probe_control_effect_v3 dpone_attest_schema_v3 dpone_admit_operation_v3 dpone_seal_effect_v3 "
    "dpone_take_over_sealed_v3 dpone_probe_pre_source_v3 dpone_open_stage_v3 dpone_renew_stage_v3 "
    "dpone_begin_stage_chunk_v3 dpone_complete_stage_chunk_v3 dpone_observe_stage_v3 "
    "dpone_observe_stage_chunk_v3 dpone_scan_stage_v3 dpone_seal_stage_v3 dpone_recover_expired_open_v3 "
    "dpone_probe_open_recovery_v3 dpone_admit_writer_v3 dpone_resolve_admit_authority_set_v3 "
    "dpone_append_effect_receipt_v3 dpone_write_xmin_checkpoint_v3 dpone_consume_stage_set_v3 "
    "dpone_consume_authority_set_v3 dpone_advance_operation_and_head_v3 dpone_prove_candidate_effect_v3 "
    "dpone_probe_effect_v3 dpone_probe_registration_v3".split()
)


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalProcedureDescriptorV1:
    portable_object: MssqlR1PortableSchemaObjectV3
    definition: MssqlR1DefinitionPayloadV1
    execution_semantics: MssqlR1ExecutionSemanticsV1
    ordered_execute_principals: tuple[MssqlR1PrincipalKindV1, ...]

    def __post_init__(self) -> None:
        if (
            type(self.portable_object) is not MssqlR1PortableSchemaObjectV3
            or self.portable_object.kind is not MssqlR1SchemaObjectKindV3.PROCEDURE
        ):
            raise MssqlR1V3ContractError("physical procedure requires an exact portable procedure")
        if (
            type(self.definition) is not MssqlR1DefinitionPayloadV1
            or self.definition.definition_kind is not MssqlR1DefinitionKindV1.MODULE_TEXT
        ):
            raise MssqlR1V3ContractError("shared procedure requires module-text definition bytes")
        if self.definition.definition_digest != self.portable_object.module_definition_digest:
            raise MssqlR1V3ContractError("physical and portable procedure definition digests differ")
        if (
            type(self.execution_semantics) is not MssqlR1ExecutionSemanticsV1
            or self.execution_semantics.transition_authority is MssqlR1TransitionAuthorityV1.CALLER_UOW
        ):
            raise MssqlR1V3ContractError("shared procedure execution semantics are invalid")
        result = self.portable_object.result_contract
        is_scan = self.portable_object.object_name == "dpone_scan_stage_v3"
        if is_scan:
            if (
                type(result) is not MssqlR1StageScanTemplateV3
                or result.cardinality is not MssqlR1ResultCardinalityV3.ZERO_OR_MANY
            ):
                raise MssqlR1V3ContractError("stage scan requires the zero-or-many result template")
        elif (
            type(result) is not MssqlR1FixedResultV3
            or result.cardinality is not MssqlR1ResultCardinalityV3.EXACTLY_ONE
            or result.ordered_columns[:5] != exact_result_prefix()
        ):
            raise MssqlR1V3ContractError("shared procedure requires the exact fixed result prefix")
        _VALIDATE.require_tuple(self.ordered_execute_principals, MssqlR1PrincipalKindV1, "execute principals")
        if self.ordered_execute_principals != tuple(
            sorted(set(self.ordered_execute_principals), key=lambda item: item.value)
        ):
            raise MssqlR1V3ContractError("execute principals must use canonical enum order")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PROCEDURE,
            (
                self.portable_object.canonical_bytes,
                self.definition.canonical_bytes,
                self.execution_semantics.canonical_bytes,
                self.ordered_execute_principals,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalProcedureDescriptorV1:
        values = list(decode_canonical_bytes(payload, _PROCEDURE, field_count=4))
        values[0] = MssqlR1PortableSchemaObjectV3.from_canonical_bytes(expect_bytes(values[0], "portable procedure"))
        values[1] = MssqlR1DefinitionPayloadV1.from_canonical_bytes(expect_bytes(values[1], "definition"))
        values[2] = MssqlR1ExecutionSemanticsV1.from_canonical_bytes(expect_bytes(values[2], "execution semantics"))
        values[3] = tuple(
            expect_enum(MssqlR1PrincipalKindV1, item, "principal")
            for item in expect_tuple(values[3], "execute principals")
        )
        return cls(*values)  # type: ignore[arg-type]


def _decode_result(payload: bytes) -> MssqlR1ProcedureResultContractV3:
    if payload.startswith(b"dpone-r1-schema-result-fixed-v3-schema-2\0"):
        return MssqlR1FixedResultV3.from_canonical_bytes(payload)
    if payload.startswith(b"dpone-r1-schema-result-template-v3-schema-2\0"):
        return MssqlR1StageScanTemplateV3.from_canonical_bytes(payload)
    if payload.startswith(b"dpone-r1-schema-result-none-v3-schema-2\0"):
        return MssqlR1NoResultV3.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("binding result union is unsupported")


def validate_module_permissions(
    procedures: tuple[MssqlR1PhysicalProcedureDescriptorV1, ...],
    expected_rules: tuple[MssqlR1PermissionRuleV3, ...],
    expected_edges: set[tuple[str, str]],
    stage_signed: frozenset[str],
) -> None:
    """Prove the exact environment-principal edges and shared signer map."""

    derived = {
        (role.value, procedure.portable_object.object_name)
        for procedure in procedures
        for role in procedure.ordered_execute_principals
    }
    if derived != expected_edges:
        raise MssqlR1V3ContractError("derived execute principals differ from the exact 6/25/0/8 matrix")
    environment_roles = {item.value for item in MssqlR1PrincipalKindV1}
    observed: set[tuple[str, str]] = set()
    for rule in expected_rules:
        if rule.subject_role.value not in environment_roles or rule.permission != "EXECUTE":
            continue
        if (
            rule.grantor_role is not MssqlR1SubjectRoleV3.PROVISIONER
            or rule.source is not MssqlR1PermissionSourceV3.DIRECT
            or rule.scope is not MssqlR1PermissionScopeV3.OBJECT
            or rule.schema_name != "dpone_authority"
            or rule.object_name is None
            or rule.column_name is not None
            or rule.effect is not MssqlR1PermissionEffectV3.GRANT
            or rule.grant_option is not False
        ):
            raise MssqlR1V3ContractError("environment EXECUTE rule has a noncanonical shape")
        observed.add((rule.subject_role.value, rule.object_name))
    if observed != derived:
        raise MssqlR1V3ContractError("schema-2 permission rules differ from derived execute edges")
    for procedure in procedures:
        name = procedure.portable_object.object_name
        expected_signer = (
            MssqlR1SignerProfileKindV3.ATTESTOR
            if name == "dpone_attest_schema_v3"
            else MssqlR1SignerProfileKindV3.STAGE_OWNER
            if name in stage_signed
            else MssqlR1SignerProfileKindV3.NONE
        )
        if procedure.portable_object.module_options.signer_profile is not expected_signer:  # type: ignore[union-attr]
            raise MssqlR1V3ContractError("shared procedure signer mapping is invalid")


@dataclass(frozen=True, slots=True)
class MssqlR1BindingModuleTemplateV1:
    module_kind: MssqlR1BindingModuleKindV1
    name_template: str
    ordered_parameters: tuple[MssqlR1SchemaProcedureParameterV3, ...]
    result_contract: MssqlR1ProcedureResultContractV3
    definition_template: MssqlR1DefinitionPayloadV1
    execution_semantics: MssqlR1ExecutionSemanticsV1
    ordered_execute_principals: tuple[MssqlR1PrincipalKindV1, ...]
    signer_kind: MssqlR1BindingSignerKindV1

    def __post_init__(self) -> None:
        _VALIDATE.require_exact_enum(self.module_kind, MssqlR1BindingModuleKindV1, "binding module kind")
        if self.name_template != f"dpone_b_{{binding_uuid_hex}}_{self.module_kind.value}_v3":
            raise MssqlR1V3ContractError("binding module name template is not exact")
        _VALIDATE.require_tuple(self.ordered_parameters, MssqlR1SchemaProcedureParameterV3, "binding parameters")
        if tuple(item.ordinal for item in self.ordered_parameters) != tuple(range(1, len(self.ordered_parameters) + 1)):
            raise MssqlR1V3ContractError("binding parameters are not contiguous")
        if (
            type(self.result_contract) is not MssqlR1FixedResultV3
            or self.result_contract.cardinality is not MssqlR1ResultCardinalityV3.EXACTLY_ONE
            or self.result_contract.ordered_columns[:5] != exact_result_prefix()
        ):
            raise MssqlR1V3ContractError("binding module requires exactly-one fixed result")
        if (
            type(self.definition_template) is not MssqlR1DefinitionPayloadV1
            or self.definition_template.definition_kind is not MssqlR1DefinitionKindV1.MODULE_TEMPLATE
        ):
            raise MssqlR1V3ContractError("binding module requires template definition bytes")
        if (
            type(self.execution_semantics) is not MssqlR1ExecutionSemanticsV1
            or self.execution_semantics.transition_authority is not MssqlR1TransitionAuthorityV1.CALLER_UOW
        ):
            raise MssqlR1V3ContractError("binding module requires caller-UoW semantics")
        _VALIDATE.require_tuple(self.ordered_execute_principals, MssqlR1PrincipalKindV1, "binding execute principals")
        if self.ordered_execute_principals != (MssqlR1PrincipalKindV1.RUNTIME,):
            raise MssqlR1V3ContractError("binding module execute principal must be runtime only")
        _VALIDATE.require_exact_enum(self.signer_kind, MssqlR1BindingSignerKindV1, "binding signer")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _BINDING,
            (
                self.module_kind,
                self.name_template,
                tuple(item.canonical_bytes for item in self.ordered_parameters),
                self.result_contract.canonical_bytes,
                self.definition_template.canonical_bytes,
                self.execution_semantics.canonical_bytes,
                self.ordered_execute_principals,
                self.signer_kind,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingModuleTemplateV1:
        values = list(decode_canonical_bytes(payload, _BINDING, field_count=8))
        values[0] = expect_enum(MssqlR1BindingModuleKindV1, values[0], "module kind")
        values[2] = tuple(
            MssqlR1SchemaProcedureParameterV3.from_canonical_bytes(expect_bytes(item, "parameter"))
            for item in expect_tuple(values[2], "binding parameters")
        )
        values[3] = _decode_result(expect_bytes(values[3], "result"))
        values[4] = MssqlR1DefinitionPayloadV1.from_canonical_bytes(expect_bytes(values[4], "definition"))
        values[5] = MssqlR1ExecutionSemanticsV1.from_canonical_bytes(expect_bytes(values[5], "execution semantics"))
        values[6] = tuple(
            expect_enum(MssqlR1PrincipalKindV1, item, "principal")
            for item in expect_tuple(values[6], "binding execute principals")
        )
        values[7] = expect_enum(MssqlR1BindingSignerKindV1, values[7], "signer")
        return cls(*values)  # type: ignore[arg-type]


def validate_procedure_inventory(values: tuple[MssqlR1PhysicalProcedureDescriptorV1, ...]) -> None:
    """Reject inexact members or changes to the closed procedure inventory order."""
    _VALIDATE.require_tuple(values, MssqlR1PhysicalProcedureDescriptorV1, "procedure")
    if tuple(item.portable_object.object_name for item in values) != PROCEDURE_NAMES:
        raise MssqlR1V3ContractError("physical procedure inventory identity/order is invalid")


def validate_binding_inventory(values: tuple[MssqlR1BindingModuleTemplateV1, ...]) -> None:
    """Reject inexact members or changes to the closed binding inventory order."""
    _VALIDATE.require_tuple(values, MssqlR1BindingModuleTemplateV1, "binding")
    if tuple(item.module_kind for item in values) != tuple(MssqlR1BindingModuleKindV1):
        raise MssqlR1V3ContractError("physical binding inventory identity/order is invalid")
