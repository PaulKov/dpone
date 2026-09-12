"""Closed aggregate for the SQL-free MSSQL R1 physical schema descriptor."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_text,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1, MssqlR1PrincipalKindV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import (
    MssqlR1MigrationProbeV1,
    validate_migration_probe_inventory,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_procedures import (
    PROCEDURE_NAMES as PROCEDURE_NAMES,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_procedures import (
    MssqlR1BindingModuleTemplateV1,
    MssqlR1PhysicalProcedureDescriptorV1,
    validate_binding_inventory,
    validate_module_permissions,
    validate_procedure_inventory,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_profiles import (
    MssqlR1PhysicalEngineProfileV1,
    MssqlR1PhysicalSessionProfileV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_relations import (
    TABLE_NAMES as TABLE_NAMES,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_relations import (
    MssqlR1PhysicalTableDescriptorV1,
    validate_resource_declarations,
    validate_table_inventory,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceDeclarationV1,
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
    validate_resource_inventory,
)
from dpone.contracts.mssql_r1_v3_schema_attestation import MssqlR1SchemaContractV3

VERSION = "dpone-mssql-r1-v3-physical-schema-2-r2"
DOMAIN = b"dpone-r1-physical-schema-descriptor-v1\0"


PRINCIPAL_SHORT = {
    MssqlR1PrincipalKindV1.PROVISIONER: frozenset(
        "provision_registration rotate_registration import_generation_authority_set probe_control_effect "
        "attest_schema probe_registration".split()
    ),
    MssqlR1PrincipalKindV1.RUNTIME: frozenset(
        "probe_control_effect admit_operation seal_effect take_over_sealed probe_pre_source open_stage renew_stage "
        "begin_stage_chunk complete_stage_chunk observe_stage observe_stage_chunk scan_stage seal_stage "
        "recover_expired_open probe_open_recovery admit_writer resolve_admit_authority_set append_effect_receipt "
        "write_xmin_checkpoint consume_stage_set consume_authority_set advance_operation_and_head "
        "prove_candidate_effect probe_effect probe_registration".split()
    ),
    MssqlR1PrincipalKindV1.OBSERVER: frozenset(
        "probe_control_effect attest_schema probe_pre_source observe_stage observe_stage_chunk probe_open_recovery "
        "probe_effect probe_registration".split()
    ),
    MssqlR1PrincipalKindV1.LOADER: frozenset(),
}
STAGE_SIGNED = frozenset(
    "dpone_open_stage_v3 dpone_begin_stage_chunk_v3 dpone_complete_stage_chunk_v3 dpone_observe_stage_v3 "
    "dpone_scan_stage_v3 dpone_seal_stage_v3 dpone_recover_expired_open_v3 dpone_consume_stage_set_v3".split()
)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalSchemaDescriptorV1:
    descriptor_version: str
    engine_profile: MssqlR1PhysicalEngineProfileV1
    session_profile: MssqlR1PhysicalSessionProfileV1
    expected_schema_contract: MssqlR1SchemaContractV3
    ordered_resource_declarations: tuple[MssqlR1PhysicalResourceDeclarationV1, ...]
    ordered_tables: tuple[MssqlR1PhysicalTableDescriptorV1, ...]
    ordered_procedures: tuple[MssqlR1PhysicalProcedureDescriptorV1, ...]
    ordered_binding_module_templates: tuple[MssqlR1BindingModuleTemplateV1, ...]
    ordered_migration_probes: tuple[MssqlR1MigrationProbeV1, ...]

    def __post_init__(self) -> None:
        if type(self.descriptor_version) is not str or self.descriptor_version != VERSION:
            raise MssqlR1V3ContractError("physical descriptor version is unsupported")
        for value, contract, field in (
            (self.engine_profile, MssqlR1PhysicalEngineProfileV1, "engine profile"),
            (self.session_profile, MssqlR1PhysicalSessionProfileV1, "session profile"),
            (self.expected_schema_contract, MssqlR1SchemaContractV3, "schema contract"),
        ):
            if type(value) is not contract:
                raise MssqlR1V3ContractError(f"{field} has an inexact type")
        self._validate_inventory()
        declarations = self._declarations()
        validate_resource_declarations(
            self.ordered_resource_declarations, self.expected_schema_contract.ordered_objects
        )
        self._validate_wrappers(declarations)
        validate_module_permissions(
            self.ordered_procedures,
            self.expected_schema_contract.ordered_permission_rules,
            {
                (role.value, f"dpone_{short}_v3")
                for role, short_names in PRINCIPAL_SHORT.items()
                for short in short_names
            },
            frozenset(STAGE_SIGNED),
        )
        errors: set[int] = set()
        for procedure in self.ordered_procedures:
            result = procedure.portable_object.result_contract
            if result is None:
                raise MssqlR1V3ContractError("physical procedure requires a result contract")
            procedure.execution_semantics.validate_contract(
                procedure.portable_object.ordered_parameters,
                result,
                declarations,
                self.expected_schema_contract.ordered_supported_codecs,
            )
            errors.update(item.error_number for item in procedure.execution_semantics.ordered_error_conditions)
        for module in self.ordered_binding_module_templates:
            module.execution_semantics.validate_contract(
                module.ordered_parameters,
                module.result_contract,
                declarations,
                self.expected_schema_contract.ordered_supported_codecs,
            )
            errors.update(item.error_number for item in module.execution_semantics.ordered_error_conditions)
            quality = module.module_kind in {
                MssqlR1BindingModuleKindV1.BATCH_QUALITY,
                MssqlR1BindingModuleKindV1.XMIN_QUALITY,
            }
            if quality != (not module.execution_semantics.ordered_write_set):
                raise MssqlR1V3ContractError("binding quality/mutation write-set shape is invalid")
        if errors != set(range(51001, 51013)):
            raise MssqlR1V3ContractError("physical error-number union is incomplete")

    def _validate_inventory(self) -> None:
        validate_table_inventory(self.ordered_tables)
        validate_procedure_inventory(self.ordered_procedures)
        validate_binding_inventory(self.ordered_binding_module_templates)
        if any(item.portable_object.schema_name != "dpone_authority" for item in self.ordered_tables) or any(
            item.portable_object.schema_name != "dpone_authority" for item in self.ordered_procedures
        ):
            raise MssqlR1V3ContractError("physical inventory requires exact schema and name identities")
        validate_resource_inventory(self.ordered_resource_declarations)
        validate_migration_probe_inventory(self.ordered_migration_probes)
        if (self.expected_schema_contract.authority_schema_name, self.expected_schema_contract.stage_schema_name) != (
            "dpone_authority",
            "dpone_stage",
        ):
            raise MssqlR1V3ContractError("physical descriptor requires the exact two managed schemas")
        projected = tuple(
            sorted(
                (
                    *(item.portable_object for item in self.ordered_tables),
                    *(item.portable_object for item in self.ordered_procedures),
                ),
                key=lambda item: (item.schema_name.encode(), item.object_name.encode()),
            )
        )
        if projected != self.expected_schema_contract.ordered_objects:
            raise MssqlR1V3ContractError("physical wrappers differ from the schema-2 portable projection")

    def _declarations(self) -> dict[bytes, MssqlR1PhysicalResourceDeclarationV1]:
        return {item.resource.canonical_bytes: item for item in self.ordered_resource_declarations}

    def _validate_wrappers(self, declarations: dict[bytes, MssqlR1PhysicalResourceDeclarationV1]) -> None:
        portable = {(item.schema_name, item.object_name) for item in self.expected_schema_contract.ordered_objects}
        for declaration in self.ordered_resource_declarations:
            for ref in (declaration.resource, declaration.associated_static_object):
                if (
                    ref is not None
                    and ref.resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT
                    and (ref.schema_name, ref.object_name) not in portable
                ):
                    raise MssqlR1V3ContractError("static resource does not resolve to portable schema authority")
        for table in self.ordered_tables:
            table_declaration = declarations.get(table.lock_resource.canonical_bytes)
            exact = MssqlR1PhysicalResourceRefV1(
                MssqlR1ResourceKindV1.STATIC_OBJECT,
                table.portable_object.schema_name,
                table.portable_object.object_name,
                None,
            )
            if (
                table_declaration is None
                or table_declaration.associated_static_object != exact
                or table_declaration.allowed_lock_kind is None
            ):
                raise MssqlR1V3ContractError("table lock resource is unbound from its portable object")
        for probe in self.ordered_migration_probes:
            if probe.observation_resource.canonical_bytes not in declarations:
                raise MssqlR1V3ContractError("migration probe references an undeclared resource")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            DOMAIN,
            (
                self.descriptor_version,
                self.engine_profile.canonical_bytes,
                self.session_profile.canonical_bytes,
                self.expected_schema_contract.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_resource_declarations),
                tuple(item.canonical_bytes for item in self.ordered_tables),
                tuple(item.canonical_bytes for item in self.ordered_procedures),
                tuple(item.canonical_bytes for item in self.ordered_binding_module_templates),
                tuple(item.canonical_bytes for item in self.ordered_migration_probes),
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalSchemaDescriptorV1:
        values = list(decode_canonical_bytes(payload, DOMAIN, field_count=9))
        if expect_text(values[0], "descriptor version") != VERSION:
            raise MssqlR1V3ContractError("physical descriptor version is unsupported")
        values[1] = MssqlR1PhysicalEngineProfileV1.from_canonical_bytes(expect_bytes(values[1], "engine"))
        values[2] = MssqlR1PhysicalSessionProfileV1.from_canonical_bytes(expect_bytes(values[2], "session"))
        values[3] = MssqlR1SchemaContractV3.from_canonical_bytes(expect_bytes(values[3], "schema contract"))
        for index, contract in (
            (4, MssqlR1PhysicalResourceDeclarationV1),
            (5, MssqlR1PhysicalTableDescriptorV1),
            (6, MssqlR1PhysicalProcedureDescriptorV1),
            (7, MssqlR1BindingModuleTemplateV1),
            (8, MssqlR1MigrationProbeV1),
        ):
            values[index] = tuple(
                contract.from_canonical_bytes(expect_bytes(item, "descriptor member"))
                for item in expect_tuple(values[index], "descriptor members")
            )
        result = cls(*values)  # type: ignore[arg-type]
        if result.canonical_bytes != payload:
            raise MssqlR1V3ContractError("physical descriptor is not byte-identical after decode")
        return result
