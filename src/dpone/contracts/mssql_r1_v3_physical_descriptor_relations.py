"""Physical table wrappers around the sole schema-2 portable authority."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_definitions import MssqlR1DefinitionPayloadV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1DefinitionKindV1,
    MssqlR1MutationPolicyV1,
    MssqlR1TableLifecycleV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_primitives import MssqlR1ScalarValidatorsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1AccessKindV1,
    MssqlR1LockCardinalityV1,
    MssqlR1LockKindV1,
    MssqlR1PhysicalResourceDeclarationV1,
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ResourceKindV1,
    MssqlR1ValueCardinalityV1,
    compatible_sql_scalar_kinds,
)
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1PortableSchemaObjectV3
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1ConstraintKindV3, MssqlR1SchemaObjectKindV3

_DOMAIN = b"dpone-r1-physical-table-descriptor-v1\0"

TABLE_NAMES = tuple(
    "dpone_authority_contract_v3 dpone_target_registration_v3 dpone_active_registration_head_v3 "
    "dpone_target_generation_head_v3 dpone_writer_operation_v3 dpone_sealed_effect_v3 "
    "dpone_staging_artifact_v3 dpone_staging_chunk_v3 dpone_generation_authority_issuance_v3 "
    "dpone_generation_authority_ref_v3 dpone_generation_authority_consumption_v3 "
    "dpone_provider_install_receipt_v3 dpone_control_receipt_v3 "
    "dpone_open_stage_recovery_receipt_v3 dpone_open_stage_recovery_artifact_v3 dpone_effect_receipt_v3 "
    "dpone_batch_effect_receipt_v3 dpone_xmin_effect_receipt_v3 dpone_target_row_hash_v3 "
    "dpone_xmin_checkpoint_v3".split()
)

_PROVIDER_INSTALL_RECEIPT = "dpone_provider_install_receipt_v3"
_PROVIDER_INSTALL_RECEIPT_COLUMNS = (
    ("installation_effect_key", "binary", 32, 0, 0, False, None, False, False),
    ("request_digest", "binary", 32, 0, 0, False, None, False, False),
    ("payload_bytes", "varbinary", -1, 0, 0, False, None, False, False),
    ("payload_digest", "binary", 32, 0, 0, False, None, False, False),
    ("committed_at", "datetime2", 8, 27, 7, False, None, False, False),
)
_PROVIDER_INSTALL_RECEIPT_CONSTRAINTS = frozenset(
    {
        (MssqlR1ConstraintKindV3.PRIMARY_KEY, ("installation_effect_key",), None, True, True),
        (MssqlR1ConstraintKindV3.UNIQUE, ("payload_digest",), None, True, True),
        (MssqlR1ConstraintKindV3.CHECK, ("payload_bytes",), None, True, True),
        (MssqlR1ConstraintKindV3.CHECK, ("payload_bytes", "payload_digest"), None, True, True),
    }
)
_PROVIDER_INSTALL_RECEIPT_FIELDS = (
    (1, "installation_effect_key", MssqlR1ProjectionScalarKindV1.DIGEST, False, 1),
    (2, "request_digest", MssqlR1ProjectionScalarKindV1.DIGEST, False, None),
    (3, "payload_bytes", MssqlR1ProjectionScalarKindV1.BINARY, False, None),
    (4, "payload_digest", MssqlR1ProjectionScalarKindV1.DIGEST, False, None),
)
_PROVIDER_INSTALL_RECEIPT_ACCESS = (
    MssqlR1AccessKindV1.DDL,
    MssqlR1AccessKindV1.INSERT,
    MssqlR1AccessKindV1.READ,
)


_VALIDATE = MssqlR1ScalarValidatorsV1(MssqlR1V3ContractError)


@dataclass(frozen=True, slots=True)
class MssqlR1PhysicalTableDescriptorV1:
    portable_object: MssqlR1PortableSchemaObjectV3
    definition: MssqlR1DefinitionPayloadV1
    lifecycle: MssqlR1TableLifecycleV1
    mutation_policy: MssqlR1MutationPolicyV1
    lock_resource: MssqlR1PhysicalResourceRefV1

    def __post_init__(self) -> None:
        if type(self.portable_object) is not MssqlR1PortableSchemaObjectV3 or (
            self.portable_object.kind is not MssqlR1SchemaObjectKindV3.TABLE
        ):
            raise MssqlR1V3ContractError("physical table requires an exact portable table")
        column_names = tuple(item.name for item in self.portable_object.ordered_columns)
        if len({name.casefold() for name in column_names}) != len(column_names):
            raise MssqlR1V3ContractError("portable table columns contain a case-fold collision")
        if type(self.definition) is not MssqlR1DefinitionPayloadV1 or (
            self.definition.definition_kind is not MssqlR1DefinitionKindV1.TABLE_DDL
        ):
            raise MssqlR1V3ContractError("physical table requires table-DDL definition bytes")
        _VALIDATE.require_exact_enum(self.lifecycle, MssqlR1TableLifecycleV1, "table lifecycle")
        _VALIDATE.require_exact_enum(self.mutation_policy, MssqlR1MutationPolicyV1, "table mutation policy")
        if type(self.lock_resource) is not MssqlR1PhysicalResourceRefV1:
            raise MssqlR1V3ContractError("physical table lock resource is invalid")
        if self.portable_object.object_name == _PROVIDER_INSTALL_RECEIPT:
            self._validate_provider_install_receipt()

    def _validate_provider_install_receipt(self) -> None:
        columns = tuple(
            (
                item.name,
                item.sql_type,
                item.maximum_length,
                item.precision,
                item.scale,
                item.nullable,
                item.collation,
                item.identity,
                item.computed,
            )
            for item in self.portable_object.ordered_columns
        )
        constraints = {
            (item.kind, item.ordered_columns, item.referenced_object, item.trusted, item.enabled)
            for item in self.portable_object.ordered_constraints
        }
        exact_resource = MssqlR1PhysicalResourceRefV1(
            MssqlR1ResourceKindV1.STATIC_OBJECT,
            "dpone_authority",
            _PROVIDER_INSTALL_RECEIPT,
            None,
        )
        if (
            columns != _PROVIDER_INSTALL_RECEIPT_COLUMNS
            or len(self.portable_object.ordered_constraints) != len(_PROVIDER_INSTALL_RECEIPT_CONSTRAINTS)
            or constraints != _PROVIDER_INSTALL_RECEIPT_CONSTRAINTS
            or self.portable_object.ordered_indexes
            or self.portable_object.ordered_triggers
            or self.portable_object.ordered_extended_properties
            or self.lock_resource != exact_resource
        ):
            raise MssqlR1V3ContractError("provider-install receipt portable shape is invalid")
        if (
            self.lifecycle is not MssqlR1TableLifecycleV1.IMMUTABLE
            or self.mutation_policy is not MssqlR1MutationPolicyV1.INSTALLER_ONLY
        ):
            raise MssqlR1V3ContractError("provider-install receipt mutation authority is invalid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAIN,
            (
                self.portable_object.canonical_bytes,
                self.definition.canonical_bytes,
                self.lifecycle,
                self.mutation_policy,
                self.lock_resource.canonical_bytes,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PhysicalTableDescriptorV1:
        values = list(decode_canonical_bytes(payload, _DOMAIN, field_count=5))
        values[0] = MssqlR1PortableSchemaObjectV3.from_canonical_bytes(expect_bytes(values[0], "portable table"))
        values[1] = MssqlR1DefinitionPayloadV1.from_canonical_bytes(expect_bytes(values[1], "definition"))
        values[2] = expect_enum(MssqlR1TableLifecycleV1, values[2], "lifecycle")
        values[3] = expect_enum(MssqlR1MutationPolicyV1, values[3], "mutation policy")
        values[4] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[4], "lock resource"))
        return cls(*values)  # type: ignore[arg-type]


def validate_resource_declarations(
    declarations: tuple[MssqlR1PhysicalResourceDeclarationV1, ...],
    portable_objects: tuple[MssqlR1PortableSchemaObjectV3, ...],
) -> None:
    """Resolve static associations and freeze compatible table field metadata."""

    portable = {(item.schema_name, item.object_name): item for item in portable_objects}
    receipt_coordinate = ("dpone_authority", _PROVIDER_INSTALL_RECEIPT)
    if receipt_coordinate in portable:
        receipt_resource = MssqlR1PhysicalResourceRefV1(
            MssqlR1ResourceKindV1.STATIC_OBJECT,
            *receipt_coordinate,
            None,
        )
        exact_declarations = tuple(item for item in declarations if item.resource == receipt_resource)
        rebound_declarations = tuple(
            item
            for item in declarations
            if item.associated_static_object == receipt_resource and item.resource != receipt_resource
        )
        if len(exact_declarations) != 1 or rebound_declarations:
            raise MssqlR1V3ContractError("provider-install receipt resource binding is invalid")
    labels = tuple(
        (item.resource.schema_name or "") + "." + (item.resource.object_name or item.resource.coordinate_name or "")
        for item in declarations
    )
    if len({unicodedata.normalize("NFC", item).casefold().encode() for item in labels}) != len(labels):
        raise MssqlR1V3ContractError("resource declarations contain a case-fold collision")
    for declaration in declarations:
        for reference in (declaration.resource, declaration.associated_static_object):
            if reference is not None and reference.resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT:
                coordinate = (reference.schema_name, reference.object_name)
                if coordinate not in portable:
                    raise MssqlR1V3ContractError("static resource does not resolve to portable schema authority")
        reference = declaration.resource
        if reference.resource_kind is not MssqlR1ResourceKindV1.STATIC_OBJECT:
            continue
        coordinate = (reference.schema_name, reference.object_name)
        schema_object = portable[coordinate]  # type: ignore[index]
        if schema_object.kind is not MssqlR1SchemaObjectKindV3.TABLE:
            if declaration.ordered_fields:
                raise MssqlR1V3ContractError("procedure resource cannot invent portable fields")
            continue
        columns = {item.name: item for item in schema_object.ordered_columns}
        for field in declaration.ordered_fields:
            column = columns.get(field.name)
            if (
                column is None
                or field.scalar_kind not in compatible_sql_scalar_kinds(column.sql_type, column.maximum_length)
                or field.value_cardinality is not MssqlR1ValueCardinalityV1.SCALAR
                or field.nullable != column.nullable
            ):
                raise MssqlR1V3ContractError("static table resource field is not SQL-type compatible")
        if reference.object_name == _PROVIDER_INSTALL_RECEIPT:
            fields = tuple(
                (item.ordinal, item.name, item.scalar_kind, item.nullable, item.instance_key_ordinal)
                for item in declaration.ordered_fields
            )
            if fields != _PROVIDER_INSTALL_RECEIPT_FIELDS or (
                declaration.ordered_allowed_access_kinds != _PROVIDER_INSTALL_RECEIPT_ACCESS
                or declaration.associated_static_object != reference
                or declaration.allowed_lock_kind is not MssqlR1LockKindV1.PHYSICAL
                or declaration.lock_subrank != 0
                or declaration.lock_cardinality is not MssqlR1LockCardinalityV1.ONE
            ):
                raise MssqlR1V3ContractError("provider-install receipt resource authority is invalid")


__all__ = [
    "MssqlR1PhysicalTableDescriptorV1",
    "TABLE_NAMES",
    "validate_resource_declarations",
]


def validate_table_inventory(values: tuple[MssqlR1PhysicalTableDescriptorV1, ...]) -> None:
    """Reject inexact members or changes to the closed table inventory order."""
    _VALIDATE.require_tuple(values, MssqlR1PhysicalTableDescriptorV1, "table")
    if tuple(item.portable_object.object_name for item in values) != TABLE_NAMES:
        raise MssqlR1V3ContractError("physical table inventory identity/order is invalid")
