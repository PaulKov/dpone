"""Black-box factory contracts for the restricted MSSQL Binding V2 compiler."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
from dataclasses import dataclass, replace
from functools import lru_cache
from types import GenericAlias
from typing import Any, Literal, get_type_hints
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_codec import canonical_bytes
from dpone.contracts.mssql_r1_v3_physical_descriptor_definitions import MssqlR1DefinitionPayloadV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1DefinitionKindV1
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MssqlR1EnvironmentPrincipalProfileV1,
    MssqlR1PrivateKeyDispositionV1,
    MssqlR1ReinstallPolicyV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_permissions import MssqlR1PermissionClosurePolicyV1
from dpone.contracts.mssql_r1_v3_provider_security_principals import MssqlR1EnvironmentPrincipalRefV1
from dpone.contracts.mssql_r1_v3_provider_security_profile import MssqlR1SharedInstallSecurityProfileV2
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import (
    MssqlR1RegisteredTargetColumnV1,
    MssqlR1RegisteredTargetIndexV1,
)
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1SubjectRoleV3
from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1
from tests.postgres_mssql_r1_v3_type_target_test_support import authorities
from tests.test_postgres_mssql_r1_source_schema_runtime import FakeCatalogConnector, _policy, issue_authority
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules
from tests.test_postgres_mssql_r1_v3_binding_semantic_inventory import (
    PHASE_REASONS,
    InputObserver,
    bind_phase_pair_scenario,
)
from tests.test_postgres_mssql_r1_v3_provider_security_contract import (
    _binding_policy,
    _memberships,
    _shared_signers,
)
from tests.test_postgres_mssql_r1_v3_registered_target_catalog_contract import registered_target_catalog

FACTORY_LIMIT = 25_165_824
PACK_LIMIT = 67_108_864
ELEMENT_LIMIT = 4_194_304

RECOVERY_AND_MESSAGE = {
    "wrong_domain": ("permanent_input_error", "Binding input uses an unsupported canonical domain."),
    "wrong_version": ("permanent_input_error", "Binding input uses an unsupported contract version."),
    "malformed_canonical_bytes": ("permanent_input_error", "Binding input is not canonical."),
    "canonical_size_exceeded": ("permanent_input_error", "Binding input exceeds the bounded canonical profile."),
    "exact_type_violation": ("permanent_input_error", "Binding input uses an inexact model type."),
    "ordinal_invalid": ("permanent_input_error", "Binding ordinals are incomplete or reordered."),
    "column_count_invalid": ("permanent_input_error", "Binding column count is outside the supported range."),
    "identifier_invalid": ("permanent_input_error", "Binding identifier policy is not satisfied."),
    "dependency_mismatch": ("operator_intervention", "Approved dependency revisions do not form one closure."),
    "authority_splice": (
        "operator_intervention",
        "Independently valid route, source or target authorities do not share one identity.",
    ),
    "mapping_coverage_invalid": (
        "permanent_input_error",
        "Source-to-target mapping is not the exact R1 identity mapping.",
    ),
    "target_key_invalid": ("permanent_input_error", "Target key is outside the R1 profile."),
    "stage_shape_invalid": ("operator_intervention", "Stage projection differs from descriptor/type authority."),
    "buffer_plan_invalid": ("operator_intervention", "Stage scan envelope differs from the accepted ABI."),
    "module_set_invalid": ("operator_intervention", "Binding module set is incomplete or reordered."),
    "template_mismatch": ("operator_intervention", "Descriptor template authority does not match the binding."),
    "signer_policy_mismatch": (
        "operator_intervention",
        "Binding signer lifecycle authority does not match Security V2.",
    ),
    "permission_widening": (
        "operator_intervention",
        "Binding permissions exceed the exact derived closure.",
    ),
    "signature_mismatch": (
        "operator_intervention",
        "Signature/template intent differs from the module set.",
    ),
    "unsupported_target_profile": ("permanent_input_error", "Target profile is not supported by R1."),
    "internal_invariant_violation": ("operator_intervention", "Binding compiler invariant was not satisfied."),
}


@dataclass(frozen=True)
class FactoryInputs:
    physical_descriptor: Any
    shared_security_profile: Any
    binding_signer_lifecycle_policy: Any
    source_schema_authority: Any
    stable_target_authority: Any
    target_catalog: Any

    def kwargs(self) -> dict[str, Any]:
        return dict(vars(self))


def _independent_complete_pack_shape(inputs: FactoryInputs) -> bytes:
    """Encode every Binding model field without importing future modules.

    This is an independent maximum-shape size golden, not a decoder oracle:
    semantic equality remains asserted against the public factory elsewhere.
    """

    def digest(value: bytes) -> bytes:
        return hashlib.sha256(value).digest()

    catalog = inputs.target_catalog
    source = inputs.source_schema_authority
    catalog_bytes = catalog.canonical_bytes
    catalog_digest = digest(catalog_bytes)
    source_bytes = source.canonical_bytes
    source_digest = digest(source_bytes)
    physical_bytes = inputs.physical_descriptor.canonical_bytes
    security_bytes = inputs.shared_security_profile.canonical_bytes
    stable_bytes = inputs.stable_target_authority.canonical_bytes
    resource_ref = canonical_bytes(
        b"dpone-r1-physical-resource-ref-v1\0",
        ("registered_target", None, None, "registered_target"),
    )
    target_ref = canonical_bytes(
        b"dpone-mssql-r1-registered-target-ref-v2\0",
        (
            "dpone-mssql-r1-registered-target-ref-2",
            catalog.target_binding_uuid,
            catalog.target_object_uuid,
            resource_ref,
            digest(stable_bytes),
            catalog_digest,
        ),
    )
    column_mappings = tuple(
        canonical_bytes(
            b"dpone-mssql-r1-business-column-mapping-v2\0",
            (
                "dpone-mssql-r1-business-column-mapping-2",
                ordinal,
                source.ordered_columns[ordinal - 1].source_column_ref.canonical_bytes,
                canonical_bytes(
                    b"dpone-mssql-r1-registered-target-column-ref-v1\0",
                    (
                        column.ordinal,
                        column.name,
                        column.nullable,
                        column.scalar_shape.canonical_bytes,
                        catalog_digest,
                        1 if ordinal == catalog.primary_key.ordered_key_column_ordinals[0] else None,
                    ),
                ),
                source.type_policy_authority.ordered_decisions[0].decision_id,
                1 if ordinal == catalog.primary_key.ordered_key_column_ordinals[0] else None,
            ),
        )
        for ordinal, column in enumerate(catalog.ordered_columns, 1)
    )
    mapping = canonical_bytes(
        b"dpone-mssql-r1-binding-target-mapping-v2\0",
        (
            "dpone-mssql-r1-binding-target-mapping-2",
            target_ref,
            source_digest,
            source.selected_source_authority_sha256,
            source.type_policy_authority.digest,
            column_mappings,
        ),
    )
    scan = next(
        item
        for item in inputs.physical_descriptor.ordered_procedures
        if item.portable_object.object_name == "dpone_scan_stage_v3"
    )
    scan_template = scan.portable_object.result_contract
    scan_resource_ref = canonical_bytes(
        b"dpone-r1-physical-resource-ref-v1\0",
        ("static_object", scan.portable_object.schema_name, scan.portable_object.object_name, None),
    )
    business_results = tuple(
        canonical_bytes(
            b"dpone-r1-schema-result-column-v3-schema-2\0",
            (
                column.ordinal,
                column.name,
                column.system_type_name,
                column.max_length,
                column.precision,
                column.scale,
                column.nullable,
                column.scalar_shape.collation,
            ),
        )
        for column in catalog.ordered_columns
    )
    key_column = catalog.ordered_columns[catalog.primary_key.ordered_key_column_ordinals[0] - 1]
    complete_key_result = canonical_bytes(
        b"dpone-r1-schema-result-column-v3-schema-2\0",
        (
            1,
            key_column.name,
            key_column.system_type_name,
            key_column.max_length,
            key_column.precision,
            key_column.scale,
            key_column.nullable,
            key_column.scalar_shape.collation,
        ),
    )
    projections = tuple(
        canonical_bytes(
            b"dpone-mssql-r1-stage-scan-projection-v2\0",
            (
                "dpone-mssql-r1-stage-scan-projection-2",
                kind,
                scan_resource_ref,
                digest(scan_template.canonical_bytes),
                business_results if kind != "xmin_complete_keys" else (complete_key_result,),
                (business_results if kind != "xmin_complete_keys" else (complete_key_result,))
                + tuple(
                    canonical_bytes(
                        b"dpone-r1-schema-result-column-v3-schema-2\0",
                        (
                            (len(business_results) if kind != "xmin_complete_keys" else 1) + suffix.ordinal,
                            suffix.name,
                            suffix.sql_type,
                            suffix.maximum_length,
                            suffix.precision,
                            suffix.scale,
                            suffix.nullable,
                            suffix.collation,
                        ),
                    )
                    for suffix in scan_template.ordered_fixed_suffix_columns
                ),
            ),
        )
        for kind in ("batch_payload", "xmin_delta", "xmin_complete_keys")
    )
    projection_by_kind = dict(zip(("batch_payload", "xmin_delta", "xmin_complete_keys"), projections, strict=True))
    row_hash_ref = canonical_bytes(
        b"dpone-r1-physical-resource-ref-v1\0",
        ("static_object", "dpone_authority", "dpone_target_row_hash_v3", None),
    )
    target_access = {
        access: canonical_bytes(b"dpone-r1-physical-resource-access-v1\0", (resource_ref, access))
        for access in ("read", "insert", "update", "delete")
    }
    row_hash_access = {
        access: canonical_bytes(b"dpone-r1-physical-resource-access-v1\0", (row_hash_ref, access))
        for access in ("read", "insert", "update", "delete")
    }
    module_kinds = tuple(item.module_kind.value for item in inputs.physical_descriptor.ordered_binding_module_templates)
    modules = []
    for template, module_kind in zip(
        inputs.physical_descriptor.ordered_binding_module_templates, module_kinds, strict=True
    ):
        artifact_kinds = (
            ("batch_payload",) if module_kind.startswith("batch_") else ("xmin_delta", "xmin_complete_keys")
        )
        invocations = tuple(
            canonical_bytes(
                b"dpone-mssql-r1-stage-scan-invocation-v2\0",
                (
                    "dpone-mssql-r1-stage-scan-invocation-2",
                    ordinal,
                    kind,
                    kind,
                    digest(projection_by_kind[kind]),
                    digest(scan.canonical_bytes),
                    (
                        "open_stage_plan_canonical_bytes",
                        "sha256_request_payload",
                        "projection_json_from_open_stage_plan",
                    ),
                ),
            )
            for ordinal, kind in enumerate(artifact_kinds, 1)
        )
        buffer_plan = canonical_bytes(
            b"dpone-mssql-r1-module-stage-buffer-plan-v2\0",
            ("dpone-mssql-r1-module-stage-buffer-plan-2", module_kind, invocations),
        )
        accesses = (
            tuple(target_access.values())
            if module_kind in {"batch_mutate", "xmin_mutate"}
            else tuple(row_hash_access.values())
            if module_kind in {"batch_row_hash", "xmin_row_hash"}
            else (target_access["read"], row_hash_access["read"])
        )
        modules.append(
            canonical_bytes(
                b"dpone-mssql-r1-instantiated-binding-module-v2\0",
                (
                    "dpone-mssql-r1-instantiated-binding-module-2",
                    module_kind,
                    "dpone_authority",
                    template.name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
                    digest(template.canonical_bytes),
                    digest(mapping),
                    buffer_plan,
                    accesses,
                ),
            )
        )
    lifecycle = inputs.binding_signer_lifecycle_policy
    signer = canonical_bytes(
        b"dpone-mssql-r1-binding-signer-identity-v2\0",
        (
            "dpone-mssql-r1-binding-signer-identity-2",
            catalog.target_binding_uuid,
            lifecycle.canonical_bytes,
            lifecycle.certificate_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            lifecycle.certificate_user_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            lifecycle.certificate_subject_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            lifecycle.certificate_owner.canonical_bytes,
            lifecycle.start_date_yyyymmdd,
            lifecycle.expiry_date_yyyymmdd,
            lifecycle.certificate_creation_profile,
            lifecycle.signature_algorithm,
            lifecycle.secret_policy_digest,
        ),
    )
    signature_intents = tuple(
        canonical_bytes(
            b"dpone-mssql-r1-binding-signature-intent-v2\0",
            ("dpone-mssql-r1-binding-signature-intent-2", ordinal, kind, digest(module), digest(signer)),
        )
        for ordinal, (kind, module) in enumerate(zip(module_kinds, modules, strict=True), 1)
    )
    binding_beneficiary = canonical_bytes(
        b"dpone-r1-security-binding-signer-instance-ref-v1\0", (catalog.target_binding_uuid,)
    )
    runtime_beneficiary = canonical_bytes(b"dpone-r1-security-environment-principal-ref-v1\0", ("runtime",))
    grantor = canonical_bytes(b"dpone-r1-security-environment-principal-ref-v1\0", ("provisioner",))
    direct_origin = canonical_bytes(b"dpone-r1-security-direct-permission-origin-v1\0", ())

    def permission_path(beneficiary: bytes, schema: str, object_name: str, permission_name: str) -> bytes:
        target = canonical_bytes(
            b"dpone-r1-security-permission-target-v1\0",
            ("object", schema, object_name, None, True),
        )
        return canonical_bytes(
            b"dpone-r1-security-resolved-permission-path-v2\0",
            (beneficiary, grantor, direct_origin, target, permission_name, "grant", False),
        )

    permission_for_access = {"read": "SELECT", "insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
    dml_projection_pairs = tuple(
        (
            access,
            permission_path(binding_beneficiary, schema, object_name, permission_for_access[access_kind]),
        )
        for resource_access, schema, object_name in (
            (target_access, catalog.schema_name, catalog.object_name),
            (row_hash_access, "dpone_authority", "dpone_target_row_hash_v3"),
        )
        for access_kind, access in resource_access.items()
    )
    dml_paths = tuple(path for _, path in dml_projection_pairs)
    runtime_paths = tuple(
        permission_path(
            runtime_beneficiary,
            "dpone_authority",
            template.name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            "EXECUTE",
        )
        for template in inputs.physical_descriptor.ordered_binding_module_templates
    )
    access_projections = tuple(
        canonical_bytes(
            b"dpone-mssql-r1-binding-access-permission-projection-v2\0",
            ("dpone-mssql-r1-binding-access-permission-projection-2", access, path),
        )
        for access, path in sorted(dml_projection_pairs)
    )
    permission = canonical_bytes(
        b"dpone-mssql-r1-binding-permission-v2\0",
        (
            "dpone-mssql-r1-binding-permission-2",
            "exact_target_and_row_hash_v1",
            access_projections,
            runtime_paths,
            signature_intents,
        ),
    )
    inventory = canonical_bytes(
        b"dpone-mssql-r1-expected-binding-inventory-v2\0",
        (
            "dpone-mssql-r1-expected-binding-inventory-2",
            catalog.target_binding_uuid,
            tuple(
                template.name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex)
                for template in inputs.physical_descriptor.ordered_binding_module_templates
            ),
            lifecycle.certificate_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            lifecycle.certificate_user_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
            tuple(digest(item) for item in modules),
            tuple(digest(item) for item in sorted((*dml_paths, *runtime_paths))),
            tuple(digest(item) for item in signature_intents),
            digest(permission),
        ),
    )
    portable = canonical_bytes(
        b"dpone-mssql-r1-binding-portable-identity-v2\0",
        (
            "dpone-mssql-r1-binding-portable-identity-2",
            mapping,
            projections,
            tuple(modules),
            signer,
            permission,
            inventory,
        ),
    )
    instantiation = canonical_bytes(
        b"dpone-mssql-r1-binding-instantiation-contract-v2\0",
        (
            "dpone-mssql-r1-binding-instantiation-2",
            digest(physical_bytes),
            digest(security_bytes),
            source_digest,
            source.type_policy_authority.digest,
            digest(stable_bytes),
            catalog_digest,
            tuple(digest(item.canonical_bytes) for item in inputs.physical_descriptor.ordered_binding_module_templates),
            digest(scan_template.canonical_bytes),
            digest(scan.execution_semantics.request_authority.canonical_bytes),
            digest(
                canonical_bytes(
                    b"dpone-mssql-r1-stage-suffix-v2\0",
                    (tuple(item.canonical_bytes for item in scan_template.ordered_fixed_suffix_columns),),
                )
            ),
            digest(canonical_bytes(b"dpone-mssql-r1-buffer-matrix-v2\0", (tuple(digest(item) for item in modules),))),
            lifecycle.canonical_bytes,
            "exact_target_and_row_hash_v1",
            "pure_fail_closed_v2",
        ),
    )
    return canonical_bytes(
        b"dpone-mssql-r1-binding-pack-v2\0",
        (
            "dpone-mssql-r1-binding-pack-2",
            physical_bytes,
            security_bytes,
            source_bytes,
            stable_bytes,
            catalog_bytes,
            instantiation,
            portable,
        ),
    )


def _binding_api() -> tuple[Any, Any]:
    pack = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_pack")
    validation = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_validation")
    return pack, validation


def _bigint_column(ordinal: int, name: str, *, nullable: bool = False) -> MssqlR1RegisteredTargetColumnV1:
    shape = PostgresMssqlSourceScalarShapeV1(
        PostgresMssqlSourceScalarFamilyV1.INT8,
        20,
        -1,
        PostgresMssqlLengthKindV1.NOT_APPLICABLE,
        None,
        None,
        None,
    )
    target = derive_type_decision(shape, maximum_input_bytes=1024).target_shape
    return MssqlR1RegisteredTargetColumnV1(
        ordinal,
        name,
        "sys",
        "bigint",
        "sys",
        "bigint",
        target,
        8,
        19,
        0,
        nullable,
        False,
        False,
        False,
        False,
        0,
        None,
        None,
    )


def _security_for(descriptor: Any) -> MssqlR1SharedInstallSecurityProfileV2:
    baseline = authorities()[4]
    return MssqlR1SharedInstallSecurityProfileV2(
        baseline.contract_version,
        descriptor.digest,
        MssqlR1EnvironmentPrincipalProfileV1.CONTAINED_SQL_USERS_V1,
        tuple(
            MssqlR1EnvironmentPrincipalRefV1(role)
            for role in (
                MssqlR1SubjectRoleV3.PROVISIONER,
                MssqlR1SubjectRoleV3.RUNTIME,
                MssqlR1SubjectRoleV3.LOADER,
                MssqlR1SubjectRoleV3.OBSERVER,
            )
        ),
        _shared_signers(descriptor),
        baseline.ephemeral_secret_policy,
        baseline.ordered_lifecycle_transitions,
        MssqlR1PermissionClosurePolicyV1.create(descriptor),
        _memberships(),
        MssqlR1PrivateKeyDispositionV1.REMOVE_AFTER_ALL_SIGNATURES,
        MssqlR1ReinstallPolicyV1.OBSERVE_EXACT_OR_BLOCK,
    )


@lru_cache(maxsize=1)
def _baseline() -> FactoryInputs:
    source, scope, *_ = issue_authority(_feature_modules(), connector=FakeCatalogConnector(columns=2), policy=_policy())
    scope.close_if_active()
    catalog = registered_target_catalog()
    columns = (_bigint_column(1, "column_1"), _bigint_column(2, "column_2"))
    catalog = replace(catalog, ordered_columns=columns)
    physical, security, stable = authorities()[3], authorities()[4], authorities()[5]
    stable = replace(
        stable,
        route_source_authority_sha256=source.selected_source_authority_sha256,
        catalog_contract_digest=catalog.digest,
    )
    return FactoryInputs(physical, security, _binding_policy(security), source, stable, catalog)


def _with_catalog(inputs: FactoryInputs, catalog: Any) -> FactoryInputs:
    return replace(
        inputs,
        target_catalog=catalog,
        stable_target_authority=replace(inputs.stable_target_authority, catalog_contract_digest=catalog.digest),
    )


def _with_source_column_nullability(inputs: FactoryInputs, *, ordinal: int, nullable: bool) -> FactoryInputs:
    source = inputs.source_schema_authority
    policy = source.type_policy_authority
    columns = list(source.ordered_columns)
    column = columns[ordinal - 1]
    columns[ordinal - 1] = replace(
        column,
        nullable=nullable,
        source_column_ref=policy.source_column_ref(
            ordinal=column.projection_ordinal,
            name=column.name,
            nullable=nullable,
            source_shape=column.source_shape,
        ),
    )
    return replace(inputs, source_schema_authority=replace(source, ordered_columns=tuple(columns)))


def _with_unused_policy_decision(inputs: FactoryInputs) -> FactoryInputs:
    source = inputs.source_schema_authority
    int4_shape = PostgresMssqlSourceScalarShapeV1(
        PostgresMssqlSourceScalarFamilyV1.INT4,
        23,
        -1,
        PostgresMssqlLengthKindV1.NOT_APPLICABLE,
        None,
        None,
        None,
    )
    decisions = (
        *source.type_policy_authority.ordered_decisions,
        derive_type_decision(int4_shape, maximum_input_bytes=1024),
    )
    policy = PostgresMssqlTypePolicyAuthorityV1(
        "dpone-postgres-mssql-type-policy-1",
        tuple(sorted(decisions, key=lambda item: item.source_shape.sort_key)),
    )
    columns = tuple(
        replace(
            column,
            source_column_ref=policy.source_column_ref(
                ordinal=column.projection_ordinal,
                name=column.name,
                nullable=column.nullable,
                source_shape=column.source_shape,
            ),
        )
        for column in source.ordered_columns
    )
    return replace(
        inputs,
        source_schema_authority=replace(source, ordered_columns=columns, type_policy_authority=policy),
    )


def _factory_guard_inputs(inputs: FactoryInputs, delta: int) -> FactoryInputs:
    """Return an exact-type factory input at the byte guard plus ``delta``."""

    assert delta in {0, 1}
    templates = list(inputs.physical_descriptor.ordered_binding_module_templates)
    used = sum(len(value.canonical_bytes) for value in vars(inputs).values())
    growth = FACTORY_LIMIT - used + delta
    definition = MssqlR1DefinitionPayloadV1.create(
        MssqlR1DefinitionKindV1.MODULE_TEMPLATE,
        "X" * (len(templates[0].definition_template.utf8_bytes) + growth - 1) + "\n",
    )
    templates[0] = replace(templates[0], definition_template=definition)
    descriptor = replace(inputs.physical_descriptor, ordered_binding_module_templates=tuple(templates))
    security = _security_for(descriptor)
    return replace(
        inputs,
        physical_descriptor=descriptor,
        shared_security_profile=security,
        binding_signer_lifecycle_policy=_binding_policy(security),
    )


def _phase_mutation(inputs: FactoryInputs, phase: str) -> FactoryInputs:
    if phase == "01":
        return replace(inputs, physical_descriptor=object())
    if phase == "02":
        return _factory_guard_inputs(inputs, 1)
    if phase == "07":
        scan = next(
            procedure
            for procedure in inputs.physical_descriptor.ordered_procedures
            if procedure.portable_object.object_name == "dpone_scan_stage_v3"
        )
        suffix_name = scan.portable_object.result_contract.ordered_fixed_suffix_columns[0].name
        columns = list(inputs.target_catalog.ordered_columns)
        columns[1] = _bigint_column(2, suffix_name)
        return _with_catalog(inputs, replace(inputs.target_catalog, ordered_columns=tuple(columns)))
    if phase == "10":
        columns = list(inputs.target_catalog.ordered_columns)
        columns[1] = _bigint_column(2, "different_name")
        return _with_catalog(inputs, replace(inputs.target_catalog, ordered_columns=tuple(columns)))
    if phase == "11":
        return replace(
            inputs,
            stable_target_authority=replace(inputs.stable_target_authority, catalog_contract_digest=b"d" * 32),
        )
    if phase == "12":
        return replace(
            inputs,
            stable_target_authority=replace(
                inputs.stable_target_authority,
                target_object_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            ),
        )
    if phase == "13":
        uuid_shape = PostgresMssqlSourceScalarShapeV1(
            PostgresMssqlSourceScalarFamilyV1.UUID,
            2950,
            -1,
            PostgresMssqlLengthKindV1.NOT_APPLICABLE,
            None,
            None,
            None,
        )
        target = derive_type_decision(uuid_shape, maximum_input_bytes=1024).target_shape
        key = replace(
            inputs.target_catalog.ordered_columns[0],
            system_type_name="uniqueidentifier",
            user_type_name="uniqueidentifier",
            scalar_shape=target,
            max_length=16,
            precision=0,
        )
        key_columns = (key, *inputs.target_catalog.ordered_columns[1:])
        return _with_catalog(inputs, replace(inputs.target_catalog, ordered_columns=key_columns))
    if phase == "14":
        uuid_shape = PostgresMssqlSourceScalarShapeV1(
            PostgresMssqlSourceScalarFamilyV1.UUID,
            2950,
            -1,
            PostgresMssqlLengthKindV1.NOT_APPLICABLE,
            None,
            None,
            None,
        )
        target = derive_type_decision(uuid_shape, maximum_input_bytes=1024).target_shape
        changed = replace(
            inputs.target_catalog.ordered_columns[1],
            system_type_name="uniqueidentifier",
            user_type_name="uniqueidentifier",
            scalar_shape=target,
            max_length=16,
            precision=0,
        )
        return _with_catalog(
            inputs, replace(inputs.target_catalog, ordered_columns=(inputs.target_catalog.ordered_columns[0], changed))
        )
    if phase == "16":
        procedures = list(inputs.physical_descriptor.ordered_procedures)
        index = next(i for i, p in enumerate(procedures) if p.portable_object.object_name == "dpone_scan_stage_v3")
        procedure = procedures[index]
        authority = procedure.execution_semantics.request_authority
        grammar = authority.projection_grammar
        changed_authority = replace(
            authority, projection_grammar=replace(grammar, grammar_version=f"{grammar.grammar_version}-alternate")
        )
        procedures[index] = replace(
            procedure,
            execution_semantics=replace(procedure.execution_semantics, request_authority=changed_authority),
        )
        descriptor = replace(inputs.physical_descriptor, ordered_procedures=tuple(procedures))
        security = _security_for(descriptor)
        return replace(
            inputs,
            physical_descriptor=descriptor,
            shared_security_profile=security,
            binding_signer_lifecycle_policy=_binding_policy(security),
        )
    raise AssertionError(f"unsupported factory phase: {phase}")


def _assert_factory_reason(pack_module: Any, validation: Any, inputs: FactoryInputs, phase: str) -> None:
    factory = pack_module.MssqlR1BindingModulePackFactoryV2()
    try:
        factory.create(**inputs.kwargs())
    except Exception as exc:  # noqa: BLE001 - exact typed boundary asserted below.
        assert type(exc) is validation.MssqlR1BindingContractErrorV2
        reason = exc.reason.value if hasattr(exc.reason, "value") else exc.reason
        assert reason == PHASE_REASONS[phase]
        recovery, message = RECOVERY_AND_MESSAGE[reason]
        observed_recovery = exc.recovery_class.value if hasattr(exc.recovery_class, "value") else exc.recovery_class
        assert observed_recovery == recovery
        assert str(exc) == message
        assert exc.args == (message,)
        assert exc.__cause__ is None
        assert exc.__context__ is None
        return
    raise AssertionError(f"factory phase {phase} must reject")


def _factory_pair(earlier: str, later: str, observe_inputs: InputObserver) -> Literal["typed_rejection"]:
    baseline = _baseline()
    combined = _phase_mutation(_phase_mutation(baseline, later), earlier)
    later_only = _phase_mutation(baseline, later)
    earlier_only = _phase_mutation(baseline, earlier)

    def leaf_bytes(value: object) -> bytes:
        payload = getattr(value, "canonical_bytes", None)
        return (
            payload
            if type(payload) is bytes
            else f"inexact:{type(value).__module__}.{type(value).__qualname__}".encode()
        )

    observe_inputs(
        tuple(
            (
                f"{fixture}.{name}",
                hashlib.sha256(leaf_bytes(value)).hexdigest(),
            )
            for fixture, inputs in (
                ("combined", combined),
                ("later_only", later_only),
                ("earlier_only", earlier_only),
            )
            for name, value in vars(inputs).items()
        )
    )
    pack_module, validation = _binding_api()
    _assert_factory_reason(pack_module, validation, combined, earlier)
    _assert_factory_reason(pack_module, validation, later_only, later)
    _assert_factory_reason(pack_module, validation, earlier_only, earlier)
    return "typed_rejection"


bind_phase_pair_scenario("factory", _factory_pair)


def test_xmin_complete_keys_uses_actual_nonfirst_key_and_rebases_result_ordinal() -> None:
    """The complete-key artifact must select the real PK, not catalog column one."""

    pack_module, _ = _binding_api()
    inputs = _baseline()
    primary_key = replace(inputs.target_catalog.primary_key, ordered_key_column_ordinals=(2,))
    inputs = _with_catalog(inputs, replace(inputs.target_catalog, primary_key=primary_key))

    pack = pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())
    mappings = pack.portable_identity.target_mapping.ordered_columns
    assert tuple((item.ordinal, item.key_ordinal) for item in mappings) == ((1, None), (2, 1))

    complete = next(
        item
        for item in pack.portable_identity.ordered_stage_projections
        if item.artifact_kind.value == "xmin_complete_keys"
    )
    assert len(complete.ordered_business_columns) == 1
    key = complete.ordered_business_columns[0]
    catalog_key = inputs.target_catalog.ordered_columns[1]
    assert (
        key.ordinal,
        key.name,
        key.sql_type,
        key.maximum_length,
        key.precision,
        key.scale,
        key.nullable,
        key.collation,
    ) == (
        1,
        catalog_key.name,
        catalog_key.system_type_name,
        catalog_key.max_length,
        catalog_key.precision,
        catalog_key.scale,
        catalog_key.nullable,
        catalog_key.scalar_shape.collation,
    )
    assert complete.ordered_result_columns[0] == key
    assert tuple(item.ordinal for item in complete.ordered_result_columns) == tuple(
        range(1, len(complete.ordered_result_columns) + 1)
    )
    assert pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(pack.canonical_bytes) == pack


def test_factory_rejects_nullable_source_for_nonnull_nonfirst_target_key() -> None:
    """Source and target key nullability is an exact phase-13 contract."""

    pack_module, validation = _binding_api()
    inputs = _baseline()
    primary_key = replace(inputs.target_catalog.primary_key, ordered_key_column_ordinals=(2,))
    inputs = _with_catalog(inputs, replace(inputs.target_catalog, primary_key=primary_key))
    inputs = _with_source_column_nullability(inputs, ordinal=2, nullable=True)
    assert inputs.source_schema_authority.ordered_columns[1].nullable is True
    assert inputs.target_catalog.ordered_columns[1].nullable is False

    _assert_factory_reason(pack_module, validation, inputs, "13")


def test_factory_rejects_unused_type_policy_decision() -> None:
    """The selected relation must consume the policy's complete shape coverage."""

    pack_module, validation = _binding_api()
    inputs = _with_unused_policy_decision(_baseline())
    used = {item.source_shape.canonical_bytes for item in inputs.source_schema_authority.ordered_columns}
    covered = {
        item.source_shape.canonical_bytes
        for item in inputs.source_schema_authority.type_policy_authority.ordered_decisions
    }
    assert len(used) == 1
    assert len(covered) == 2
    assert used < covered

    _assert_factory_reason(pack_module, validation, inputs, "10")


def test_factory_repeat_success_round_trip_and_rotation_stability() -> None:
    pack_module, _ = _binding_api()
    factory = pack_module.MssqlR1BindingModulePackFactoryV2()
    first = factory.create(**_baseline().kwargs())
    second = factory.create(**_baseline().kwargs())
    assert first.canonical_bytes == second.canonical_bytes
    assert pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(first.canonical_bytes) == first
    identity = first.portable_identity
    assert tuple(item.artifact_kind.value for item in identity.ordered_stage_projections) == (
        "batch_payload",
        "xmin_delta",
        "xmin_complete_keys",
    )
    assert tuple(item.module_kind.value for item in identity.ordered_modules) == (
        "batch_mutate",
        "batch_row_hash",
        "batch_quality",
        "xmin_mutate",
        "xmin_row_hash",
        "xmin_quality",
    )
    invocations = tuple(item for module in identity.ordered_modules for item in module.buffer_plan.ordered_inputs)
    assert len(invocations) == 9
    assert {item.buffer_symbol.value for item in invocations} == {
        "batch_payload",
        "xmin_delta",
        "xmin_complete_keys",
    }
    assert all(
        tuple(source.value for source in item.ordered_envelope_sources)
        == (
            "open_stage_plan_canonical_bytes",
            "sha256_request_payload",
            "projection_json_from_open_stage_plan",
        )
        for item in invocations
    )
    mappings = identity.target_mapping.ordered_columns
    assert tuple(item.ordinal for item in mappings) == (1, 2)
    assert {item.key_ordinal for item in mappings} == {1, None}
    permissions = identity.permission_contract
    assert len(permissions.ordered_access_projections) == 8
    assert len(permissions.ordered_runtime_execute_paths) == 6
    assert len(permissions.ordered_signature_intents) == 6
    assert len(permissions.ordered_permission_paths) == 14
    inventory = identity.expected_inventory
    assert inventory.ordered_module_names == tuple(item.object_name for item in identity.ordered_modules)
    assert inventory.permission_contract_digest == permissions.digest


def test_factory_size_constants_and_failure_table_are_exact() -> None:
    pack_module, validation = _binding_api()
    enums = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
    assert pack_module.MAX_BINDING_FACTORY_EMBEDDED_INPUT_BYTES_V2 == FACTORY_LIMIT
    assert pack_module.MAX_BINDING_PACK_CANONICAL_BYTES_V2 == PACK_LIMIT
    assert pack_module.MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2 == ELEMENT_LIMIT
    assert tuple(item.value for item in enums.MssqlR1StageBufferSymbolV2) == (
        "batch_payload",
        "xmin_delta",
        "xmin_complete_keys",
    )
    assert tuple(item.value for item in enums.MssqlR1StageEnvelopeSourceV2) == (
        "open_stage_plan_canonical_bytes",
        "sha256_request_payload",
        "projection_json_from_open_stage_plan",
    )
    assert tuple(item.value for item in validation.MssqlR1BindingFailureReasonV2) == tuple(
        dict.fromkeys(PHASE_REASONS.values())
    ) + ("internal_invariant_violation",)
    assert tuple(item.value for item in validation.MssqlR1BindingRecoveryClassV2) == (
        "permanent_input_error",
        "operator_intervention",
    )
    for reason in validation.MssqlR1BindingFailureReasonV2:
        error = validation.MssqlR1BindingContractErrorV2(reason)
        recovery, message = RECOVERY_AND_MESSAGE[reason.value]
        assert error.reason is reason
        assert error.recovery_class.value == recovery
        assert str(error) == message
        assert error.args == (message,)
        assert error.__cause__ is error.__context__ is None


def test_factory_phase02_measures_without_encoding_before_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """The public factory must reject guard+1 without materializing authority bytes."""

    pack_module, validation = _binding_api()
    factory = pack_module.MssqlR1BindingModulePackFactoryV2()

    class WrongType:
        @property
        def canonical_bytes(self) -> bytes:
            raise AssertionError("phase 01 must run before canonical-byte access")

    wrong = replace(_baseline(), physical_descriptor=WrongType())
    _assert_factory_reason(pack_module, validation, wrong, "01")

    over_guard = _factory_guard_inputs(_baseline(), 1)
    at_guard = _factory_guard_inputs(_baseline(), 0)
    guarded_types: dict[type[object], tuple[str, property]] = {}
    canonical_accesses: list[str] = []
    for name, value in vars(over_guard).items():
        descriptor = inspect.getattr_static(type(value), "canonical_bytes")
        assert isinstance(descriptor, property) and descriptor.fget is not None
        guarded_types[type(value)] = (name, descriptor)

    for model, (name, descriptor) in guarded_types.items():
        getter = descriptor.fget
        assert getter is not None

        def guarded(instance: object, *, _getter: Any = getter, _name: str = name) -> bytes:
            canonical_accesses.append(_name)
            return _getter(instance)

        monkeypatch.setattr(model, "canonical_bytes", property(guarded))

    _assert_factory_reason(pack_module, validation, over_guard, "02")
    assert canonical_accesses == []

    result = factory.create(**at_guard.kwargs())
    assert result == pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(result.canonical_bytes)
    assert set(canonical_accesses) == set(vars(at_guard))


@pytest.mark.parametrize("authority_name", tuple(FactoryInputs.__dataclass_fields__))
def test_factory_round_trips_each_upstream_authority_before_derivation(
    authority_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every exact authority must be decoded before semantic derivation and again for the final pack proof."""

    pack_module, _ = _binding_api()
    inputs = _baseline()
    authority = getattr(inputs, authority_name)
    model = type(authority)
    original = model.from_canonical_bytes
    observed: list[bytes] = []

    def recording_decoder(_cls: type[object], payload: bytes) -> object:
        observed.append(payload)
        return original(payload)

    monkeypatch.setattr(model, "from_canonical_bytes", classmethod(recording_decoder))
    result = pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())

    assert len(observed) >= 2
    assert all(payload == authority.canonical_bytes for payload in observed)
    assert pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(result.canonical_bytes) == result


def test_factory_final_redecode_requires_exact_pack_equality(monkeypatch: pytest.MonkeyPatch) -> None:
    """A corrupted final decode is an internal postcondition failure, never a returned pack."""

    pack_module, validation = _binding_api()
    inputs = _baseline()
    expected = pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())
    original = pack_module.decode_canonical_bytes
    observed: list[bytes] = []
    pack_domain = b"dpone-mssql-r1-binding-pack-v2\0"

    def corrupt_final_identity(payload: bytes, domain: bytes, *, field_count: int) -> tuple[object, ...]:
        values = original(payload, domain, field_count=field_count)
        if domain != pack_domain:
            return values
        observed.append(payload)
        return (*values[:-1], values[-1] + b"corrupt")

    monkeypatch.setattr(pack_module, "decode_canonical_bytes", corrupt_final_identity)
    with pytest.raises(validation.MssqlR1BindingContractErrorV2) as caught:
        pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())

    error = caught.value
    assert observed == [expected.canonical_bytes]
    assert error.reason.value == "internal_invariant_violation"
    assert error.recovery_class.value == "operator_intervention"
    assert str(error) == RECOVERY_AND_MESSAGE["internal_invariant_violation"][1]
    assert error.args == (RECOVERY_AND_MESSAGE["internal_invariant_violation"][1],)
    assert error.__cause__ is error.__context__ is None


def test_factory_derived_output_bounds_fail_as_internal_postcondition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Impossible derived-output quota breaches are phase 99, never public phase 02."""

    pack_module, validation = _binding_api()
    factory = pack_module.MssqlR1BindingModulePackFactoryV2()
    inputs = _baseline()
    baseline = factory.create(**inputs.kwargs())

    for constant, impossible_limit in (
        ("MAX_BINDING_PACK_CANONICAL_BYTES_V2", len(baseline.canonical_bytes) - 1),
        ("MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2", 1),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(pack_module, constant, impossible_limit)
            with pytest.raises(validation.MssqlR1BindingContractErrorV2) as caught:
                factory.create(**inputs.kwargs())
        error = caught.value
        assert error.reason.value == "internal_invariant_violation"
        assert error.reason.value != "canonical_size_exceeded"
        assert error.recovery_class.value == "operator_intervention"
        assert str(error) == RECOVERY_AND_MESSAGE["internal_invariant_violation"][1]
        assert error.args == (RECOVERY_AND_MESSAGE["internal_invariant_violation"][1],)
        assert error.__cause__ is error.__context__ is None


def test_binding_public_annotations_are_exact_and_closed() -> None:
    """Freeze every specification-tabled model and factory annotation."""

    pack_module, _ = _binding_api()
    mapping = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
    stage = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
    modules = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
    permissions = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_permissions")

    from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
    from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
        MssqlR1PhysicalResourceAccessV1,
        MssqlR1PhysicalResourceRefV1,
    )
    from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import MssqlR1PhysicalSchemaDescriptorV1
    from dpone.contracts.mssql_r1_v3_provider_security_enums import (
        MssqlR1CertificateCreationProfileV1,
        MssqlR1CertificateSignatureAlgorithmV1,
    )
    from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
        MssqlR1BindingSignerLifecyclePolicyV2,
        MssqlR1ResolvedPermissionPathV2,
    )
    from dpone.contracts.mssql_r1_v3_provider_security_principals import MssqlR1EnvironmentPrincipalRefV1
    from dpone.contracts.mssql_r1_v3_registered_target_catalog import (
        MssqlR1RegisteredTargetCatalogV1,
        MssqlR1RegisteredTargetColumnRefV1,
    )
    from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1ResultColumnV3
    from dpone.contracts.mssql_r1_v3_stage_evidence import R1StageArtifactKindV1
    from dpone.contracts.mssql_r1_v3_verified_target_authority import MssqlR1RotationStableTargetAuthorityV1
    from dpone.contracts.postgres_mssql_source_schema_authority import (
        PostgresMssqlSelectedRelationSchemaAuthorityV1,
    )
    from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlSourceColumnRefV1

    def tuple_of(model: object) -> object:
        return GenericAlias(tuple, (model, Ellipsis))

    expected: dict[type[object], dict[str, object]] = {
        mapping.MssqlR1RegisteredTargetRefV2: {
            "contract_version": Literal["dpone-mssql-r1-registered-target-ref-2"],
            "target_binding_uuid": UUID,
            "target_object_uuid": UUID,
            "resource_ref": MssqlR1PhysicalResourceRefV1,
            "stable_target_authority_digest": bytes,
            "target_catalog_digest": bytes,
        },
        mapping.MssqlR1BusinessColumnMappingV2: {
            "contract_version": Literal["dpone-mssql-r1-business-column-mapping-2"],
            "ordinal": int,
            "source_column_ref": PostgresMssqlSourceColumnRefV1,
            "target_column_ref": MssqlR1RegisteredTargetColumnRefV1,
            "type_decision_id": str,
            "key_ordinal": Literal[1] | None,
        },
        mapping.MssqlR1BindingTargetMappingV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-target-mapping-2"],
            "registered_target": mapping.MssqlR1RegisteredTargetRefV2,
            "source_schema_authority_digest": bytes,
            "route_source_authority_sha256": bytes,
            "type_policy_authority_digest": bytes,
            "ordered_columns": tuple_of(mapping.MssqlR1BusinessColumnMappingV2),
        },
        stage.MssqlR1StageScanProjectionV2: {
            "contract_version": Literal["dpone-mssql-r1-stage-scan-projection-2"],
            "artifact_kind": R1StageArtifactKindV1,
            "core_scan_procedure_ref": MssqlR1PhysicalResourceRefV1,
            "stage_scan_template_digest": bytes,
            "ordered_business_columns": tuple[MssqlR1ResultColumnV3, ...],
            "ordered_result_columns": tuple[MssqlR1ResultColumnV3, ...],
        },
        stage.MssqlR1StageScanInvocationV2: {
            "contract_version": Literal["dpone-mssql-r1-stage-scan-invocation-2"],
            "ordinal": int,
            "artifact_kind": R1StageArtifactKindV1,
            "buffer_symbol": importlib.import_module(
                "dpone.contracts.mssql_r1_v3_binding_modules"
            ).MssqlR1StageBufferSymbolV2,
            "projection_digest": bytes,
            "scan_procedure_digest": bytes,
            "ordered_envelope_sources": tuple_of(
                importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules").MssqlR1StageEnvelopeSourceV2
            ),
        },
        modules.MssqlR1ModuleStageBufferPlanV2: {
            "contract_version": Literal["dpone-mssql-r1-module-stage-buffer-plan-2"],
            "module_kind": MssqlR1BindingModuleKindV1,
            "ordered_inputs": tuple_of(stage.MssqlR1StageScanInvocationV2),
        },
        modules.MssqlR1InstantiatedBindingModuleV2: {
            "contract_version": Literal["dpone-mssql-r1-instantiated-binding-module-2"],
            "module_kind": MssqlR1BindingModuleKindV1,
            "schema_name": Literal["dpone_authority"],
            "object_name": str,
            "descriptor_template_digest": bytes,
            "target_mapping_digest": bytes,
            "buffer_plan": modules.MssqlR1ModuleStageBufferPlanV2,
            "ordered_target_access_intents": tuple[MssqlR1PhysicalResourceAccessV1, ...],
        },
        permissions.MssqlR1BindingSignerIdentityV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-signer-identity-2"],
            "target_binding_uuid": UUID,
            "lifecycle_policy": MssqlR1BindingSignerLifecyclePolicyV2,
            "certificate_name": str,
            "certificate_user_name": str,
            "certificate_subject": str,
            "certificate_owner": MssqlR1EnvironmentPrincipalRefV1,
            "start_date_yyyymmdd": str,
            "expiry_date_yyyymmdd": str,
            "certificate_creation_profile": MssqlR1CertificateCreationProfileV1,
            "signature_algorithm": MssqlR1CertificateSignatureAlgorithmV1,
            "secret_policy_digest": bytes,
        },
        permissions.MssqlR1BindingSignatureIntentV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-signature-intent-2"],
            "ordinal": int,
            "module_kind": MssqlR1BindingModuleKindV1,
            "semantic_module_digest": bytes,
            "signer_identity_digest": bytes,
        },
        permissions.MssqlR1BindingAccessPermissionProjectionV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-access-permission-projection-2"],
            "source_access": MssqlR1PhysicalResourceAccessV1,
            "permission_path": MssqlR1ResolvedPermissionPathV2,
        },
        permissions.MssqlR1BindingPermissionContractV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-permission-2"],
            "permission_profile": Literal["exact_target_and_row_hash_v1"],
            "ordered_access_projections": tuple_of(permissions.MssqlR1BindingAccessPermissionProjectionV2),
            "ordered_runtime_execute_paths": tuple[MssqlR1ResolvedPermissionPathV2, ...],
            "ordered_signature_intents": tuple_of(permissions.MssqlR1BindingSignatureIntentV2),
        },
        pack_module.MssqlR1ExpectedBindingInventoryV2: {
            "contract_version": Literal["dpone-mssql-r1-expected-binding-inventory-2"],
            "target_binding_uuid": UUID,
            "ordered_module_names": tuple[str, ...],
            "certificate_name": str,
            "certificate_user_name": str,
            "ordered_semantic_module_digests": tuple[bytes, ...],
            "ordered_permission_path_digests": tuple[bytes, ...],
            "ordered_signature_intent_digests": tuple[bytes, ...],
            "permission_contract_digest": bytes,
        },
        pack_module.MssqlR1BindingInstantiationContractV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-instantiation-2"],
            "physical_descriptor_digest": bytes,
            "shared_security_profile_digest": bytes,
            "source_schema_authority_digest": bytes,
            "type_policy_authority_digest": bytes,
            "stable_target_authority_digest": bytes,
            "target_catalog_digest": bytes,
            "ordered_binding_template_digests": tuple[bytes, ...],
            "stage_scan_template_digest": bytes,
            "stage_scan_request_authority_digest": bytes,
            "stage_scan_suffix_digest": bytes,
            "buffer_matrix_digest": bytes,
            "signer_lifecycle_policy": MssqlR1BindingSignerLifecyclePolicyV2,
            "permission_projection_profile": Literal["exact_target_and_row_hash_v1"],
            "compiler_policy": Literal["pure_fail_closed_v2"],
        },
        pack_module.MssqlR1BindingPortableIdentityV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-portable-identity-2"],
            "target_mapping": mapping.MssqlR1BindingTargetMappingV2,
            "ordered_stage_projections": tuple_of(stage.MssqlR1StageScanProjectionV2),
            "ordered_modules": tuple_of(modules.MssqlR1InstantiatedBindingModuleV2),
            "signer_identity": permissions.MssqlR1BindingSignerIdentityV2,
            "permission_contract": permissions.MssqlR1BindingPermissionContractV2,
            "expected_inventory": pack_module.MssqlR1ExpectedBindingInventoryV2,
        },
        pack_module.MssqlR1BindingModulePackV2: {
            "contract_version": Literal["dpone-mssql-r1-binding-pack-2"],
            "physical_descriptor_payload": bytes,
            "shared_security_profile_payload": bytes,
            "source_schema_authority_payload": bytes,
            "stable_target_authority_payload": bytes,
            "target_catalog_payload": bytes,
            "binding_instantiation_contract_payload": bytes,
            "portable_identity": pack_module.MssqlR1BindingPortableIdentityV2,
        },
    }
    assert {model: get_type_hints(model) for model in expected} == expected

    create = pack_module.MssqlR1BindingModulePackFactoryV2.create
    signature = inspect.signature(create)
    assert tuple(signature.parameters) == (
        "self",
        "physical_descriptor",
        "shared_security_profile",
        "binding_signer_lifecycle_policy",
        "source_schema_authority",
        "stable_target_authority",
        "target_catalog",
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in signature.parameters.items()
        if name != "self"
    )
    assert get_type_hints(create) == {
        "physical_descriptor": MssqlR1PhysicalSchemaDescriptorV1,
        "shared_security_profile": MssqlR1SharedInstallSecurityProfileV2,
        "binding_signer_lifecycle_policy": MssqlR1BindingSignerLifecyclePolicyV2,
        "source_schema_authority": PostgresMssqlSelectedRelationSchemaAuthorityV1,
        "stable_target_authority": MssqlR1RotationStableTargetAuthorityV1,
        "target_catalog": MssqlR1RegisteredTargetCatalogV1,
        "return": pack_module.MssqlR1BindingModulePackV2,
    }


def test_factory_never_normalizes_baseexception(monkeypatch: pytest.MonkeyPatch) -> None:
    pack_module, _ = _binding_api()
    inputs = _baseline()
    valid_pack = pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())

    def interrupted(_instance: object) -> bytes:
        raise KeyboardInterrupt("must escape unchanged")

    with monkeypatch.context() as factory_patch:
        factory_patch.setattr(type(inputs.physical_descriptor), "canonical_bytes", property(interrupted))
        with pytest.raises(KeyboardInterrupt, match="must escape unchanged") as caught:
            pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())
        assert caught.value.__cause__ is caught.value.__context__ is None

    def decode_interrupted(_cls: type[object], _payload: bytes) -> object:
        raise KeyboardInterrupt("reader must escape unchanged")

    with monkeypatch.context() as reader_patch:
        reader_patch.setattr(
            type(inputs.physical_descriptor),
            "from_canonical_bytes",
            classmethod(decode_interrupted),
        )
        with pytest.raises(KeyboardInterrupt, match="reader must escape unchanged") as caught:
            pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(valid_pack.canonical_bytes)
        assert caught.value.__cause__ is caught.value.__context__ is None


def test_ordinary_upstream_exception_is_redacted_and_unlinked(monkeypatch: pytest.MonkeyPatch) -> None:
    pack_module, validation = _binding_api()
    inputs = _baseline()
    valid_pack = pack_module.MssqlR1BindingModulePackFactoryV2().create(**inputs.kwargs())

    def secret_failure(_cls: type[object], _payload: bytes) -> object:
        raise ValueError("secret payload must never escape")

    monkeypatch.setattr(
        type(inputs.physical_descriptor),
        "from_canonical_bytes",
        classmethod(secret_failure),
    )
    with pytest.raises(validation.MssqlR1BindingContractErrorV2) as caught:
        pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(valid_pack.canonical_bytes)
    error = caught.value
    assert error.reason.value == "internal_invariant_violation"
    assert error.recovery_class.value == "operator_intervention"
    assert str(error) == RECOVERY_AND_MESSAGE["internal_invariant_violation"][1]
    assert "secret" not in str(error)
    assert error.__cause__ is error.__context__ is None


def test_maximum_upstream_shape_fits_factory_guard_and_universal_pack_bound() -> None:
    """Prove the frozen maxima without importing any future Binding module."""

    source, scope, *_ = issue_authority(
        _feature_modules(),
        connector=FakeCatalogConnector(columns=1024),
        policy=_policy(),
    )
    scope.close_if_active()
    catalog = registered_target_catalog()
    columns = tuple(_bigint_column(ordinal, f"column_{ordinal}") for ordinal in range(1, 1025))
    primary = MssqlR1RegisteredTargetIndexV1(
        1,
        "PK_orders",
        "clustered",
        True,
        True,
        False,
        False,
        False,
        False,
        None,
        (1,),
        (False,),
        (),
    )
    all_ordinals = tuple(range(1, 1025))
    secondary = tuple(
        MssqlR1RegisteredTargetIndexV1(
            ordinal,
            f"IX_{ordinal:04d}",
            "nonclustered",
            False,
            False,
            False,
            False,
            False,
            False,
            None,
            all_ordinals,
            (False,) * 1024,
            (),
        )
        for ordinal in range(2, 1001)
    )
    catalog = replace(
        catalog,
        ordered_columns=columns,
        primary_key=primary,
        ordered_secondary_indexes=secondary,
    )
    physical, security, stable = authorities()[3], authorities()[4], authorities()[5]
    stable = replace(
        stable,
        route_source_authority_sha256=source.selected_source_authority_sha256,
        catalog_contract_digest=catalog.digest,
    )
    maximum = FactoryInputs(physical, security, _binding_policy(security), source, stable, catalog)
    exact_input_bytes = sum(len(value.canonical_bytes) for value in vars(maximum).values())
    assert exact_input_bytes == 20_073_257
    assert exact_input_bytes < FACTORY_LIMIT
    input_vector = [
        {"name": name, "sha256": hashlib.sha256(value.canonical_bytes).hexdigest()}
        for name, value in vars(maximum).items()
    ]
    input_vector_preimage = (
        json.dumps(input_vector, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    input_vector_digest = hashlib.sha256(b"dpone-binding-v2-input-vectors-v1\0" + input_vector_preimage).hexdigest()
    assert tuple(item["name"] for item in input_vector) == (
        "physical_descriptor",
        "shared_security_profile",
        "binding_signer_lifecycle_policy",
        "source_schema_authority",
        "stable_target_authority",
        "target_catalog",
    )
    assert input_vector_digest == "ce7f395e231413feaf1c20bf79485a92bffcc8ee3d5fbcd779cbc509023551af"
    complete_pack_shape = _independent_complete_pack_shape(maximum)
    assert len(complete_pack_shape) == 21_504_928
    assert hashlib.sha256(complete_pack_shape).hexdigest() == (
        "944ddbc711adfd54690da1127ff5a3226c552c38a8451fa282f20e160150030b"
    )
    assert len(complete_pack_shape) < PACK_LIMIT

    # The only upstream unbounded leaf is a definition body. Increasing its
    # UTF-8 body by N increases both the descriptor and six-authority preimage
    # by exactly N, which gives allocation-independent guard/guard+1 fixtures.
    templates = list(physical.ordered_binding_module_templates)
    original_definition = templates[0].definition_template
    guard_growth = FACTORY_LIMIT - exact_input_bytes
    padded_definition = MssqlR1DefinitionPayloadV1.create(
        original_definition.definition_kind,
        original_definition.utf8_bytes.decode("utf-8")[:-1] + "X" * guard_growth + "\n",
    )
    templates[0] = replace(templates[0], definition_template=padded_definition)
    padded_descriptor = replace(physical, ordered_binding_module_templates=tuple(templates))
    padded_security = _security_for(padded_descriptor)
    padded = replace(
        maximum,
        physical_descriptor=padded_descriptor,
        shared_security_profile=padded_security,
        binding_signer_lifecycle_policy=_binding_policy(padded_security),
    )
    assert sum(len(value.canonical_bytes) for value in vars(padded).values()) == FACTORY_LIMIT
    padded_pack_shape = _independent_complete_pack_shape(padded)
    assert len(padded_pack_shape) == 26_597_495
    assert hashlib.sha256(padded_pack_shape).hexdigest() == (
        "ed83d3972c0b24b130a9d03e7bc55c56ee3fbfe0fca48bf2f227859f81654cc9"
    )
    assert len(padded_pack_shape) < PACK_LIMIT
    templates[0] = replace(
        templates[0],
        definition_template=MssqlR1DefinitionPayloadV1.create(
            original_definition.definition_kind,
            padded_definition.utf8_bytes.decode("utf-8")[:-1] + "X\n",
        ),
    )
    plus_one_descriptor = replace(physical, ordered_binding_module_templates=tuple(templates))
    plus_one_security = _security_for(plus_one_descriptor)
    plus_one = replace(
        maximum,
        physical_descriptor=plus_one_descriptor,
        shared_security_profile=plus_one_security,
        binding_signer_lifecycle_policy=_binding_policy(plus_one_security),
    )
    assert sum(len(value.canonical_bytes) for value in vars(plus_one).values()) == FACTORY_LIMIT + 1

    # Closed monotonic bound: unbounded definition bytes occur only in the one
    # embedded descriptor; every derived variable-width leaf is bounded by the
    # 1024-column cardinality and 128 UTF-16-unit identifier ceiling.  The
    # constants include tag/u32/u64 framing at each repeated layer.
    fixed_derived_bytes = 2 * 1024 * 1024
    maximum_per_column_derived_bytes = 24 * 1024
    maximum_derived_bytes = fixed_derived_bytes + 1024 * maximum_per_column_derived_bytes
    assert FACTORY_LIMIT + maximum_derived_bytes == 52_428_800
    assert FACTORY_LIMIT + maximum_derived_bytes < PACK_LIMIT

    # Every valid index has at most 1024 distinct key/include ordinals and one
    # descending flag per key.  Remaining input and derived model fields use a
    # deliberately conservative 256 framed elements per business column.
    maximum_index_elements = 999 * (13 + 2 * 1024)
    maximum_column_and_derived_elements = 1024 * 256
    maximum_fixed_elements = 65_536
    maximum_elements = maximum_index_elements + maximum_column_and_derived_elements + maximum_fixed_elements
    assert maximum_elements == 2_386_619
    assert maximum_elements < ELEMENT_LIMIT

    # Canonical nulls are the smallest possible sequence leaves (tag + u32
    # framing), so the hostile quota boundary itself also fits below 64 MiB.
    minimum_quota_payload_bytes = 1 + 5 * ELEMENT_LIMIT
    assert minimum_quota_payload_bytes == 20_971_521
    assert minimum_quota_payload_bytes < PACK_LIMIT
