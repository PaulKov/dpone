"""Hermetic builders for Provider Attestation V2 partitions."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_r1_v3_binding_pack import MssqlR1BindingModulePackFactoryV2, MssqlR1BindingModulePackV2
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_provider_attestation_binding_observation import (
    MssqlR1ObservedBindingModuleV1,
    MssqlR1ObservedBindingPrefixObjectV1,
    MssqlR1ObservedBindingSignatureV1,
    MssqlR1ObservedBindingSignerV2,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
    QUERY_RESULT_PAIRS,
    MssqlR1AttestationQueryKindV1,
    MssqlR1AttestationResultAuthorityKindV1,
    MssqlR1MigrationStatementPhaseV1,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_identity import (
    MssqlR1CertifiedServerBuildProfileV1,
    MssqlR1MigrationStatementRefV1,
    MssqlR1MigrationTargetRefV1,
    MssqlR1ObservedDatabaseIdentityV1,
    MssqlR1ProviderAttestationModuleDefinitionAuthorityV2,
    MssqlR1ProviderAttestationStatementAuthorityV2,
    MssqlR1ProviderAttestationStatementRegistryV2,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (
    MssqlR1BindingPrefixInventoryQueryResultV1,
    MssqlR1CertificateQueryResultV2,
    MssqlR1ModuleQueryResultV1,
    MssqlR1PermissionQueryResultV2,
    MssqlR1PostInstallStatementResultV2,
    MssqlR1SchemaQueryResultV1,
    MssqlR1SignatureQueryResultV1,
    MssqlR1TableQueryResultV1,
    project_schema_permission_rule,
)
from dpone.contracts.mssql_r1_v3_provider_security_certificate import MssqlR1CertificateCatalogObservationV1
from dpone.contracts.mssql_r1_v3_provider_security_principals import MssqlR1EnvironmentPrincipalRefV1
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import MssqlR1RegisteredTargetColumnV1
from dpone.contracts.mssql_r1_v3_schema_attestation import MssqlR1SchemaAttestationV3
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1ModuleOptionsV3
from dpone.contracts.mssql_r1_v3_schema_observation import (
    MssqlR1ModuleSignatureObservationV3,
    MssqlR1ObservedPermissionV3,
    MssqlR1ObservedPrincipalAuthorityKindV3,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
    MssqlR1ObservedTriggerIdentityV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SchemaObjectKindV3, MssqlR1SignerProfileKindV3
from dpone.contracts.mssql_r1_v3_schema_security import (
    ENVIRONMENT_ROLES,
    MODULE_ROLE_BY_PROFILE,
    MssqlR1AuthenticationTypeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1PrincipalAuthoritySetV3,
    MssqlR1PrincipalBindingV3,
    MssqlR1PrincipalTypeV3,
    MssqlR1SubjectRoleV3,
)
from dpone.contracts.mssql_r1_v3_verified_target_authority import MssqlR1RotationStableTargetAuthorityV1
from dpone.contracts.postgres_mssql_source_schema_authority import (
    PostgresMssqlSelectedRelationSchemaAuthorityV1,
    derive_observed_source_column,
)
from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1
from tests.postgres_mssql_r1_v3_type_target_test_support import authorities, registration
from tests.test_postgres_mssql_r1_v3_provider_security_contract import _binding_policy
from tests.test_postgres_mssql_r1_v3_registered_target_catalog_contract import registered_target_catalog
from tests.test_postgres_mssql_r1_v3_schema_contract_v2 import _attestation, _attestation_parts, _verification

DIGEST = bytes.fromhex("11" * 32)
NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
SOURCE_DOCUMENT = (
    b'{"database":{"canonical_name":"warehouse","oid":16384},"dialect":"postgres",'
    b'"principals":{"effective":{"canonical_name":"dpone_reader","oid":17001},'
    b'"session":{"canonical_name":"dpone_login","oid":17002}},'
    b'"relation":{"namespace_oid":2200,"relation":"orders","relation_oid":22001,"schema":"sales"},'
    b'"role":"source","system_identifier":"7272727272727272727","timeline_id":7,'
    b'"topology_role":"primary","version":1}'
)


def digest(seed: int) -> bytes:
    return hashlib.sha256(seed.to_bytes(4, "big")).digest()


def module_options() -> MssqlR1ModuleOptionsV3:
    return MssqlR1ModuleOptionsV3("caller", True, True, False, False, MssqlR1SignerProfileKindV3.NONE)


def build_profile() -> MssqlR1CertifiedServerBuildProfileV1:
    return MssqlR1CertifiedServerBuildProfileV1("sqlserver-2022-standalone-r1", 16, "16.", 160, ("RTM",), (3,))


def statement_ref(
    statement_id: str = "schema_observe",
    spec: bytes = DIGEST,
) -> MssqlR1MigrationStatementRefV1:
    return MssqlR1MigrationStatementRefV1(statement_id, spec, MssqlR1MigrationStatementPhaseV1.POST_DECISION_ATTEST)


def observed_identity(
    *,
    server: bytes = DIGEST,
    database_id: int = 5,
    database_guid: UUID = UUID("11111111-1111-4111-8111-111111111111"),
    family: UUID = UUID("22222222-2222-4222-8222-222222222222"),
    fork: UUID = UUID("33333333-3333-4333-8333-333333333333"),
    name: str = "r1_target",
) -> MssqlR1ObservedDatabaseIdentityV1:
    return MssqlR1ObservedDatabaseIdentityV1(
        server,
        database_id,
        database_guid,
        family,
        fork,
        name,
        hashlib.sha256(name.encode("utf-8")).digest(),
        "Latin1_General_100_CI_AS_SC_UTF8",
        160,
        "16.0.1000.6",
        "RTM",
        3,
        build_profile(),
    )


def target_ref_for(identity: MssqlR1ObservedDatabaseIdentityV1) -> MssqlR1MigrationTargetRefV1:
    return MssqlR1MigrationTargetRefV1("dpone-mssql-target-physical-identity-1", identity.projected_target_digest())


def binding_modules() -> tuple[MssqlR1ObservedBindingModuleV1, ...]:
    return tuple(
        MssqlR1ObservedBindingModuleV1(
            kind,
            "dpone_authority",
            f"dpone_{kind.value}_v3",
            index,
            b"CREATE PROCEDURE dbo.x AS\n",
            module_options(),
        )
        for index, kind in enumerate(MssqlR1BindingModuleKindV1, 1)
    )


def binding_signatures() -> tuple[MssqlR1ObservedBindingSignatureV1, ...]:
    return tuple(
        MssqlR1ObservedBindingSignatureV1(hashlib.sha256(kind.value.encode()).digest(), index, 90 + index, b"crypt")
        for index, kind in enumerate(MssqlR1BindingModuleKindV1, 1)
    )


def prefix_object() -> MssqlR1ObservedBindingPrefixObjectV1:
    return MssqlR1ObservedBindingPrefixObjectV1("dpone_authority", "dpone_b_x_cert_v3", "certificate", 9)


def certificate_observation(
    name: str = "attestor_cert",
    subject: str = "dpone R1 V3 attestor",
    user: str = "attestor_user",
    thumb: bytes = b"thumbprint-attestor-000000000001",
    sid: bytes = b"sid-attestor-000000000000000001",
) -> MssqlR1CertificateCatalogObservationV1:
    return MssqlR1CertificateCatalogObservationV1(
        name,
        True,
        subject,
        "20000101",
        "99991231",
        MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER),
        thumb,
        False,
        user,
        True,
        MssqlR1PrincipalTypeV3.CERTIFICATE,
        MssqlR1AuthenticationTypeV3.NONE,
        sid,
        thumb,
        (),
        (),
    )


def binding_signer(
    target: UUID = UUID("10000000-0000-0000-0000-000000000001"),
) -> MssqlR1ObservedBindingSignerV2:
    return MssqlR1ObservedBindingSignerV2(
        target,
        certificate_observation(
            "dpone_b_10000000000000000000000000000001_cert_v3",
            "dpone R1 V3 binding 10000000000000000000000000000001",
            "dpone_b_10000000000000000000000000000001_cert_user_v3",
            b"thumbprint-binding-00000000000001",
            b"sid-binding-00000000000000000001",
        ),
        400,
        10,
        11,
    )


def schema_parts() -> tuple[Any, ...]:
    return _attestation_parts()


def schema_attestation_fixture(**overrides: Any) -> MssqlR1SchemaAttestationV3:
    return _attestation(**overrides)


def statement_registry(
    modules: tuple[MssqlR1ObservedBindingModuleV1, ...] | None = None,
) -> MssqlR1ProviderAttestationStatementRegistryV2:
    observed = modules or binding_modules()
    statements = []
    for ordinal, (kind, result) in enumerate(QUERY_RESULT_PAIRS, 1):
        definitions = ()
        if kind is MssqlR1AttestationQueryKindV1.MODULE:
            definitions = tuple(
                MssqlR1ProviderAttestationModuleDefinitionAuthorityV2(
                    item.module_kind,
                    hashlib.sha256(item.object_name.encode()).digest(),
                    item.normalized_definition_bytes,
                    item.persisted_options,
                )
                for item in observed
            )
        query = f"SELECT {ordinal};\n".encode()
        statements.append(
            MssqlR1ProviderAttestationStatementAuthorityV2(
                "dpone-r1-provider-attestation-statement-2",
                ordinal,
                MssqlR1MigrationStatementRefV1(
                    kind.value,
                    hashlib.sha256(f"{kind.value}-spec".encode()).digest(),
                    MssqlR1MigrationStatementPhaseV1.POST_DECISION_ATTEST,
                ),
                kind,
                result,
                query,
                definitions,
            )
        )
    return MssqlR1ProviderAttestationStatementRegistryV2(
        "dpone-r1-provider-attestation-registry-2",
        hashlib.sha256(b"renderer-registry").digest(),
        tuple(statements),
    )


def _int8_shape() -> PostgresMssqlSourceScalarShapeV1:
    return PostgresMssqlSourceScalarShapeV1(
        PostgresMssqlSourceScalarFamilyV1.INT8,
        20,
        -1,
        PostgresMssqlLengthKindV1.NOT_APPLICABLE,
        None,
        None,
        None,
    )


def _bigint_column(ordinal: int, name: str) -> MssqlR1RegisteredTargetColumnV1:
    target = derive_type_decision(_int8_shape(), maximum_input_bytes=1024).target_shape
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
        False,
        False,
        False,
        False,
        False,
        0,
        None,
        None,
    )


def _source_authority() -> PostgresMssqlSelectedRelationSchemaAuthorityV1:
    shape = _int8_shape()
    policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(shape, maximum_input_bytes=1024),),
        (shape,),
    )
    selected = hashlib.sha256(SOURCE_DOCUMENT).digest()
    columns = []
    for ordinal in (1, 2):
        columns.append(
            derive_observed_source_column(
                policy,
                {
                    "namespace_oid": 2200,
                    "relation_oid": 22001,
                    "attribute_number": ordinal,
                    "column_name": f"column_{ordinal}",
                    "type_oid": 20,
                    "type_namespace_oid": 11,
                    "type_namespace_name": "pg_catalog",
                    "type_name": "int8",
                    "type_kind": "b",
                    "type_modifier": -1,
                    "nullable": False,
                    "collation_oid": 0,
                    "generated_kind": "",
                    "identity_kind": "",
                },
                ordinal,
                selected,
            )
        )
    return PostgresMssqlSelectedRelationSchemaAuthorityV1(
        "dpone-postgres-mssql-selected-relation-schema-authority-1",
        SOURCE_DOCUMENT,
        selected,
        "postgres-16-live-user-columns-only-relation-v1",
        "r",
        "p",
        False,
        tuple(columns),
        policy,
    )


def _bindings() -> tuple[MssqlR1PrincipalBindingV3, ...]:
    return tuple(
        sorted(
            (
                MssqlR1PrincipalBindingV3(
                    MssqlR1SubjectRoleV3.LOADER,
                    "loader_user",
                    digest(20),
                    MssqlR1PrincipalTypeV3.SQL_USER,
                    MssqlR1AuthenticationTypeV3.DATABASE,
                    None,
                    (),
                ),
                MssqlR1PrincipalBindingV3(
                    MssqlR1SubjectRoleV3.OBSERVER,
                    "observer_user",
                    digest(21),
                    MssqlR1PrincipalTypeV3.EXTERNAL_USER,
                    MssqlR1AuthenticationTypeV3.EXTERNAL,
                    None,
                    (),
                ),
                MssqlR1PrincipalBindingV3(
                    MssqlR1SubjectRoleV3.PROVISIONER,
                    "provisioner_user",
                    digest(22),
                    MssqlR1PrincipalTypeV3.WINDOWS_USER,
                    MssqlR1AuthenticationTypeV3.WINDOWS,
                    digest(32),
                    (),
                ),
                MssqlR1PrincipalBindingV3(
                    MssqlR1SubjectRoleV3.RUNTIME,
                    "runtime_user",
                    digest(23),
                    MssqlR1PrincipalTypeV3.SQL_USER,
                    MssqlR1AuthenticationTypeV3.INSTANCE,
                    digest(33),
                    (),
                ),
            ),
            key=lambda item: item.canonical_bytes,
        )
    )


def _principals(stage_user: str = "stage_user") -> tuple[MssqlR1ObservedPrincipalV3, ...]:
    observed = [
        MssqlR1ObservedPrincipalV3(
            MssqlR1ObservedPrincipalAuthorityKindV3.ENVIRONMENT,
            binding.subject_role,
            binding.database_principal_name,
            10 + index,
            binding.database_principal_sid_digest,
            binding.server_principal_sid_digest,
            binding.principal_type,
            binding.authentication_type,
            None,
        )
        for index, binding in enumerate(_bindings())
    ]
    observed.extend(
        (
            MssqlR1ObservedPrincipalV3(
                MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER,
                MssqlR1SubjectRoleV3.ATTESTATION_MODULE,
                "attestor_user",
                20,
                digest(24),
                None,
                MssqlR1PrincipalTypeV3.CERTIFICATE,
                MssqlR1AuthenticationTypeV3.NONE,
                None,
            ),
            MssqlR1ObservedPrincipalV3(
                MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER,
                MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE,
                stage_user,
                21,
                digest(25),
                None,
                MssqlR1PrincipalTypeV3.CERTIFICATE,
                MssqlR1AuthenticationTypeV3.NONE,
                None,
            ),
        )
    )
    return tuple(sorted(observed, key=lambda item: item.canonical_bytes))


def _observed_objects(contract: Any, principals: tuple[MssqlR1ObservedPrincipalV3, ...]):
    by_role = {item.subject_role: item for item in principals if item.subject_role is not None}
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    schema_ids = {"dpone_authority": 100, "dpone_stage": 101}
    objects = []
    next_id = 200
    cert_id = {
        MssqlR1SignerProfileKindV3.ATTESTOR: 300,
        MssqlR1SignerProfileKindV3.STAGE_OWNER: 301,
    }
    thumbs = {
        MssqlR1SignerProfileKindV3.ATTESTOR: digest(50),
        MssqlR1SignerProfileKindV3.STAGE_OWNER: digest(51),
    }
    profiles = {item.signer_profile: item for item in contract.ordered_signer_profiles}
    for portable in contract.ordered_objects:
        schema_id = schema_ids[portable.schema_name]
        object_id = next_id
        next_id += 1
        triggers = []
        for trigger in portable.ordered_triggers:
            triggers.append(MssqlR1ObservedTriggerIdentityV3(trigger.digest, schema_id, next_id, object_id))
            next_id += 1
        triggers_t = tuple(sorted(triggers, key=lambda item: (item.portable_trigger_digest, item.object_id)))
        signatures: tuple[MssqlR1ModuleSignatureObservationV3, ...] = ()
        profile = (
            MssqlR1SignerProfileKindV3.NONE
            if portable.kind is MssqlR1SchemaObjectKindV3.TABLE
            else portable.module_options.signer_profile
        )
        if profile is not MssqlR1SignerProfileKindV3.NONE:
            signer = by_role[MODULE_ROLE_BY_PROFILE[profile]]
            signatures = (
                MssqlR1ModuleSignatureObservationV3(
                    profile,
                    object_id,
                    profiles[profile].certificate_name,
                    cert_id[profile],
                    thumbs[profile],
                    signer.principal_id,
                    signer.database_sid_digest,
                    digest(70 + cert_id[profile]),
                    False,
                ),
            )
        objects.append(
            MssqlR1ObservedSchemaObjectV3(
                portable,
                schema_id,
                object_id,
                None,
                provisioner.principal_id,
                provisioner.database_sid_digest,
                triggers_t,
                signatures,
            )
        )
    return tuple(objects)


def _observed_permissions(contract: Any, principals: tuple[MssqlR1ObservedPrincipalV3, ...]):
    by_role = {item.subject_role: item for item in principals if item.subject_role is not None}
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    items = []
    for rule in contract.ordered_permission_rules:
        grantee = by_role[rule.subject_role]
        items.append(
            MssqlR1ObservedPermissionV3(
                MssqlR1PermissionSourceV3.DIRECT,
                None,
                grantee.principal_id,
                grantee.database_sid_digest,
                provisioner.principal_id,
                provisioner.database_sid_digest,
                rule.scope,
                rule.schema_name,
                rule.object_name,
                rule.column_name,
                rule.permission,
                rule.effect,
                rule.grant_option,
            )
        )
    return tuple(sorted(items, key=lambda item: item.canonical_bytes))


def _expected_paths(descriptor: Any, pack: MssqlR1BindingModulePackV2):
    merged = sorted(
        (
            *(
                project_schema_permission_rule(rule)
                for rule in descriptor.expected_schema_contract.ordered_permission_rules
            ),
            *pack.portable_identity.permission_contract.ordered_permission_paths,
        ),
        key=lambda item: item.canonical_bytes,
    )
    seen: set[bytes] = set()
    unique = []
    for item in merged:
        digest_bytes = item.canonical_bytes
        if digest_bytes not in seen:
            seen.add(digest_bytes)
            unique.append(item)
    return tuple(unique)


@lru_cache(maxsize=1)
def golden_catalog_inputs() -> dict[str, Any]:
    """Build one closed descriptor/Security/Binding/schema input set for catalog create."""

    physical, security = authorities()[3], authorities()[4]
    catalog = replace(
        registered_target_catalog(),
        ordered_columns=(_bigint_column(1, "column_1"), _bigint_column(2, "column_2")),
    )
    source = _source_authority()
    payload = registration(catalog)
    stable = replace(
        MssqlR1RotationStableTargetAuthorityV1.from_registration(payload),
        route_source_authority_sha256=source.selected_source_authority_sha256,
    )
    pack = MssqlR1BindingModulePackFactoryV2().create(
        physical_descriptor=physical,
        shared_security_profile=security,
        binding_signer_lifecycle_policy=_binding_policy(security),
        source_schema_authority=source,
        stable_target_authority=stable,
        target_catalog=catalog,
    )
    principals = _principals("stage_user")
    by_role = {item.subject_role: item for item in principals if item.subject_role is not None}
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    contract = physical.expected_schema_contract
    schemas = (
        MssqlR1ObservedSchemaV3(
            "dpone_authority", 100, provisioner.principal_id, provisioner.principal_id, provisioner.database_sid_digest
        ),
        MssqlR1ObservedSchemaV3(
            "dpone_stage", 101, provisioner.principal_id, provisioner.principal_id, provisioner.database_sid_digest
        ),
    )
    objects = _observed_objects(contract, principals)
    verification = _verification(payload)
    authority = MssqlR1PrincipalAuthoritySetV3.create(verification, _bindings())
    source_attestation = MssqlR1SchemaAttestationV3.create(
        registration_verification=verification,
        expected_contract=contract,
        principal_authority_set=authority,
        observed_server_instance_identity_sha256=payload.server_instance_identity_sha256,
        database_id=7,
        observed_database_guid=payload.database_guid,
        observed_database_family_guid=payload.database_family_guid,
        observed_recovery_fork_guid=payload.recovery_fork_guid,
        ordered_observed_schemas=schemas,
        ordered_observed_objects=objects,
        ordered_observed_principals=principals,
        ordered_observed_role_memberships=(),
        ordered_observed_permissions=_observed_permissions(contract, principals),
        projection_revision=1,
        observed_at=NOW,
    )
    identity = observed_identity(
        server=payload.server_instance_identity_sha256,
        database_id=7,
        database_guid=payload.database_guid,
        family=payload.database_family_guid,
        fork=payload.recovery_fork_guid,
        name="warehouse",
    )
    tables = tuple(item for item in objects if item.portable_object.kind is MssqlR1SchemaObjectKindV3.TABLE)
    modules = tuple(item for item in objects if item.portable_object.kind is MssqlR1SchemaObjectKindV3.PROCEDURE)
    pack_modules = tuple(
        MssqlR1ObservedBindingModuleV1(
            item.module_kind,
            item.schema_name,
            item.object_name,
            index,
            b"CREATE PROCEDURE dbo.x AS\n",
            module_options(),
        )
        for index, item in enumerate(pack.portable_identity.ordered_modules, 1)
    )
    pack_signatures = tuple(
        MssqlR1ObservedBindingSignatureV1(digest_value, index, 90 + index, b"crypt")
        for index, digest_value in enumerate(
            pack.portable_identity.expected_inventory.ordered_signature_intent_digests, 1
        )
    )
    signer_identity = pack.portable_identity.signer_identity
    signer = MssqlR1ObservedBindingSignerV2(
        signer_identity.target_binding_uuid,
        certificate_observation(
            signer_identity.certificate_name,
            signer_identity.certificate_subject,
            signer_identity.certificate_user_name,
            b"thumbprint-binding-00000000000001",
            b"sid-binding-00000000000000000001",
        ),
        400,
        10,
        11,
    )
    shared = tuple(
        certificate_observation(
            profile.certificate_name,
            f"dpone R1 V3 {profile.signer_profile.value.replace('_', ' ')}",
            profile.certificate_user_name,
            f"thumbprint-{profile.signer_profile.value}".ljust(32, "0").encode(),
            f"sid-{profile.signer_profile.value}".ljust(32, "0").encode(),
        )
        for profile in contract.ordered_signer_profiles
    )
    qs = MssqlR1SchemaQueryResultV1(identity, schemas, principals, ())
    qt = MssqlR1TableQueryResultV1(tables)
    qm = MssqlR1ModuleQueryResultV1(modules, pack_modules)
    qc = MssqlR1CertificateQueryResultV2(shared, signer)
    qp = MssqlR1PermissionQueryResultV2(_expected_paths(physical, pack))
    flatten = tuple(signature for item in objects for signature in item.ordered_signatures)
    qg = MssqlR1SignatureQueryResultV1(flatten, pack_signatures)
    qb = MssqlR1BindingPrefixInventoryQueryResultV1(())
    typed = (qs, qt, qm, qc, qp, qg, qb)
    registry = statement_registry(pack_modules)
    results = tuple(
        MssqlR1PostInstallStatementResultV2(
            statement.statement_ref,
            statement.attestation_kind,
            statement.result_authority_kind,
            statement.query_definition_digest(),
            typed[index],
        )
        for index, statement in enumerate(registry.ordered_statements)
    )
    return {
        "physical": physical,
        "security": security,
        "pack": pack,
        "source_attestation": source_attestation,
        "identity": identity,
        "target_ref": target_ref_for(identity),
        "profile": build_profile(),
        "registry": registry,
        "results": results,
        "qs": qs,
        "qt": qt,
        "qm": qm,
        "qg": qg,
        "objects": objects,
        "ENVIRONMENT_ROLES": ENVIRONMENT_ROLES,
        "QUERY_KINDS": (
            MssqlR1AttestationQueryKindV1,
            MssqlR1AttestationResultAuthorityKindV1,
        ),
    }
