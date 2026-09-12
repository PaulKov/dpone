"""Case oracles for Provider Attestation V2. Production imports stay inside handlers."""

from __future__ import annotations

from collections.abc import Callable

Outcome = tuple[str, str | None]


def _accept(value: object) -> Outcome:
    if value is None:
        raise AssertionError("owned symbol produced no value")
    return ("accept", None)


def _reject(call: Callable[[], object]) -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_errors import MssqlR1ProviderAttestationError

    try:
        call()
    except MssqlR1ProviderAttestationError as error:
        return ("reject", error.failure.reason.value)
    raise AssertionError("typed attestation rejection required")


def execute_case(case_id: str) -> Outcome:
    return _HANDLERS[case_id]()


def _abi_statement_ref_roundtrip() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_ref

    ref = statement_ref()
    cloned = type(ref).from_canonical_bytes(ref.canonical_bytes)
    assert cloned == ref and cloned.canonical_bytes == ref.canonical_bytes
    return _accept(ref)


def _abi_statement_ref_wrong_domain() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_identity import MssqlR1MigrationStatementRefV1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_ref

    ref = statement_ref()
    return _reject(
        lambda: MssqlR1MigrationStatementRefV1.from_canonical_bytes(
            b"dpone-unknown-domain\0" + ref.canonical_bytes[21:]
        )
    )


def _abi_statement_ref_empty() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_identity import MssqlR1MigrationStatementRefV1

    return _reject(lambda: MssqlR1MigrationStatementRefV1.from_canonical_bytes(b""))


def _abi_failure_roundtrip() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
        MssqlR1ProviderAttestationFailureReasonV2,
        MssqlR1ProviderAttestationRecoveryActionV2,
    )
    from dpone.contracts.mssql_r1_v3_provider_attestation_errors import MssqlR1ProviderAttestationFailureV2

    failure = MssqlR1ProviderAttestationFailureV2(
        MssqlR1ProviderAttestationFailureReasonV2.ATTESTATION_AUTHORITY_SPLICE,
        "authority",
        MssqlR1ProviderAttestationRecoveryActionV2.REGENERATE_ATTESTATION_INPUT,
        "Attestation authorities do not belong to one provider generation.",
    )
    assert type(failure).from_canonical_bytes(failure.canonical_bytes) == failure
    return _accept(failure)


def _abi_error_carrier() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
        MssqlR1ProviderAttestationFailureReasonV2,
        MssqlR1ProviderAttestationRecoveryActionV2,
    )
    from dpone.contracts.mssql_r1_v3_provider_attestation_errors import (
        MssqlR1ProviderAttestationError,
        MssqlR1ProviderAttestationFailureV2,
    )

    failure = MssqlR1ProviderAttestationFailureV2(
        MssqlR1ProviderAttestationFailureReasonV2.ATTESTATION_AUTHORITY_SPLICE,
        "authority",
        MssqlR1ProviderAttestationRecoveryActionV2.REGENERATE_ATTESTATION_INPUT,
        "Attestation authorities do not belong to one provider generation.",
    )
    error = MssqlR1ProviderAttestationError(failure)
    assert error.failure is failure and str(error) == failure.redacted_message
    return _accept(error)


def _abi_result_kind_closed() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import MssqlR1AttestationResultAuthorityKindV1

    assert MssqlR1AttestationResultAuthorityKindV1.SCHEMA_INVENTORY.value == "schema_inventory"
    return _accept(MssqlR1AttestationResultAuthorityKindV1)


def _abi_query_kind_pairs() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
        QUERY_RESULT_PAIRS,
        MssqlR1AttestationQueryKindV1,
        MssqlR1AttestationResultAuthorityKindV1,
    )

    assert tuple(MssqlR1AttestationQueryKindV1) == tuple(item[0] for item in QUERY_RESULT_PAIRS)
    assert tuple(item.value for item in MssqlR1AttestationResultAuthorityKindV1) == tuple(
        item[1].value for item in QUERY_RESULT_PAIRS
    )
    return _accept(QUERY_RESULT_PAIRS)


def _abi_phase_closed() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import MssqlR1MigrationStatementPhaseV1

    assert MssqlR1MigrationStatementPhaseV1.POST_DECISION_ATTEST.value == "post_decision_attest"
    return _accept(MssqlR1MigrationStatementPhaseV1)


def _abi_reason_closed() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import MssqlR1ProviderAttestationFailureReasonV2

    assert MssqlR1ProviderAttestationFailureReasonV2.ATTESTATION_BOUNDS_EXCEEDED.value == "attestation_bounds_exceeded"
    return _accept(MssqlR1ProviderAttestationFailureReasonV2)


def _abi_recovery_closed() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import MssqlR1ProviderAttestationRecoveryActionV2

    assert (
        MssqlR1ProviderAttestationRecoveryActionV2.REGENERATE_ATTESTATION_INPUT.value == "regenerate_attestation_input"
    )
    return _accept(MssqlR1ProviderAttestationRecoveryActionV2)


def _abi_profile_roundtrip() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import build_profile

    profile = build_profile()
    assert type(profile).from_canonical_bytes(profile.canonical_bytes) == profile
    return _accept(profile)


def _abi_target_ref_roundtrip() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import observed_identity, target_ref_for

    identity = observed_identity()
    target = target_ref_for(identity)
    assert identity.matches_target_ref(target)
    assert type(target).from_canonical_bytes(target.canonical_bytes) == target
    return _accept(target)


def _abi_observed_database_roundtrip() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import observed_identity

    identity = observed_identity()
    assert type(identity).from_canonical_bytes(identity.canonical_bytes) == identity
    return _accept(identity)


def _query_schema_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1SchemaQueryResultV1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import observed_identity, schema_parts

    _, _, _, schemas, _, principals, memberships, _ = schema_parts()
    result = MssqlR1SchemaQueryResultV1(
        observed_identity(),
        tuple(sorted(schemas, key=lambda item: item.schema_name.encode())),
        tuple(sorted(principals, key=lambda item: item.canonical_bytes)),
        tuple(sorted(memberships, key=lambda item: item.canonical_bytes)),
    )
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_table_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1TableQueryResultV1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import schema_parts

    _, _, _, _, objects, _, _, _ = schema_parts()
    tables = tuple(item for item in objects if item.portable_object.kind.value == "table")
    result = MssqlR1TableQueryResultV1(tables)
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_module_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1ModuleQueryResultV1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import binding_modules, schema_parts

    _, _, _, _, objects, _, _, _ = schema_parts()
    modules = tuple(item for item in objects if item.portable_object.kind.value == "procedure")
    result = MssqlR1ModuleQueryResultV1(modules, binding_modules())
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_signature_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1SignatureQueryResultV1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import binding_signatures

    result = MssqlR1SignatureQueryResultV1((), binding_signatures())
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_permission_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1PermissionQueryResultV2

    result = MssqlR1PermissionQueryResultV2(())
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_prefix_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (
        MssqlR1BindingPrefixInventoryQueryResultV1,
    )
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import prefix_object

    result = MssqlR1BindingPrefixInventoryQueryResultV1((prefix_object(),))
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_binding_module() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import binding_modules

    module = binding_modules()[0]
    assert type(module).from_canonical_bytes(module.canonical_bytes) == module
    return _accept(module)


def _query_binding_signer() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import binding_signer

    signer = binding_signer()
    assert type(signer).from_canonical_bytes(signer.canonical_bytes) == signer
    return _accept(signer)


def _query_module_definition() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_registry

    definition = statement_registry().ordered_statements[2].ordered_module_definitions[0]
    assert type(definition).from_canonical_bytes(definition.canonical_bytes) == definition
    return _accept(definition)


def _query_statement_authority() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_registry

    statement = statement_registry().ordered_statements[0]
    assert type(statement).from_canonical_bytes(statement.canonical_bytes) == statement
    return _accept(statement)


def _query_binding_signature() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import binding_signatures

    signature = binding_signatures()[0]
    assert type(signature).from_canonical_bytes(signature.canonical_bytes) == signature
    return _accept(signature)


def _query_certificate_result() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1CertificateQueryResultV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import (
        binding_signer,
        certificate_observation,
    )

    result = MssqlR1CertificateQueryResultV2(
        (
            certificate_observation(),
            certificate_observation(
                "stage_cert",
                "dpone R1 V3 stage owner",
                "stage_user",
                b"thumbprint-stage-owner-00000000001",
                b"sid-stage-owner-0000000000000001",
            ),
        ),
        binding_signer(),
    )
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_post_install() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
        MssqlR1AttestationQueryKindV1,
        MssqlR1AttestationResultAuthorityKindV1,
    )
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (
        MssqlR1PostInstallStatementResultV2,
        MssqlR1SchemaQueryResultV1,
    )
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import observed_identity, statement_ref

    result = MssqlR1PostInstallStatementResultV2(
        statement_ref("schema"),
        MssqlR1AttestationQueryKindV1.SCHEMA,
        MssqlR1AttestationResultAuthorityKindV1.SCHEMA_INVENTORY,
        __import__("hashlib").sha256(b"SELECT 1;\n").digest(),
        MssqlR1SchemaQueryResultV1(observed_identity(), (), (), ()),
    )
    assert type(result).from_canonical_bytes(result.canonical_bytes) == result
    return _accept(result)


def _query_wrong_arm() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
        MssqlR1AttestationQueryKindV1,
        MssqlR1AttestationResultAuthorityKindV1,
    )
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (
        MssqlR1PostInstallStatementResultV2,
        MssqlR1SchemaQueryResultV1,
    )
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import observed_identity, statement_ref

    return _reject(
        lambda: MssqlR1PostInstallStatementResultV2(
            statement_ref("schema"),
            MssqlR1AttestationQueryKindV1.SCHEMA,
            MssqlR1AttestationResultAuthorityKindV1.TABLE_INVENTORY,
            __import__("hashlib").sha256(b"SELECT 1;\n").digest(),
            MssqlR1SchemaQueryResultV1(observed_identity(), (), (), ()),
        )
    )


def _schema_merge_accept() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import stable_merge_provider_schema_v2
    from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SchemaObjectKindV3
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import schema_parts

    _, _, _, _, objects, _, _, _ = schema_parts()
    tables = tuple(item for item in objects if item.portable_object.kind is MssqlR1SchemaObjectKindV3.TABLE)
    modules = tuple(item for item in objects if item.portable_object.kind is MssqlR1SchemaObjectKindV3.PROCEDURE)
    merged = stable_merge_provider_schema_v2(tables, modules, objects)
    assert {item.canonical_bytes for item in merged} == {item.canonical_bytes for item in objects}
    return _accept(merged)


def _schema_merge_kind_overlap() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import stable_merge_provider_schema_v2
    from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SchemaObjectKindV3
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import schema_parts

    _, _, _, _, objects, _, _, _ = schema_parts()
    tables = tuple(item for item in objects if item.portable_object.kind is MssqlR1SchemaObjectKindV3.TABLE)
    return _reject(lambda: stable_merge_provider_schema_v2(tables, tables, objects))


def _schema_attestation_roundtrip() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import (
        MssqlR1StableSchemaAttestationV2,
        derive_stable_schema_digests,
    )
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import schema_attestation_fixture

    source = schema_attestation_fixture()
    derived = derive_stable_schema_digests(
        expected_contract_bytes=source.expected_contract_bytes,
        principal_authority_set_bytes=source.principal_authority_set_bytes,
        ordered_observed_schemas=source.ordered_observed_schemas,
        ordered_observed_objects=source.ordered_observed_objects,
        ordered_observed_principals=source.ordered_observed_principals,
        ordered_observed_role_memberships=source.ordered_observed_role_memberships,
        server_instance_identity_sha256=source.server_instance_identity_sha256,
        database_id=source.database_id,
        database_guid=source.database_guid,
        database_family_guid=source.database_family_guid,
        recovery_fork_guid=source.recovery_fork_guid,
    )
    stable = MssqlR1StableSchemaAttestationV2(
        source.schema_contract_version,
        source.target_binding_uuid,
        source.registration_payload_digest,
        source.registration_verification_receipt_digest,
        source.registered_resolved_profile_digest,
        source.registered_physical_authority_digest,
        source.server_instance_identity_sha256,
        source.database_id,
        source.database_guid,
        source.database_family_guid,
        source.recovery_fork_guid,
        source.expected_contract_bytes,
        source.principal_authority_set_bytes,
        source.ordered_observed_schemas,
        source.ordered_observed_objects,
        source.ordered_observed_principals,
        source.ordered_observed_role_memberships,
        *derived,
        1,
    )
    assert type(stable).from_canonical_bytes(stable.canonical_bytes) == stable
    return _accept(stable)


def _schema_create_accept() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import MssqlR1StableSchemaAttestationV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import golden_catalog_inputs

    inputs = golden_catalog_inputs()
    stable = MssqlR1StableSchemaAttestationV2.create(
        source_schema_attestation=inputs["source_attestation"],
        schema_query_result=inputs["qs"],
        table_query_result=inputs["qt"],
        module_query_result=inputs["qm"],
        signature_query_result=inputs["qg"],
        active_physical_descriptor=inputs["physical"],
    )
    assert stable.projection_revision == 1
    return _accept(stable)


def _schema_create_identity_mismatch() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import MssqlR1SchemaQueryResultV1
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import MssqlR1StableSchemaAttestationV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import (
        golden_catalog_inputs,
        observed_identity,
    )

    inputs = golden_catalog_inputs()
    qs = MssqlR1SchemaQueryResultV1(
        observed_identity(),
        inputs["qs"].ordered_schemas,
        inputs["qs"].ordered_principals,
        inputs["qs"].ordered_role_memberships,
    )
    return _reject(
        lambda: MssqlR1StableSchemaAttestationV2.create(
            source_schema_attestation=inputs["source_attestation"],
            schema_query_result=qs,
            table_query_result=inputs["qt"],
            module_query_result=inputs["qm"],
            signature_query_result=inputs["qg"],
            active_physical_descriptor=inputs["physical"],
        )
    )


def _catalog_closure_empty() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import MssqlR1PermissionClosureResultV2

    closure = MssqlR1PermissionClosureResultV2(
        __import__("hashlib").sha256(b"observation-policy").digest(), (), (), (), ()
    )
    assert type(closure).from_canonical_bytes(closure.canonical_bytes) == closure
    return _accept(closure)


def _catalog_registry_seven() -> Outcome:
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_registry

    registry = statement_registry()
    cloned = type(registry).from_canonical_bytes(registry.canonical_bytes)
    assert cloned == registry
    return _accept(registry)


def _catalog_live_identity() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import MssqlR1BindingLiveCatalogIdentityV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import (
        binding_modules,
        binding_signatures,
        binding_signer,
        observed_identity,
    )

    live = MssqlR1BindingLiveCatalogIdentityV2(
        observed_identity(),
        binding_modules(),
        binding_signer(),
        binding_signatures(),
        (),
    )
    assert type(live).from_canonical_bytes(live.canonical_bytes) == live
    return _accept(live)


def _catalog_shared_security() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import MssqlR1StableSharedSecurityAttestationV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import certificate_observation

    shared = MssqlR1StableSharedSecurityAttestationV2(
        (
            certificate_observation(),
            certificate_observation(
                "stage_cert",
                "dpone R1 V3 stage owner",
                "stage_user",
                b"thumbprint-stage-owner-00000000001",
                b"sid-stage-owner-0000000000000001",
            ),
        ),
        (),
    )
    assert type(shared).from_canonical_bytes(shared.canonical_bytes) == shared
    return _accept(shared)


def _catalog_binding() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import MssqlR1StableBindingAttestationV2
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import MssqlR1BindingLiveCatalogIdentityV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import (
        DIGEST,
        binding_modules,
        binding_signatures,
        binding_signer,
        observed_identity,
    )

    binding = MssqlR1StableBindingAttestationV2(
        DIGEST,
        MssqlR1BindingLiveCatalogIdentityV2(
            observed_identity(),
            binding_modules(),
            binding_signer(),
            binding_signatures(),
            (),
        ),
    )
    assert type(binding).from_canonical_bytes(binding.canonical_bytes) == binding
    return _accept(binding)


def _catalog_create_accept() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import MssqlR1StableCatalogAttestationV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import golden_catalog_inputs

    inputs = golden_catalog_inputs()
    catalog = MssqlR1StableCatalogAttestationV2.create(
        expected_target_ref=inputs["target_ref"],
        provider_contract_digest=__import__("hashlib").sha256(b"provider-contract").digest(),
        physical_schema_descriptor=inputs["physical"],
        security_profile=inputs["security"],
        binding_pack=inputs["pack"],
        expected_build_profile=inputs["profile"],
        statement_registry=inputs["registry"],
        ordered_statement_results=inputs["results"],
        source_schema_attestation=inputs["source_attestation"],
    )
    assert catalog.attestation_version == "dpone-r1-stable-catalog-attestation-2"
    return _accept(catalog)


def _catalog_create_splice() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_identity import MssqlR1MigrationTargetRefV1
    from dpone.contracts.mssql_r1_v3_provider_attestation_stable_catalog import MssqlR1StableCatalogAttestationV2
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import DIGEST, golden_catalog_inputs

    inputs = golden_catalog_inputs()
    return _reject(
        lambda: MssqlR1StableCatalogAttestationV2.create(
            expected_target_ref=MssqlR1MigrationTargetRefV1("dpone-mssql-target-physical-identity-1", DIGEST),
            provider_contract_digest=__import__("hashlib").sha256(b"provider-contract").digest(),
            physical_schema_descriptor=inputs["physical"],
            security_profile=inputs["security"],
            binding_pack=inputs["pack"],
            expected_build_profile=inputs["profile"],
            statement_registry=inputs["registry"],
            ordered_statement_results=inputs["results"],
            source_schema_attestation=inputs["source_attestation"],
        )
    )


def _mutation_nested_q() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_validation import preflight_provider_attestation_canonical_v2

    domain = b"dpone-r1-provider-migration-statement-ref-v1\0"
    inner = b"q"
    for _ in range(1500):
        inner = b"q" + len(inner).to_bytes(4, "big") + inner
    payload = bytearray(domain)
    for _ in range(3):
        payload.extend(len(inner).to_bytes(4, "big"))
        payload.extend(inner)
    return _reject(lambda: preflight_provider_attestation_canonical_v2(bytes(payload), domain, 3, 262144))


def _mutation_cap_malformed() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_identity import MssqlR1MigrationStatementRefV1

    return _reject(lambda: MssqlR1MigrationStatementRefV1.from_canonical_bytes(b"x" * 262144))


def _mutation_size_emit() -> Outcome:
    from dpone.contracts.mssql_r1_v3_provider_attestation_validation import canonical_encoded_size_v1
    from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_fixtures import statement_ref

    ref = statement_ref()
    domain = b"dpone-r1-provider-migration-statement-ref-v1\0"
    assert canonical_encoded_size_v1(domain, (ref.statement_id, ref.statement_spec_digest, ref.phase)) == len(
        ref.canonical_bytes
    )
    return _accept(canonical_encoded_size_v1)


_HANDLERS: dict[str, Callable[[], Outcome]] = {
    "abi.error.carrier": _abi_error_carrier,
    "abi.failure.roundtrip": _abi_failure_roundtrip,
    "abi.observed.database.roundtrip": _abi_observed_database_roundtrip,
    "abi.phase.closed": _abi_phase_closed,
    "abi.profile.roundtrip": _abi_profile_roundtrip,
    "abi.query.kind.pairs": _abi_query_kind_pairs,
    "abi.result.kind.closed": _abi_result_kind_closed,
    "abi.reason.closed": _abi_reason_closed,
    "abi.recovery.closed": _abi_recovery_closed,
    "abi.statement.ref.empty": _abi_statement_ref_empty,
    "abi.statement.ref.roundtrip": _abi_statement_ref_roundtrip,
    "abi.statement.ref.wrong.domain": _abi_statement_ref_wrong_domain,
    "abi.target.ref.roundtrip": _abi_target_ref_roundtrip,
    "catalog.binding.roundtrip": _catalog_binding,
    "catalog.closure.empty": _catalog_closure_empty,
    "catalog.create.accept": _catalog_create_accept,
    "catalog.create.splice": _catalog_create_splice,
    "catalog.live.identity": _catalog_live_identity,
    "catalog.registry.seven": _catalog_registry_seven,
    "catalog.shared.security": _catalog_shared_security,
    "mutation.cap.malformed": _mutation_cap_malformed,
    "mutation.nested.q": _mutation_nested_q,
    "mutation.size.emit": _mutation_size_emit,
    "query.binding.module": _query_binding_module,
    "query.binding.prefix": _query_prefix_result,
    "query.binding.signature": _query_binding_signature,
    "query.binding.signer": _query_binding_signer,
    "query.module.definition": _query_module_definition,
    "query.certificate.result": _query_certificate_result,
    "query.module.result": _query_module_result,
    "query.permission.result": _query_permission_result,
    "query.post.install": _query_post_install,
    "query.schema.result": _query_schema_result,
    "query.signature.result": _query_signature_result,
    "query.statement.authority": _query_statement_authority,
    "query.table.result": _query_table_result,
    "query.wrong.arm": _query_wrong_arm,
    "schema.attestation.roundtrip": _schema_attestation_roundtrip,
    "schema.create.accept": _schema_create_accept,
    "schema.create.identity.mismatch": _schema_create_identity_mismatch,
    "schema.merge.accept": _schema_merge_accept,
    "schema.merge.kind.overlap": _schema_merge_kind_overlap,
}
