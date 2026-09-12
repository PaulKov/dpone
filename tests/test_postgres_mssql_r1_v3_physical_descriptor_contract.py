from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import fields, replace
from typing import Literal

import pytest

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_enum,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_coordinates import (
    MssqlR1ComparisonCoordinateV1,
    MssqlR1ComparisonLiteralV1,
    MssqlR1ResourceExistenceOperandV1,
    MssqlR1ResourceInstanceSelectorV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_definitions import MssqlR1DefinitionPayloadV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
    MssqlR1BindingModuleKindV1,
    MssqlR1BindingSignerKindV1,
    MssqlR1ComparisonOperatorV1,
    MssqlR1ComparisonSourceV1,
    MssqlR1DefinitionKindV1,
    MssqlR1ExecutionPathV1,
    MssqlR1FreshProbeKindV1,
    MssqlR1MigrationDispositionV1,
    MssqlR1MigrationObservationKindV1,
    MssqlR1MutationPolicyV1,
    MssqlR1OutcomeClassV1,
    MssqlR1PrincipalKindV1,
    MssqlR1ProjectionRoleV1,
    MssqlR1RedactionClassV1,
    MssqlR1ReplayBooleanOperatorV1,
    MssqlR1ReplayComparatorV1,
    MssqlR1RequestBindingKindV1,
    MssqlR1ResourceInstanceSelectorKindV1,
    MssqlR1RetryClassV1,
    MssqlR1RevisionRuleKindV1,
    MssqlR1TableLifecycleV1,
    MssqlR1TransitionAuthorityV1,
    MssqlR1TransitionCardinalityV1,
    MssqlR1TransitionKindV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import (
    MssqlR1MigrationProbeV1,
    MssqlR1PhysicalErrorConditionV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_execution import MssqlR1ExecutionSemanticsV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_locks import (
    MssqlR1LockActionV1,
    MssqlR1LockMechanismV1,
    MssqlR1LockModeV1,
    MssqlR1LockOwnerV1,
    MssqlR1LockTimeoutPolicyV1,
    MssqlR1PhysicalLockStepV1,
    require_lock_order,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_procedures import (
    MssqlR1BindingModuleTemplateV1,
    MssqlR1PhysicalProcedureDescriptorV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_profiles import (
    MssqlR1PhysicalEngineProfileV1,
    MssqlR1PhysicalSessionProfileV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_relations import MssqlR1PhysicalTableDescriptorV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_replay import (
    MssqlR1ComparisonPairV1,
    MssqlR1ReplayClauseGroupV1,
    MssqlR1ReplayClauseV1,
    MssqlR1ReplayOutcomePolicyV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_requests import (
    MssqlR1OutcomeVariantV1,
    MssqlR1ProjectionFieldV1,
    MssqlR1ProjectionGrammarV1,
    MssqlR1RequestAuthorityV1,
    MssqlR1ScalarParameterBindingV1,
    validate_request_authority,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    ACCESS_MATRIX,
    MssqlR1AccessKindV1,
    MssqlR1LockCardinalityV1,
    MssqlR1LockKindV1,
    MssqlR1PhysicalResourceAccessV1,
    MssqlR1PhysicalResourceDeclarationV1,
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ResourceFieldV1,
    MssqlR1ResourceKindV1,
    MssqlR1ValueCardinalityV1,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_transitions import (
    MssqlR1RevisionRuleV1,
    MssqlR1StateChangeV1,
    MssqlR1StateTransitionV1,
    MssqlR1TransitionApplicabilityV1,
)
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import (
    DOMAIN,
    PROCEDURE_NAMES,
    TABLE_NAMES,
    VERSION,
    MssqlR1PhysicalSchemaDescriptorV1,
)
from dpone.contracts.mssql_r1_v3_schema_attestation import MssqlR1SchemaContractV3
from dpone.contracts.mssql_r1_v3_schema_modules import (
    MssqlR1FixedResultV3,
    MssqlR1ModuleOptionsV3,
    MssqlR1PortableSchemaObjectV3,
    MssqlR1PortableTriggerV3,
    MssqlR1StageScanTemplateV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1ConstraintKindV3,
    MssqlR1ExtendedPropertyV3,
    MssqlR1IndexDirectionV3,
    MssqlR1ParameterDirectionV3,
    MssqlR1ResultCardinalityV3,
    MssqlR1ResultColumnV3,
    MssqlR1SchemaColumnV3,
    MssqlR1SchemaObjectKindV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SignerProfileKindV3,
    MssqlR1SupportedCodecEntryV3,
)
from dpone.contracts.mssql_r1_v3_schema_relations import (
    MssqlR1SchemaConstraintV3,
    MssqlR1SchemaIndexKeyV3,
    MssqlR1SchemaIndexV3,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionRuleV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1SignerProfileV3,
    MssqlR1SubjectRoleV3,
)

ENUM_BRANCH_REGISTRY = (
    MssqlR1DefinitionKindV1,
    MssqlR1TableLifecycleV1,
    MssqlR1MutationPolicyV1,
    MssqlR1ResourceKindV1,
    MssqlR1AccessKindV1,
    MssqlR1LockKindV1,
    MssqlR1LockMechanismV1,
    MssqlR1LockActionV1,
    MssqlR1LockModeV1,
    MssqlR1LockOwnerV1,
    MssqlR1LockCardinalityV1,
    MssqlR1LockTimeoutPolicyV1,
    MssqlR1ResourceInstanceSelectorKindV1,
    MssqlR1TransitionKindV1,
    MssqlR1TransitionAuthorityV1,
    MssqlR1TransitionCardinalityV1,
    MssqlR1RevisionRuleKindV1,
    MssqlR1ReplayComparatorV1,
    MssqlR1ReplayBooleanOperatorV1,
    MssqlR1ComparisonOperatorV1,
    MssqlR1ComparisonSourceV1,
    MssqlR1PrincipalKindV1,
    MssqlR1ProjectionScalarKindV1,
    MssqlR1ValueCardinalityV1,
    MssqlR1ProjectionRoleV1,
    MssqlR1RequestBindingKindV1,
    MssqlR1ExecutionPathV1,
    MssqlR1OutcomeClassV1,
    MssqlR1RetryClassV1,
    MssqlR1FreshProbeKindV1,
    MssqlR1RedactionClassV1,
    MssqlR1MigrationObservationKindV1,
    MssqlR1MigrationDispositionV1,
    MssqlR1BindingModuleKindV1,
    MssqlR1BindingSignerKindV1,
)

DIGEST = hashlib.sha256(b"physical-descriptor-test").digest()
CODEC = MssqlR1SupportedCodecEntryV3("test.request", "1", DIGEST)
RECEIPT_TABLE_NAME = "dpone_provider_install_receipt_v3"


def _resource(name: str) -> MssqlR1PhysicalResourceRefV1:
    return MssqlR1PhysicalResourceRefV1(MssqlR1ResourceKindV1.STATIC_OBJECT, "dpone_authority", name, None)


def _prefix() -> tuple[MssqlR1ResultColumnV3, ...]:
    return (
        MssqlR1ResultColumnV3(1, "result_contract_version", "varchar", 64, 0, 0, False, "Latin1_General_100_BIN2"),
        MssqlR1ResultColumnV3(2, "outcome", "varchar", 32, 0, 0, False, "Latin1_General_100_BIN2"),
        MssqlR1ResultColumnV3(3, "request_digest", "binary", 32, 0, 0, False, None),
        MssqlR1ResultColumnV3(4, "projection_revision", "bigint", 8, 19, 0, False, None),
        MssqlR1ResultColumnV3(5, "server_observed_at", "datetime2", 8, 27, 7, False, None),
    )


def _params() -> tuple[MssqlR1SchemaProcedureParameterV3, ...]:
    return (
        MssqlR1SchemaProcedureParameterV3(
            1, "request_payload", "varbinary", -1, 0, 0, MssqlR1ParameterDirectionV3.INPUT
        ),
        MssqlR1SchemaProcedureParameterV3(2, "request_digest", "binary", 32, 0, 0, MssqlR1ParameterDirectionV3.INPUT),
        MssqlR1SchemaProcedureParameterV3(
            3, "projection_json", "nvarchar", -1, 0, 0, MssqlR1ParameterDirectionV3.INPUT
        ),
    )


def _request() -> MssqlR1RequestAuthorityV1:
    grammar = MssqlR1ProjectionGrammarV1(
        "test-request-v1",
        MssqlR1ProjectionRoleV1.REQUEST_JSON,
        (
            MssqlR1ProjectionFieldV1(
                1, "effect_key", MssqlR1ProjectionScalarKindV1.DIGEST, MssqlR1ValueCardinalityV1.SCALAR, False
            ),
        ),
    )
    return MssqlR1RequestAuthorityV1(
        MssqlR1RequestBindingKindV1.PAYLOAD_PROJECTION,
        CODEC,
        "request_payload",
        "request_digest",
        "projection_json",
        grammar,
        (),
    )


def _policy(path: MssqlR1ExecutionPathV1 = MssqlR1ExecutionPathV1.READ_ONLY_PROBE) -> MssqlR1ReplayOutcomePolicyV1:
    left = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.RESULT_COLUMN,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    right = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    pair = MssqlR1ComparisonPairV1(left, MssqlR1ComparisonOperatorV1.EQUAL, right)
    clause = MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.EXACT_REQUEST, (pair,), None, False)
    group = MssqlR1ReplayClauseGroupV1(1, MssqlR1ReplayBooleanOperatorV1.ALL, (clause,))
    return MssqlR1ReplayOutcomePolicyV1("found", path, MssqlR1ReplayBooleanOperatorV1.ALL, (group,))


def _errors() -> tuple[MssqlR1PhysicalErrorConditionV1, ...]:
    return tuple(
        sorted(
            (
                MssqlR1PhysicalErrorConditionV1(
                    f"error_{number}",
                    number,
                    1,
                    MssqlR1OutcomeClassV1.UNKNOWN,
                    MssqlR1RetryClassV1.BLOCKED,
                    MssqlR1FreshProbeKindV1.NONE,
                    f"blocker_{number}",
                    MssqlR1RedactionClassV1.PUBLIC,
                )
                for number in range(51001, 51013)
            ),
            key=lambda item: item.canonical_bytes,
        )
    )


def _semantics(
    *, scan: bool, mutating: bool = False, errors: tuple[MssqlR1PhysicalErrorConditionV1, ...] = ()
) -> MssqlR1ExecutionSemanticsV1:
    if mutating:
        resource = _resource(TABLE_NAMES[0])
        selector = MssqlR1ResourceInstanceSelectorV1(
            MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False
        )
        state = MssqlR1ComparisonCoordinateV1(
            MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
            resource,
            "state",
            MssqlR1ProjectionScalarKindV1.TEXT,
            MssqlR1ValueCardinalityV1.SCALAR,
            False,
        )
        current = MssqlR1ComparisonCoordinateV1(
            MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
            resource,
            "revision",
            MssqlR1ProjectionScalarKindV1.INTEGER,
            MssqlR1ValueCardinalityV1.SCALAR,
            False,
        )
        candidate = replace(current, source=MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD)
        transition = MssqlR1StateTransitionV1(
            1,
            MssqlR1TransitionKindV1.CAS,
            resource,
            selector,
            MssqlR1TransitionCardinalityV1.ONE,
            MssqlR1StateChangeV1(state, ("open",), "closed"),
            (MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.ADJACENT, current, None, None, candidate, 1),),
            (MssqlR1TransitionApplicabilityV1("found", MssqlR1ExecutionPathV1.FRESH_MUTATION),),
        )
        return MssqlR1ExecutionSemanticsV1(
            _request(),
            (MssqlR1OutcomeVariantV1("found", (), ()),),
            (
                MssqlR1PhysicalLockStepV1(
                    1,
                    MssqlR1LockKindV1.PHYSICAL,
                    resource,
                    MssqlR1LockMechanismV1.GUARDED_ROW,
                    MssqlR1LockActionV1.ACQUIRE,
                    MssqlR1LockModeV1.UPDATE,
                    MssqlR1LockOwnerV1.TRANSACTION,
                    MssqlR1LockCardinalityV1.ONE,
                    selector,
                    MssqlR1LockTimeoutPolicyV1.BOUNDED_ENVIRONMENT,
                    True,
                    True,
                    False,
                ),
            ),
            (MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.READ),),
            (MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.UPDATE),),
            MssqlR1TransitionAuthorityV1.SELF_CONTAINED,
            (transition,),
            (
                _policy(MssqlR1ExecutionPathV1.FRESH_MUTATION),
                _policy(MssqlR1ExecutionPathV1.IDEMPOTENT_REPLAY),
            ),
            errors,
        )
    return MssqlR1ExecutionSemanticsV1(
        _request(),
        () if scan else (MssqlR1OutcomeVariantV1("found", (), ()),),
        (),
        (),
        (),
        MssqlR1TransitionAuthorityV1.READ_ONLY,
        (),
        () if scan else (_policy(),),
        errors,
    )


def _portable_table(name: str) -> MssqlR1PortableSchemaObjectV3:
    if name == RECEIPT_TABLE_NAME:
        columns = (
            MssqlR1SchemaColumnV3(1, "installation_effect_key", "binary", 32, 0, 0, False, None, False, False),
            MssqlR1SchemaColumnV3(2, "request_digest", "binary", 32, 0, 0, False, None, False, False),
            MssqlR1SchemaColumnV3(3, "payload_bytes", "varbinary", -1, 0, 0, False, None, False, False),
            MssqlR1SchemaColumnV3(4, "payload_digest", "binary", 32, 0, 0, False, None, False, False),
            MssqlR1SchemaColumnV3(5, "committed_at", "datetime2", 8, 27, 7, False, None, False, False),
        )
        constraints = tuple(
            sorted(
                (
                    MssqlR1SchemaConstraintV3(
                        "synthetic_provider_install_receipt_pk",
                        MssqlR1ConstraintKindV3.PRIMARY_KEY,
                        ("installation_effect_key",),
                        None,
                        hashlib.sha256(b"synthetic provider receipt primary key").digest(),
                        True,
                        True,
                    ),
                    MssqlR1SchemaConstraintV3(
                        "synthetic_provider_install_receipt_payload_uq",
                        MssqlR1ConstraintKindV3.UNIQUE,
                        ("payload_digest",),
                        None,
                        hashlib.sha256(b"synthetic provider receipt unique payload").digest(),
                        True,
                        True,
                    ),
                    MssqlR1SchemaConstraintV3(
                        "synthetic_provider_install_receipt_payload_nonempty_ck",
                        MssqlR1ConstraintKindV3.CHECK,
                        ("payload_bytes",),
                        None,
                        hashlib.sha256(b"synthetic provider receipt payload nonempty").digest(),
                        True,
                        True,
                    ),
                    MssqlR1SchemaConstraintV3(
                        "synthetic_provider_install_receipt_payload_digest_ck",
                        MssqlR1ConstraintKindV3.CHECK,
                        ("payload_bytes", "payload_digest"),
                        None,
                        hashlib.sha256(b"synthetic provider receipt payload digest").digest(),
                        True,
                        True,
                    ),
                ),
                key=lambda item: item.canonical_bytes,
            )
        )
        return MssqlR1PortableSchemaObjectV3(
            MssqlR1SchemaObjectKindV3.TABLE,
            "dpone_authority",
            name,
            None,
            columns,
            constraints,
            (),
            (),
            None,
            None,
            (),
            (),
        )
    columns = (MssqlR1SchemaColumnV3(1, "id", "bigint", 8, 19, 0, False, None, False, False),)
    if name == TABLE_NAMES[0]:
        columns += (
            MssqlR1SchemaColumnV3(2, "state", "varchar", 32, 0, 0, False, "Latin1_General_100_BIN2", False, False),
            MssqlR1SchemaColumnV3(3, "revision", "bigint", 8, 19, 0, False, None, False, False),
        )
    if name in {"dpone_sealed_effect_v3", "dpone_effect_receipt_v3"}:
        columns += (MssqlR1SchemaColumnV3(2, "request_digest", "binary", 32, 0, 0, False, None, False, False),)
    return MssqlR1PortableSchemaObjectV3(
        MssqlR1SchemaObjectKindV3.TABLE,
        "dpone_authority",
        name,
        None,
        columns,
        (),
        (),
        (),
        None,
        None,
        (),
        (),
    )


def _portable_procedure(name: str) -> tuple[MssqlR1PortableSchemaObjectV3, MssqlR1DefinitionPayloadV1]:
    text = f"synthetic module definition {name}\n"
    definition = MssqlR1DefinitionPayloadV1.create(MssqlR1DefinitionKindV1.MODULE_TEXT, text)
    scan = name == "dpone_scan_stage_v3"
    result = (
        MssqlR1StageScanTemplateV3(
            MssqlR1ResultCardinalityV3.ZERO_OR_MANY,
            DIGEST,
            (MssqlR1ResultColumnV3(1, "artifact_id", "uniqueidentifier", 16, 0, 0, False, None),),
        )
        if scan
        else MssqlR1FixedResultV3(MssqlR1ResultCardinalityV3.EXACTLY_ONE, _prefix())
    )
    signer = (
        MssqlR1SignerProfileKindV3.ATTESTOR
        if name == "dpone_attest_schema_v3"
        else MssqlR1SignerProfileKindV3.STAGE_OWNER
        if name
        in {
            "dpone_open_stage_v3",
            "dpone_begin_stage_chunk_v3",
            "dpone_complete_stage_chunk_v3",
            "dpone_observe_stage_v3",
            "dpone_scan_stage_v3",
            "dpone_seal_stage_v3",
            "dpone_recover_expired_open_v3",
            "dpone_consume_stage_set_v3",
        }
        else MssqlR1SignerProfileKindV3.NONE
    )
    portable = MssqlR1PortableSchemaObjectV3(
        MssqlR1SchemaObjectKindV3.PROCEDURE,
        "dpone_authority",
        name,
        definition.definition_digest,
        (),
        (),
        (),
        _params(),
        result,
        MssqlR1ModuleOptionsV3("caller", True, True, False, False, signer),
        (),
        (),
    )
    return portable, definition


def _permissions() -> tuple[MssqlR1PermissionRuleV3, ...]:
    mapping = {
        MssqlR1SubjectRoleV3.PROVISIONER: MssqlR1PrincipalKindV1.PROVISIONER,
        MssqlR1SubjectRoleV3.RUNTIME: MssqlR1PrincipalKindV1.RUNTIME,
        MssqlR1SubjectRoleV3.OBSERVER: MssqlR1PrincipalKindV1.OBSERVER,
    }
    rules = []
    for subject, principal in mapping.items():
        for short in PRINCIPAL_SHORT[principal]:
            rules.append(
                MssqlR1PermissionRuleV3(
                    subject,
                    MssqlR1SubjectRoleV3.PROVISIONER,
                    MssqlR1PermissionSourceV3.DIRECT,
                    MssqlR1PermissionScopeV3.OBJECT,
                    "dpone_authority",
                    f"dpone_{short}_v3",
                    None,
                    "EXECUTE",
                    MssqlR1PermissionEffectV3.GRANT,
                    False,
                )
            )
    return tuple(sorted(rules, key=lambda item: item.canonical_bytes))


PRINCIPAL_SHORT = {
    MssqlR1PrincipalKindV1.PROVISIONER: {
        "provision_registration",
        "rotate_registration",
        "import_generation_authority_set",
        "probe_control_effect",
        "attest_schema",
        "probe_registration",
    },
    MssqlR1PrincipalKindV1.RUNTIME: {
        "probe_control_effect",
        "admit_operation",
        "seal_effect",
        "take_over_sealed",
        "probe_pre_source",
        "open_stage",
        "renew_stage",
        "begin_stage_chunk",
        "complete_stage_chunk",
        "observe_stage",
        "observe_stage_chunk",
        "scan_stage",
        "seal_stage",
        "recover_expired_open",
        "probe_open_recovery",
        "admit_writer",
        "resolve_admit_authority_set",
        "append_effect_receipt",
        "write_xmin_checkpoint",
        "consume_stage_set",
        "consume_authority_set",
        "advance_operation_and_head",
        "prove_candidate_effect",
        "probe_effect",
        "probe_registration",
    },
    MssqlR1PrincipalKindV1.OBSERVER: {
        "probe_control_effect",
        "attest_schema",
        "probe_pre_source",
        "observe_stage",
        "observe_stage_chunk",
        "probe_open_recovery",
        "probe_effect",
        "probe_registration",
    },
}


def _principals(name: str) -> tuple[MssqlR1PrincipalKindV1, ...]:
    short = name.removeprefix("dpone_").removesuffix("_v3")
    return tuple(
        sorted((role for role, names in PRINCIPAL_SHORT.items() if short in names), key=lambda item: item.value)
    )


def _binding(kind: MssqlR1BindingModuleKindV1) -> MssqlR1BindingModuleTemplateV1:
    text = f"synthetic binding definition {kind.value}\n"
    definition = MssqlR1DefinitionPayloadV1.create(MssqlR1DefinitionKindV1.MODULE_TEMPLATE, text)
    parameters = (
        MssqlR1SchemaProcedureParameterV3(1, "request_digest", "binary", 32, 0, 0, MssqlR1ParameterDirectionV3.INPUT),
    )
    authority = MssqlR1RequestAuthorityV1(
        MssqlR1RequestBindingKindV1.SEALED_REQUEST_DIGEST, CODEC, None, "request_digest", None, None, ()
    )
    sealed_resource = _resource("dpone_sealed_effect_v3")
    read = MssqlR1PhysicalResourceAccessV1(sealed_resource, MssqlR1AccessKindV1.READ)
    sealed_coordinate = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
        sealed_resource,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    parameter_coordinate = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )

    def binding_policy(path: MssqlR1ExecutionPathV1) -> MssqlR1ReplayOutcomePolicyV1:
        pair = MssqlR1ComparisonPairV1(
            parameter_coordinate,
            MssqlR1ComparisonOperatorV1.EQUAL,
            sealed_coordinate,
        )
        clause = MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.EXACT_REQUEST, (pair,), None, False)
        group = MssqlR1ReplayClauseGroupV1(1, MssqlR1ReplayBooleanOperatorV1.ALL, (clause,))
        return MssqlR1ReplayOutcomePolicyV1("found", path, MssqlR1ReplayBooleanOperatorV1.ALL, (group,))

    quality = kind in {MssqlR1BindingModuleKindV1.BATCH_QUALITY, MssqlR1BindingModuleKindV1.XMIN_QUALITY}
    write = () if quality else (MssqlR1PhysicalResourceAccessV1(_resource(TABLE_NAMES[0]), MssqlR1AccessKindV1.UPDATE),)
    semantics = MssqlR1ExecutionSemanticsV1(
        authority,
        (MssqlR1OutcomeVariantV1("found", (), ()),),
        (),
        (read,),
        write,
        MssqlR1TransitionAuthorityV1.CALLER_UOW,
        (),
        (
            binding_policy(MssqlR1ExecutionPathV1.FRESH_MUTATION),
            binding_policy(MssqlR1ExecutionPathV1.IDEMPOTENT_REPLAY),
        ),
        _errors() if kind is MssqlR1BindingModuleKindV1.BATCH_MUTATE else (),
    )
    return MssqlR1BindingModuleTemplateV1(
        kind,
        f"dpone_b_{{binding_uuid_hex}}_{kind.value}_v3",
        parameters,
        MssqlR1FixedResultV3(MssqlR1ResultCardinalityV3.EXACTLY_ONE, _prefix()),
        definition,
        semantics,
        (MssqlR1PrincipalKindV1.RUNTIME,),
        MssqlR1BindingSignerKindV1.BINDING_SCOPED,
    )


def descriptor() -> MssqlR1PhysicalSchemaDescriptorV1:
    tables = tuple(
        MssqlR1PhysicalTableDescriptorV1(
            _portable_table(name),
            MssqlR1DefinitionPayloadV1.create(
                MssqlR1DefinitionKindV1.TABLE_DDL, f"synthetic table definition {name}\n"
            ),
            MssqlR1TableLifecycleV1.IMMUTABLE
            if name == RECEIPT_TABLE_NAME
            else MssqlR1TableLifecycleV1.GUARDED_MUTABLE,
            MssqlR1MutationPolicyV1.INSTALLER_ONLY
            if name == RECEIPT_TABLE_NAME
            else MssqlR1MutationPolicyV1.GUARDED_PROCEDURE_ONLY,
            _resource(name),
        )
        for name in TABLE_NAMES
    )
    procedure_pairs = tuple(_portable_procedure(name) for name in PROCEDURE_NAMES)
    procedures = tuple(
        MssqlR1PhysicalProcedureDescriptorV1(
            portable,
            definition,
            _semantics(
                scan=portable.object_name == "dpone_scan_stage_v3",
                mutating=portable.object_name == PROCEDURE_NAMES[0],
            ),
            _principals(portable.object_name),
        )
        for portable, definition in procedure_pairs
    )
    objects = tuple(
        sorted(
            (*(item.portable_object for item in tables), *(item.portable_object for item in procedures)),
            key=lambda item: (item.schema_name.encode(), item.object_name.encode()),
        )
    )
    contract = MssqlR1SchemaContractV3(
        "dpone_authority",
        "dpone_stage",
        DIGEST,
        objects,
        _permissions(),
        (
            MssqlR1SignerProfileV3(MssqlR1SignerProfileKindV3.ATTESTOR, "attestor_cert", "attestor_user"),
            MssqlR1SignerProfileV3(MssqlR1SignerProfileKindV3.STAGE_OWNER, "stage_cert", "stage_user"),
        ),
        (CODEC,),
    )
    catalog = MssqlR1PhysicalResourceRefV1(MssqlR1ResourceKindV1.CATALOG, None, None, "migration_catalog")
    declarations = [
        MssqlR1PhysicalResourceDeclarationV1(
            _resource(name),
            _resource(name),
            (
                MssqlR1ResourceFieldV1(
                    1,
                    "installation_effect_key",
                    MssqlR1ProjectionScalarKindV1.DIGEST,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    1,
                ),
                MssqlR1ResourceFieldV1(
                    2,
                    "request_digest",
                    MssqlR1ProjectionScalarKindV1.DIGEST,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    None,
                ),
                MssqlR1ResourceFieldV1(
                    3,
                    "payload_bytes",
                    MssqlR1ProjectionScalarKindV1.BINARY,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    None,
                ),
                MssqlR1ResourceFieldV1(
                    4,
                    "payload_digest",
                    MssqlR1ProjectionScalarKindV1.DIGEST,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    None,
                ),
            )
            if name == RECEIPT_TABLE_NAME
            else (
                MssqlR1ResourceFieldV1(
                    1,
                    "request_digest",
                    MssqlR1ProjectionScalarKindV1.DIGEST,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    None,
                ),
            )
            if name in {"dpone_sealed_effect_v3", "dpone_effect_receipt_v3"}
            else (
                MssqlR1ResourceFieldV1(
                    1, "state", MssqlR1ProjectionScalarKindV1.TEXT, MssqlR1ValueCardinalityV1.SCALAR, False, None
                ),
                MssqlR1ResourceFieldV1(
                    2,
                    "revision",
                    MssqlR1ProjectionScalarKindV1.INTEGER,
                    MssqlR1ValueCardinalityV1.SCALAR,
                    False,
                    None,
                ),
            )
            if name == TABLE_NAMES[0]
            else (),
            (MssqlR1AccessKindV1.DDL, MssqlR1AccessKindV1.INSERT, MssqlR1AccessKindV1.READ)
            if name == RECEIPT_TABLE_NAME
            else (MssqlR1AccessKindV1.READ, MssqlR1AccessKindV1.UPDATE)
            if name == TABLE_NAMES[0]
            else (MssqlR1AccessKindV1.READ,),
            MssqlR1LockKindV1.PHYSICAL if name in TABLE_NAMES else None,
            0,
            MssqlR1LockCardinalityV1.ONE,
        )
        for name in (*TABLE_NAMES, *PROCEDURE_NAMES)
    ]
    declarations.append(
        MssqlR1PhysicalResourceDeclarationV1(
            catalog, None, (), (MssqlR1AccessKindV1.READ,), None, 0, MssqlR1LockCardinalityV1.ONE
        )
    )
    declarations_tuple = tuple(sorted(declarations, key=lambda item: item.canonical_bytes))
    probe = MssqlR1MigrationProbeV1(
        "inventory_absent",
        MssqlR1MigrationObservationKindV1.INVENTORY_ABSENT,
        catalog,
        MssqlR1MigrationDispositionV1.INSTALL,
        None,
        True,
    )
    bindings = [_binding(kind) for kind in MssqlR1BindingModuleKindV1]
    return MssqlR1PhysicalSchemaDescriptorV1(
        VERSION,
        MssqlR1PhysicalEngineProfileV1(16, 160, "Latin1_General_100_BIN2", "PARTIAL", "DISABLED", False, False),
        MssqlR1PhysicalSessionProfileV1("SERIALIZABLE", True, True, True, True, True, True, False, True, True, True),
        contract,
        declarations_tuple,
        tables,
        procedures,
        tuple(bindings),
        (probe,),
    )


def test_full_20_29_6_descriptor_round_trips_byte_identically() -> None:
    value = descriptor()
    decoded = MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(value.canonical_bytes)
    assert decoded == value
    assert decoded.canonical_bytes == value.canonical_bytes
    assert decoded.digest == value.digest


def test_provider_install_receipt_has_exact_inventory_shape_and_resource_authority() -> None:
    value = descriptor()
    assert VERSION == "dpone-mssql-r1-v3-physical-schema-2-r2"
    assert len(TABLE_NAMES) == 20
    receipt_index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    assert TABLE_NAMES[receipt_index - 1] == "dpone_generation_authority_consumption_v3"
    assert TABLE_NAMES[receipt_index + 1] == "dpone_control_receipt_v3"
    assert len(PROCEDURE_NAMES) == 29
    assert len(value.ordered_binding_module_templates) == 6

    receipt = value.ordered_tables[receipt_index]
    assert tuple(
        (column.name, column.sql_type, column.maximum_length, column.precision, column.scale, column.nullable)
        for column in receipt.portable_object.ordered_columns
    ) == (
        ("installation_effect_key", "binary", 32, 0, 0, False),
        ("request_digest", "binary", 32, 0, 0, False),
        ("payload_bytes", "varbinary", -1, 0, 0, False),
        ("payload_digest", "binary", 32, 0, 0, False),
        ("committed_at", "datetime2", 8, 27, 7, False),
    )
    assert {
        (constraint.kind, constraint.ordered_columns) for constraint in receipt.portable_object.ordered_constraints
    } == {
        (MssqlR1ConstraintKindV3.PRIMARY_KEY, ("installation_effect_key",)),
        (MssqlR1ConstraintKindV3.UNIQUE, ("payload_digest",)),
        (MssqlR1ConstraintKindV3.CHECK, ("payload_bytes",)),
        (MssqlR1ConstraintKindV3.CHECK, ("payload_bytes", "payload_digest")),
    }
    assert all(
        constraint.trusted is True and constraint.enabled is True
        for constraint in receipt.portable_object.ordered_constraints
    )
    assert receipt.lifecycle is MssqlR1TableLifecycleV1.IMMUTABLE
    assert receipt.mutation_policy is MssqlR1MutationPolicyV1.INSTALLER_ONLY

    declaration = next(
        item for item in value.ordered_resource_declarations if item.resource == _resource(RECEIPT_TABLE_NAME)
    )
    assert tuple(
        (field.name, field.scalar_kind, field.instance_key_ordinal) for field in declaration.ordered_fields
    ) == (
        ("installation_effect_key", MssqlR1ProjectionScalarKindV1.DIGEST, 1),
        ("request_digest", MssqlR1ProjectionScalarKindV1.DIGEST, None),
        ("payload_bytes", MssqlR1ProjectionScalarKindV1.BINARY, None),
        ("payload_digest", MssqlR1ProjectionScalarKindV1.DIGEST, None),
    )
    assert declaration.ordered_allowed_access_kinds == (
        MssqlR1AccessKindV1.DDL,
        MssqlR1AccessKindV1.INSERT,
        MssqlR1AccessKindV1.READ,
    )


def _replace_receipt_portable(
    value: MssqlR1PhysicalSchemaDescriptorV1,
    portable: MssqlR1PortableSchemaObjectV3,
) -> MssqlR1PhysicalSchemaDescriptorV1:
    receipt_index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    tables = list(value.ordered_tables)
    tables[receipt_index] = replace(tables[receipt_index], portable_object=portable)
    objects = tuple(
        sorted(
            (
                portable if item.object_name == RECEIPT_TABLE_NAME else item
                for item in value.expected_schema_contract.ordered_objects
            ),
            key=lambda item: (item.schema_name.encode(), item.object_name.encode()),
        )
    )
    return replace(
        value,
        ordered_tables=tuple(tables),
        expected_schema_contract=replace(value.expected_schema_contract, ordered_objects=objects),
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: replace(value, descriptor_version="legacy"), "version"),
        (lambda value: replace(value, ordered_tables=value.ordered_tables[:-1]), "inventory"),
        (lambda value: replace(value, ordered_procedures=tuple(reversed(value.ordered_procedures))), "inventory"),
        (
            lambda value: replace(value, ordered_binding_module_templates=value.ordered_binding_module_templates[:-1]),
            "inventory",
        ),
    ],
)
def test_aggregate_identity_mutations_fail_closed(mutation: object, message: str) -> None:
    with pytest.raises(MssqlR1V3ContractError, match=message):
        mutation(descriptor())  # type: ignore[operator]


def test_decode_rejects_extra_field_and_foreign_domain() -> None:
    value = descriptor()
    extra = canonical_bytes(
        b"dpone-r1-physical-schema-descriptor-v1\0",
        tuple(decode_canonical_bytes(value.canonical_bytes, b"dpone-r1-physical-schema-descriptor-v1\0", field_count=9))
        + ("extra",),
    )
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(extra)
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(b"legacy\0")


def test_nested_canonical_contracts_reject_truncated_and_trailing_input() -> None:
    instances = _supplemental_instances()
    for instance in (instances[14], instances[16], instances[24], instances[27]):
        payload = instance.canonical_bytes
        for tampered in (payload[:-1], payload + b"\0"):
            with pytest.raises(MssqlR1V3ContractError):
                type(instance).from_canonical_bytes(tampered)


def test_raw_enum_and_bool_for_int_fail_closed() -> None:
    with pytest.raises(MssqlR1V3ContractError, match="exact enum"):
        replace(descriptor().ordered_tables[0], lifecycle="immutable")
    with pytest.raises(MssqlR1V3ContractError, match="integer"):
        MssqlR1PhysicalEngineProfileV1(True, 160, "Latin1_General_100_BIN2", "PARTIAL", "DISABLED", False, False)


def test_definition_digest_and_canonical_text_fail_closed() -> None:
    valid = MssqlR1DefinitionPayloadV1.create(MssqlR1DefinitionKindV1.TABLE_DDL, "synthetic table definition\n")
    with pytest.raises(MssqlR1V3ContractError, match="digest"):
        replace(valid, definition_digest=DIGEST)
    with pytest.raises(MssqlR1V3ContractError, match="canonical"):
        MssqlR1DefinitionPayloadV1.create(MssqlR1DefinitionKindV1.TABLE_DDL, "synthetic table definition\r\n")


def _descriptor_field_registry() -> dict[type[object], tuple[str, ...]]:
    return {
        MssqlR1PhysicalEngineProfileV1: (
            "engine_major",
            "compatibility_level",
            "contract_collation",
            "containment",
            "delayed_durability",
            "mars_enabled",
            "pooling_enabled",
        ),
        MssqlR1DefinitionPayloadV1: ("definition_kind", "utf8_bytes", "definition_digest"),
        MssqlR1PhysicalSessionProfileV1: (
            "isolation_level",
            "ansi_nulls",
            "ansi_padding",
            "ansi_warnings",
            "arithabort",
            "concat_null_yields_null",
            "quoted_identifier",
            "numeric_roundabort",
            "xact_abort",
            "nocount",
            "autocommit_enabled",
        ),
        MssqlR1PhysicalResourceRefV1: ("resource_kind", "schema_name", "object_name", "coordinate_name"),
        MssqlR1ResourceFieldV1: (
            "ordinal",
            "name",
            "scalar_kind",
            "value_cardinality",
            "nullable",
            "instance_key_ordinal",
        ),
        MssqlR1PhysicalResourceAccessV1: ("resource", "access_kind"),
        MssqlR1PhysicalResourceDeclarationV1: (
            "resource",
            "associated_static_object",
            "ordered_fields",
            "ordered_allowed_access_kinds",
            "allowed_lock_kind",
            "lock_subrank",
            "lock_cardinality",
        ),
        MssqlR1ComparisonCoordinateV1: (
            "source",
            "resource",
            "field_name",
            "scalar_kind",
            "value_cardinality",
            "nullable",
        ),
        MssqlR1ComparisonLiteralV1: ("scalar_kind", "value"),
        MssqlR1ResourceInstanceSelectorV1: ("selector_kind", "ordered_coordinates", "allow_empty"),
        MssqlR1PhysicalLockStepV1: (
            "ordinal",
            "lock_kind",
            "resource",
            "mechanism",
            "action",
            "mode",
            "owner",
            "cardinality",
            "instance_selector",
            "timeout_policy",
            "requires_updlock",
            "requires_holdlock",
            "requires_tablockx",
        ),
        MssqlR1ProjectionFieldV1: ("ordinal", "name", "scalar_kind", "value_cardinality", "nullable"),
        MssqlR1ProjectionGrammarV1: ("grammar_version", "projection_role", "ordered_fields"),
        MssqlR1ScalarParameterBindingV1: ("parameter_name", "value_source"),
        MssqlR1RequestAuthorityV1: (
            "binding_kind",
            "codec",
            "request_payload_parameter",
            "request_digest_parameter",
            "projection_parameter",
            "projection_grammar",
            "ordered_scalar_parameter_bindings",
        ),
        MssqlR1OutcomeVariantV1: ("outcome_literal", "ordered_null_columns", "ordered_present_columns"),
        MssqlR1RevisionRuleV1: (
            "rule_kind",
            "current_value",
            "expected_value",
            "requested_candidate",
            "candidate_value",
            "delta",
        ),
        MssqlR1StateChangeV1: ("state_field", "ordered_predecessor_states", "candidate_state"),
        MssqlR1TransitionApplicabilityV1: ("outcome_literal", "execution_path"),
        MssqlR1StateTransitionV1: (
            "ordinal",
            "transition_kind",
            "owner_resource",
            "instance_selector",
            "cardinality",
            "state_change",
            "ordered_revision_rules",
            "ordered_applicabilities",
        ),
        MssqlR1ResourceExistenceOperandV1: ("resource", "instance_selector"),
        MssqlR1ComparisonPairV1: ("left", "operator", "right"),
        MssqlR1ReplayClauseV1: (
            "ordinal",
            "comparator",
            "ordered_comparisons",
            "fresh_proof_resource",
            "requires_descendant_proof",
        ),
        MssqlR1ReplayClauseGroupV1: ("ordinal", "boolean_operator", "ordered_clauses"),
        MssqlR1ReplayOutcomePolicyV1: (
            "outcome_literal",
            "execution_path",
            "group_operator",
            "ordered_groups",
        ),
        MssqlR1PhysicalErrorConditionV1: (
            "condition_id",
            "error_number",
            "error_state",
            "outcome_class",
            "retry_class",
            "fresh_probe_kind",
            "public_blocker_code",
            "redaction_class",
        ),
        MssqlR1MigrationProbeV1: (
            "probe_id",
            "observation_kind",
            "observation_resource",
            "disposition",
            "blocker_code",
            "zero_mutation_before_decision",
        ),
        MssqlR1ExecutionSemanticsV1: (
            "request_authority",
            "ordered_outcome_variants",
            "ordered_lock_steps",
            "ordered_read_set",
            "ordered_write_set",
            "transition_authority",
            "ordered_state_transitions",
            "ordered_replay_outcome_policies",
            "ordered_error_conditions",
        ),
        MssqlR1PhysicalTableDescriptorV1: (
            "portable_object",
            "definition",
            "lifecycle",
            "mutation_policy",
            "lock_resource",
        ),
        MssqlR1PhysicalProcedureDescriptorV1: (
            "portable_object",
            "definition",
            "execution_semantics",
            "ordered_execute_principals",
        ),
        MssqlR1BindingModuleTemplateV1: (
            "module_kind",
            "name_template",
            "ordered_parameters",
            "result_contract",
            "definition_template",
            "execution_semantics",
            "ordered_execute_principals",
            "signer_kind",
        ),
        MssqlR1PhysicalSchemaDescriptorV1: (
            "descriptor_version",
            "engine_profile",
            "session_profile",
            "expected_schema_contract",
            "ordered_resource_declarations",
            "ordered_tables",
            "ordered_procedures",
            "ordered_binding_module_templates",
            "ordered_migration_probes",
        ),
    }


DESCRIPTOR_FIELD_REGISTRY = _descriptor_field_registry()


def test_mutation_registry_covers_every_descriptor_dataclass_field() -> None:
    for contract, expected in DESCRIPTOR_FIELD_REGISTRY.items():
        assert tuple(field.name for field in fields(contract)) == expected
    assert len(DESCRIPTOR_FIELD_REGISTRY) == 32


def _supplemental_instances() -> tuple[object, ...]:
    value = descriptor()
    resource = _resource(TABLE_NAMES[0])
    parameter = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    candidate = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
        resource,
        "revision",
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    current = replace(candidate, source=MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD)
    selector = MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False)
    pair = _policy().ordered_groups[0].ordered_clauses[0].ordered_comparisons[0]
    clause = _policy().ordered_groups[0].ordered_clauses[0]
    state = MssqlR1StateChangeV1(
        replace(
            candidate,
            source=MssqlR1ComparisonSourceV1.RESOURCE_FIELD,
            field_name="state",
            scalar_kind=MssqlR1ProjectionScalarKindV1.TEXT,
        ),
        ("open",),
        "closed",
    )
    transition = MssqlR1StateTransitionV1(
        1,
        MssqlR1TransitionKindV1.CAS,
        resource,
        selector,
        MssqlR1TransitionCardinalityV1.ONE,
        state,
        (MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.UNCHANGED, current, None, None, candidate, 0),),
        (MssqlR1TransitionApplicabilityV1("found", MssqlR1ExecutionPathV1.FRESH_MUTATION),),
    )
    lock = MssqlR1PhysicalLockStepV1(
        1,
        MssqlR1LockKindV1.PHYSICAL,
        resource,
        MssqlR1LockMechanismV1.GUARDED_ROW,
        MssqlR1LockActionV1.ACQUIRE,
        MssqlR1LockModeV1.UPDATE,
        MssqlR1LockOwnerV1.TRANSACTION,
        MssqlR1LockCardinalityV1.ONE,
        selector,
        MssqlR1LockTimeoutPolicyV1.BOUNDED_ENVIRONMENT,
        True,
        True,
        False,
    )
    declaration = MssqlR1PhysicalResourceDeclarationV1(
        resource,
        resource,
        (
            MssqlR1ResourceFieldV1(
                1,
                "id",
                MssqlR1ProjectionScalarKindV1.INTEGER,
                MssqlR1ValueCardinalityV1.SCALAR,
                False,
                1,
            ),
        ),
        (MssqlR1AccessKindV1.READ,),
        MssqlR1LockKindV1.PHYSICAL,
        0,
        MssqlR1LockCardinalityV1.ONE,
    )
    return (
        value.engine_profile,
        value.session_profile,
        value.ordered_tables[0].definition,
        resource,
        declaration.ordered_fields[0],
        MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.READ),
        declaration,
        parameter,
        MssqlR1ComparisonLiteralV1(MssqlR1ProjectionScalarKindV1.INTEGER, 1),
        selector,
        lock,
        _request().projection_grammar.ordered_fields[0],  # type: ignore[union-attr]
        _request().projection_grammar,  # type: ignore[union-attr]
        MssqlR1ScalarParameterBindingV1(
            "request_digest",
            replace(parameter, source=MssqlR1ComparisonSourceV1.REQUEST_FIELD, field_name="effect_key"),
        ),
        _request(),
        MssqlR1OutcomeVariantV1("found", (), ()),
        transition.ordered_revision_rules[0],
        state,
        transition.ordered_applicabilities[0],
        transition,
        MssqlR1ResourceExistenceOperandV1(resource, selector),
        pair,
        clause,
        _policy().ordered_groups[0],
        _policy(),
        _errors()[0],
        value.ordered_migration_probes[0],
        value.ordered_procedures[0].execution_semantics,
        value.ordered_tables[0],
        value.ordered_procedures[0],
        value.ordered_binding_module_templates[0],
        value,
    )


def test_every_descriptor_dataclass_round_trips_canonically() -> None:
    for instance in _supplemental_instances():
        decoded = type(instance).from_canonical_bytes(instance.canonical_bytes)
        assert decoded == instance


def test_every_closed_enum_branch_has_exact_canonical_round_trip() -> None:
    assert len(ENUM_BRANCH_REGISTRY) == 35
    assert len({contract.__name__ for contract in ENUM_BRANCH_REGISTRY}) == len(ENUM_BRANCH_REGISTRY)
    for contract in ENUM_BRANCH_REGISTRY:
        assert tuple(contract)
        for member in contract:
            payload = canonical_bytes(b"dpone-r1-physical-enum-test\0", (member,))
            (decoded,) = decode_canonical_bytes(payload, b"dpone-r1-physical-enum-test\0", field_count=1)
            assert expect_enum(contract, decoded, contract.__name__) is member


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        (MssqlR1ProjectionScalarKindV1.TEXT, "value"),
        (MssqlR1ProjectionScalarKindV1.BINARY, b"value"),
        (MssqlR1ProjectionScalarKindV1.DIGEST, b"d" * 32),
        (MssqlR1ProjectionScalarKindV1.UUID, "00000000-0000-0000-0000-000000000001"),
        (MssqlR1ProjectionScalarKindV1.INTEGER, -1),
        (MssqlR1ProjectionScalarKindV1.BOOLEAN, False),
        (MssqlR1ProjectionScalarKindV1.UTC, "2026-01-01T00:00:00.000000Z"),
    ],
)
def test_literal_union_branches_round_trip(
    kind: MssqlR1ProjectionScalarKindV1,
    value: str | bytes | int | bool,
) -> None:
    literal = MssqlR1ComparisonLiteralV1(kind, value)
    assert MssqlR1ComparisonLiteralV1.from_canonical_bytes(literal.canonical_bytes) == literal


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        (MssqlR1ProjectionScalarKindV1.TEXT, object()),
        (MssqlR1ProjectionScalarKindV1.BINARY, "bytes"),
        (MssqlR1ProjectionScalarKindV1.DIGEST, b"short"),
        (MssqlR1ProjectionScalarKindV1.UUID, "NOT-CANONICAL"),
        (MssqlR1ProjectionScalarKindV1.INTEGER, True),
        (MssqlR1ProjectionScalarKindV1.BOOLEAN, 1),
        (MssqlR1ProjectionScalarKindV1.UTC, "2026-01-01T00:00:00Z"),
    ],
)
def test_literal_union_rejects_inexact_value_branch(
    kind: MssqlR1ProjectionScalarKindV1,
    value: object,
) -> None:
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1ComparisonLiteralV1(kind, value)  # type: ignore[arg-type]


def test_selector_union_branches_are_distinct_and_exact() -> None:
    scalar = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.REQUEST_FIELD,
        None,
        "key",
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    ordered = replace(scalar, value_cardinality=MssqlR1ValueCardinalityV1.ORDERED_SET)
    selectors = (
        MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False),
        MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE, (scalar,), False),
        MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES, (ordered,), True),
    )
    assert len({item.canonical_bytes for item in selectors}) == 3
    assert all(
        MssqlR1ResourceInstanceSelectorV1.from_canonical_bytes(item.canonical_bytes) == item for item in selectors
    )


def test_selector_union_rejects_cross_branch_shapes() -> None:
    scalar = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.REQUEST_FIELD,
        None,
        "key",
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    ordered = replace(scalar, value_cardinality=MssqlR1ValueCardinalityV1.ORDERED_SET)
    invalid = (
        (MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (scalar,)),
        (MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE, (ordered,)),
        (MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES, (scalar,)),
    )
    for kind, coordinates in invalid:
        with pytest.raises(MssqlR1V3ContractError, match="selector"):
            MssqlR1ResourceInstanceSelectorV1(kind, coordinates, False)


def _validate_aggregate_selector_boundary(
    cardinality: MssqlR1TransitionCardinalityV1,
    selector_kind: MssqlR1ResourceInstanceSelectorKindV1,
    allow_empty: bool,
    *,
    declaration_has_key: bool = True,
) -> None:
    resource = _resource(TABLE_NAMES[0])
    authority = _request()
    if selector_kind is MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES:
        grammar = authority.projection_grammar
        assert grammar is not None
        coordinate = MssqlR1ComparisonCoordinateV1(
            MssqlR1ComparisonSourceV1.REQUEST_FIELD,
            None,
            "keys",
            MssqlR1ProjectionScalarKindV1.INTEGER,
            MssqlR1ValueCardinalityV1.ORDERED_SET,
            False,
        )
        authority = replace(
            authority,
            projection_grammar=replace(
                grammar,
                ordered_fields=(
                    *grammar.ordered_fields,
                    MssqlR1ProjectionFieldV1(
                        2,
                        "keys",
                        MssqlR1ProjectionScalarKindV1.INTEGER,
                        MssqlR1ValueCardinalityV1.ORDERED_SET,
                        False,
                    ),
                ),
            ),
        )
    else:
        coordinate = _integer_coordinate(MssqlR1ComparisonSourceV1.RESOURCE_FIELD, "id", resource)
    selector = MssqlR1ResourceInstanceSelectorV1(selector_kind, (coordinate,), allow_empty)
    transition = MssqlR1StateTransitionV1(
        1,
        MssqlR1TransitionKindV1.APPEND,
        resource,
        selector,
        cardinality,
        None,
        (),
        (MssqlR1TransitionApplicabilityV1("found", MssqlR1ExecutionPathV1.FRESH_MUTATION),),
    )
    fields = (
        (
            MssqlR1ResourceFieldV1(
                1,
                "id",
                MssqlR1ProjectionScalarKindV1.INTEGER,
                MssqlR1ValueCardinalityV1.SCALAR,
                False,
                1,
            ),
        )
        if declaration_has_key
        else ()
    )
    declaration = MssqlR1PhysicalResourceDeclarationV1(
        resource,
        resource,
        fields,
        (MssqlR1AccessKindV1.READ, MssqlR1AccessKindV1.UPDATE),
        (MssqlR1LockKindV1.PHYSICAL if cardinality is MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET else None),
        0,
        (
            MssqlR1LockCardinalityV1.EXACT_REQUEST_SET
            if cardinality is MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET
            else MssqlR1LockCardinalityV1.ONE
        ),
    )
    execution = MssqlR1ExecutionSemanticsV1(
        authority,
        (MssqlR1OutcomeVariantV1("found", (), ()),),
        (),
        (MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.READ),),
        (MssqlR1PhysicalResourceAccessV1(resource, MssqlR1AccessKindV1.UPDATE),),
        MssqlR1TransitionAuthorityV1.SELF_CONTAINED,
        (transition,),
        (
            _policy(MssqlR1ExecutionPathV1.FRESH_MUTATION),
            _policy(MssqlR1ExecutionPathV1.IDEMPOTENT_REPLAY),
        ),
        (),
    )
    execution.validate_contract(
        _params(),
        MssqlR1FixedResultV3(MssqlR1ResultCardinalityV3.EXACTLY_ONE, _prefix()),
        {resource.canonical_bytes: declaration},
        (CODEC,),
    )


@pytest.mark.parametrize(
    ("cardinality", "selector_kind", "allow_empty"),
    [
        (
            MssqlR1TransitionCardinalityV1.ZERO_OR_ONE,
            MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE,
            True,
        ),
        (
            MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET,
            MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES,
            False,
        ),
        (
            MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET,
            MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES,
            True,
        ),
    ],
)
def test_aggregate_selector_cardinality_boundaries_accept_exact_shapes(
    cardinality: MssqlR1TransitionCardinalityV1,
    selector_kind: MssqlR1ResourceInstanceSelectorKindV1,
    allow_empty: bool,
) -> None:
    _validate_aggregate_selector_boundary(cardinality, selector_kind, allow_empty)


@pytest.mark.parametrize(
    ("cardinality", "selector_kind", "allow_empty", "declaration_has_key"),
    [
        (
            MssqlR1TransitionCardinalityV1.ZERO_OR_ONE,
            MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE,
            False,
            True,
        ),
        (
            MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET,
            MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE,
            False,
            True,
        ),
        (
            MssqlR1TransitionCardinalityV1.EXACT_REQUEST_SET,
            MssqlR1ResourceInstanceSelectorKindV1.ORDERED_VALUES,
            False,
            False,
        ),
    ],
)
def test_aggregate_selector_cardinality_boundaries_reject_widening(
    cardinality: MssqlR1TransitionCardinalityV1,
    selector_kind: MssqlR1ResourceInstanceSelectorKindV1,
    allow_empty: bool,
    declaration_has_key: bool,
) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="selector shape|complete instance key"):
        _validate_aggregate_selector_boundary(
            cardinality,
            selector_kind,
            allow_empty,
            declaration_has_key=declaration_has_key,
        )


def _resource_for_kind(kind: MssqlR1ResourceKindV1) -> MssqlR1PhysicalResourceRefV1:
    if kind is MssqlR1ResourceKindV1.STATIC_OBJECT:
        return MssqlR1PhysicalResourceRefV1(kind, "dpone_authority", "synthetic", None)
    return MssqlR1PhysicalResourceRefV1(kind, None, None, f"synthetic_{kind.value}")


def test_resource_reference_union_and_access_matrix_are_closed() -> None:
    for resource_kind in MssqlR1ResourceKindV1:
        resource = _resource_for_kind(resource_kind)
        assert MssqlR1PhysicalResourceRefV1.from_canonical_bytes(resource.canonical_bytes) == resource
        if resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT:
            with pytest.raises(MssqlR1V3ContractError, match="coordinate shape"):
                MssqlR1PhysicalResourceRefV1(resource_kind, None, None, "synthetic")
        else:
            with pytest.raises(MssqlR1V3ContractError, match="coordinate shape|schema/object"):
                MssqlR1PhysicalResourceRefV1(resource_kind, "dpone_authority", "synthetic", None)

        for access_kind in MssqlR1AccessKindV1:
            arguments = (
                resource,
                resource if resource_kind is MssqlR1ResourceKindV1.STATIC_OBJECT else None,
                (),
                (access_kind,),
                None,
                0,
                MssqlR1LockCardinalityV1.ONE,
            )
            if access_kind.value in ACCESS_MATRIX[resource_kind]:
                declaration = MssqlR1PhysicalResourceDeclarationV1(*arguments)
                assert declaration.ordered_allowed_access_kinds == (access_kind,)
            else:
                with pytest.raises(MssqlR1V3ContractError, match="widens"):
                    MssqlR1PhysicalResourceDeclarationV1(*arguments)


def test_request_binding_union_branches_are_distinct_and_exact() -> None:
    payload = _request()
    sealed = _binding(MssqlR1BindingModuleKindV1.BATCH_MUTATE).execution_semantics.request_authority
    assert {payload.binding_kind, sealed.binding_kind} == set(MssqlR1RequestBindingKindV1)
    assert payload.canonical_bytes != sealed.canonical_bytes
    for authority in (payload, sealed):
        assert MssqlR1RequestAuthorityV1.from_canonical_bytes(authority.canonical_bytes) == authority


def _integer_coordinate(
    source: MssqlR1ComparisonSourceV1,
    field_name: str,
    resource: MssqlR1PhysicalResourceRefV1 | None = None,
    *,
    nullable: bool = False,
) -> MssqlR1ComparisonCoordinateV1:
    return MssqlR1ComparisonCoordinateV1(
        source,
        resource,
        field_name,
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        nullable,
    )


def test_comparison_operator_matrix_has_exact_operand_shapes() -> None:
    left = _integer_coordinate(MssqlR1ComparisonSourceV1.RESULT_COLUMN, "revision")
    right = _integer_coordinate(MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER, "expected_revision")
    nullable = replace(left, nullable=True)
    existence = MssqlR1ResourceExistenceOperandV1(
        _resource(TABLE_NAMES[0]),
        MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False),
    )
    pairs = (
        MssqlR1ComparisonPairV1(left, MssqlR1ComparisonOperatorV1.EQUAL, right),
        MssqlR1ComparisonPairV1(left, MssqlR1ComparisonOperatorV1.NOT_EQUAL, right),
        MssqlR1ComparisonPairV1(left, MssqlR1ComparisonOperatorV1.ADJACENT, right),
        MssqlR1ComparisonPairV1(nullable, MssqlR1ComparisonOperatorV1.IS_NULL, None),
        MssqlR1ComparisonPairV1(nullable, MssqlR1ComparisonOperatorV1.IS_NOT_NULL, None),
        MssqlR1ComparisonPairV1(existence, MssqlR1ComparisonOperatorV1.ROW_EXISTS, None),
        MssqlR1ComparisonPairV1(existence, MssqlR1ComparisonOperatorV1.ROW_ABSENT, None),
    )
    assert {pair.operator for pair in pairs} == set(MssqlR1ComparisonOperatorV1)
    assert all(MssqlR1ComparisonPairV1.from_canonical_bytes(pair.canonical_bytes) == pair for pair in pairs)


def _revision_rule_matrix() -> tuple[MssqlR1RevisionRuleV1, ...]:
    owner = _resource(TABLE_NAMES[0])
    current = _integer_coordinate(MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD, "revision", owner)
    candidate = replace(current, source=MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD)
    expected = _integer_coordinate(MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER, "expected_revision")
    requested = MssqlR1ComparisonLiteralV1(MssqlR1ProjectionScalarKindV1.INTEGER, 2)
    return (
        MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.CREATE_ONE, None, None, None, candidate, 1),
        MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.EQUAL_REQUEST, current, expected, None, candidate, 0),
        MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.ADJACENT, current, None, None, candidate, 1),
        MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.UNCHANGED, current, None, None, candidate, 0),
        MssqlR1RevisionRuleV1(
            MssqlR1RevisionRuleKindV1.NULLABLE_INITIAL, replace(current, nullable=True), None, None, candidate, 1
        ),
        MssqlR1RevisionRuleV1(MssqlR1RevisionRuleKindV1.SET_CANDIDATE, current, expected, requested, candidate, 0),
    )


def test_revision_rule_matrix_round_trips_every_equation() -> None:
    rules = _revision_rule_matrix()
    assert {rule.rule_kind for rule in rules} == set(MssqlR1RevisionRuleKindV1)
    assert all(MssqlR1RevisionRuleV1.from_canonical_bytes(rule.canonical_bytes) == rule for rule in rules)


def test_transition_kind_matrix_round_trips_exact_shapes() -> None:
    owner = _resource(TABLE_NAMES[0])
    selector = MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False)
    applicability = (MssqlR1TransitionApplicabilityV1("found", MssqlR1ExecutionPathV1.FRESH_MUTATION),)
    rules = _revision_rule_matrix()
    transitions = (
        MssqlR1StateTransitionV1(
            1,
            MssqlR1TransitionKindV1.CREATE,
            owner,
            selector,
            MssqlR1TransitionCardinalityV1.ONE,
            None,
            (rules[0],),
            applicability,
        ),
        _supplemental_instances()[19],
        MssqlR1StateTransitionV1(
            1,
            MssqlR1TransitionKindV1.APPEND,
            owner,
            selector,
            MssqlR1TransitionCardinalityV1.ONE_OR_MORE,
            None,
            (),
            applicability,
        ),
    )
    assert all(isinstance(item, MssqlR1StateTransitionV1) for item in transitions)
    assert {item.transition_kind for item in transitions} == set(MssqlR1TransitionKindV1)
    assert all(MssqlR1StateTransitionV1.from_canonical_bytes(item.canonical_bytes) == item for item in transitions)


def test_replay_formula_matrix_round_trips_boolean_and_comparator_branches() -> None:
    pair = _policy().ordered_groups[0].ordered_clauses[0].ordered_comparisons[0]
    owner = _resource(TABLE_NAMES[0])
    clauses = (
        MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.EXACT_REQUEST, (pair,), None, False),
        MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.EXACT_PROJECTION, (pair,), None, False),
        MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF, (pair,), owner, False),
    )
    assert {clause.comparator for clause in clauses} == set(MssqlR1ReplayComparatorV1)
    for operator in MssqlR1ReplayBooleanOperatorV1:
        group = MssqlR1ReplayClauseGroupV1(1, operator, (clauses[0],))
        policy = MssqlR1ReplayOutcomePolicyV1("found", MssqlR1ExecutionPathV1.READ_ONLY_PROBE, operator, (group,))
        assert MssqlR1ReplayOutcomePolicyV1.from_canonical_bytes(policy.canonical_bytes) == policy


def test_null_comparison_union_has_exact_unary_shape() -> None:
    nullable = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.RESULT_COLUMN,
        None,
        "optional_value",
        MssqlR1ProjectionScalarKindV1.TEXT,
        MssqlR1ValueCardinalityV1.SCALAR,
        True,
    )
    assert MssqlR1ComparisonPairV1(nullable, MssqlR1ComparisonOperatorV1.IS_NULL, None).right is None
    with pytest.raises(MssqlR1V3ContractError, match="NULL comparison"):
        MssqlR1ComparisonPairV1(
            nullable,
            MssqlR1ComparisonOperatorV1.IS_NOT_NULL,
            MssqlR1ComparisonLiteralV1(MssqlR1ProjectionScalarKindV1.TEXT, "present"),
        )
    with pytest.raises(MssqlR1V3ContractError, match="binary comparison"):
        MssqlR1ComparisonPairV1(nullable, MssqlR1ComparisonOperatorV1.EQUAL, None)


def test_resource_access_and_identity_duplicates_fail_closed() -> None:
    declaration = descriptor().ordered_resource_declarations[0]
    access = declaration.ordered_allowed_access_kinds[0]
    with pytest.raises(MssqlR1V3ContractError, match="canonical order"):
        replace(declaration, ordered_allowed_access_kinds=(access, access))
    probe = descriptor().ordered_migration_probes[0]
    with pytest.raises(MssqlR1V3ContractError, match="migration probe"):
        replace(descriptor(), ordered_migration_probes=(probe, probe))


def test_casefold_ordering_and_access_widening_mutations_fail_closed() -> None:
    value = descriptor()
    declaration = next(
        item for item in value.ordered_resource_declarations if item.resource == _resource(TABLE_NAMES[0])
    )
    state, revision = declaration.ordered_fields
    with pytest.raises(MssqlR1V3ContractError, match="case-fold collision"):
        replace(declaration, ordered_fields=(state, replace(revision, name=state.name.upper())))
    with pytest.raises(MssqlR1V3ContractError, match="canonical order"):
        replace(declaration, ordered_allowed_access_kinds=tuple(reversed(declaration.ordered_allowed_access_kinds)))
    catalog = next(
        item
        for item in value.ordered_resource_declarations
        if item.resource.resource_kind is MssqlR1ResourceKindV1.CATALOG
    )
    with pytest.raises(MssqlR1V3ContractError, match="widens"):
        replace(
            catalog,
            ordered_allowed_access_kinds=(MssqlR1AccessKindV1.READ, MssqlR1AccessKindV1.UPDATE),
        )


@pytest.mark.parametrize(
    "source",
    [
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        MssqlR1ComparisonSourceV1.RESULT_COLUMN,
        MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
        MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
    ],
)
def test_scalar_parameter_binding_rejects_non_authoritative_sources(
    source: MssqlR1ComparisonSourceV1,
) -> None:
    coordinate = MssqlR1ComparisonCoordinateV1(
        source,
        _resource(TABLE_NAMES[0])
        if source
        in {
            MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
        }
        else None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    with pytest.raises(MssqlR1V3ContractError, match="binding source"):
        MssqlR1ScalarParameterBindingV1("request_digest", coordinate)


def test_session_binding_requires_exact_session_resource() -> None:
    coordinate = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.SESSION_BINDING,
        _resource(TABLE_NAMES[0]),
        "session_epoch",
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    with pytest.raises(MssqlR1V3ContractError, match="exact session resource"):
        MssqlR1ScalarParameterBindingV1("request_digest", coordinate)


def test_state_change_requires_current_resource_field() -> None:
    valid = _supplemental_instances()[17]
    assert isinstance(valid, MssqlR1StateChangeV1)
    invalid = replace(valid.state_field, source=MssqlR1ComparisonSourceV1.RESULT_COLUMN, resource=None)
    with pytest.raises(MssqlR1V3ContractError, match="state field"):
        replace(valid, state_field=invalid)


def test_fixed_result_requires_outcome_and_policy_coverage() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[0]
    empty = replace(procedure.execution_semantics, ordered_outcome_variants=())
    with pytest.raises(MssqlR1V3ContractError, match="nonempty unique outcome"):
        empty.validate_contract(
            procedure.portable_object.ordered_parameters,
            procedure.portable_object.result_contract,
            value._declarations(),
            value.expected_schema_contract.ordered_supported_codecs,
        )


@pytest.mark.parametrize(
    "authority",
    [
        replace(_request(), request_payload_parameter="request_digest"),
        replace(_request(), projection_parameter="request_digest"),
        replace(_request(), projection_parameter="request_payload"),
    ],
)
def test_request_roles_cannot_alias_one_portable_parameter(authority: MssqlR1RequestAuthorityV1) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="distinct parameter authorities"):
        validate_request_authority(authority, _params(), (CODEC,))


def test_portable_parameter_coordinates_reject_casefold_collisions() -> None:
    source = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.REQUEST_FIELD,
        None,
        "effect_key",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    authority = replace(
        _request(),
        ordered_scalar_parameter_bindings=(MssqlR1ScalarParameterBindingV1("REQUEST_DIGEST", source),),
    )
    duplicate = MssqlR1SchemaProcedureParameterV3(
        4, "REQUEST_DIGEST", "binary", 32, 0, 0, MssqlR1ParameterDirectionV3.INPUT
    )
    with pytest.raises(MssqlR1V3ContractError, match="parameter.*case-fold collision"):
        validate_request_authority(authority, (*_params(), duplicate), (CODEC,))


def test_portable_result_coordinates_reject_casefold_collisions() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[0]
    result = procedure.portable_object.result_contract
    assert isinstance(result, MssqlR1FixedResultV3)
    duplicate = MssqlR1ResultColumnV3(6, "OUTCOME", "varchar", 32, 0, 0, False, "Latin1_General_100_BIN2")
    with pytest.raises(MssqlR1V3ContractError, match="result column.*case-fold collision"):
        procedure.execution_semantics.validate_contract(
            procedure.portable_object.ordered_parameters,
            replace(result, ordered_columns=(*result.ordered_columns, duplicate)),
            value._declarations(),
            value.expected_schema_contract.ordered_supported_codecs,
        )


def test_portable_table_columns_reject_casefold_collisions() -> None:
    table = descriptor().ordered_tables[0]
    column = table.portable_object.ordered_columns[0]
    duplicate = replace(column, ordinal=2, name=column.name.upper())
    portable = replace(table.portable_object, ordered_columns=(column, duplicate))
    with pytest.raises(MssqlR1V3ContractError, match="table column.*case-fold collision"):
        replace(table, portable_object=portable)


def test_stage_scan_suffix_columns_reject_casefold_collisions() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[PROCEDURE_NAMES.index("dpone_scan_stage_v3")]
    result = procedure.portable_object.result_contract
    assert isinstance(result, MssqlR1StageScanTemplateV3)
    column = result.ordered_fixed_suffix_columns[0]
    duplicate = replace(column, ordinal=2, name=column.name.upper())
    invalid = replace(result, ordered_fixed_suffix_columns=(column, duplicate))
    with pytest.raises(MssqlR1V3ContractError, match="stage-scan result column.*case-fold collision"):
        procedure.execution_semantics.validate_contract(
            procedure.portable_object.ordered_parameters,
            invalid,
            value._declarations(),
            value.expected_schema_contract.ordered_supported_codecs,
        )


def test_fresh_coherent_proof_requires_aggregate_row_identity_predicate() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[1]
    semantics = procedure.execution_semantics
    policy = semantics.ordered_replay_outcome_policies[0]
    group = policy.ordered_groups[0]
    clause = group.ordered_clauses[0]
    invalid_clause = replace(
        clause,
        comparator=MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF,
        fresh_proof_resource=_resource(TABLE_NAMES[0]),
    )
    invalid_policy = replace(policy, ordered_groups=(replace(group, ordered_clauses=(invalid_clause,)),))
    with pytest.raises(MssqlR1V3ContractError, match="lacks an exact row identity predicate"):
        replace(semantics, ordered_replay_outcome_policies=(invalid_policy,)).validate_contract(
            procedure.portable_object.ordered_parameters,
            procedure.portable_object.result_contract,  # type: ignore[arg-type]
            value._declarations(),
            value.expected_schema_contract.ordered_supported_codecs,
        )


def test_stage_scan_cannot_hide_self_contained_transition_applicability() -> None:
    value = descriptor()
    scan_index = PROCEDURE_NAMES.index("dpone_scan_stage_v3")
    scan = value.ordered_procedures[scan_index]
    mutating = value.ordered_procedures[0].execution_semantics
    invalid = replace(
        mutating,
        ordered_outcome_variants=(),
        ordered_replay_outcome_policies=(),
        ordered_error_conditions=(),
    )
    procedures = list(value.ordered_procedures)
    procedures[scan_index] = replace(scan, execution_semantics=invalid)
    with pytest.raises(MssqlR1V3ContractError, match="stage scan.*read-only|transition applicability"):
        replace(value, ordered_procedures=tuple(procedures))


def test_undeclared_lock_and_malformed_candidate_raise_stable_contract_errors() -> None:
    lock = descriptor().ordered_procedures[0].execution_semantics.ordered_lock_steps[0]
    with pytest.raises(MssqlR1V3ContractError, match="undeclared resource"):
        require_lock_order((lock,), {})
    rule = descriptor().ordered_procedures[0].execution_semantics.ordered_state_transitions[0].ordered_revision_rules[0]
    parameter = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.INTEGER,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    with pytest.raises(MssqlR1V3ContractError, match="unsupported operand type"):
        replace(
            rule,
            rule_kind=MssqlR1RevisionRuleKindV1.SET_CANDIDATE,
            expected_value=parameter,
            requested_candidate=object(),
            delta=0,
        )


def _replace_policy_right(
    value: MssqlR1PhysicalSchemaDescriptorV1,
    procedure_index: int,
    right: MssqlR1ComparisonCoordinateV1,
) -> MssqlR1PhysicalSchemaDescriptorV1:
    procedure = value.ordered_procedures[procedure_index]
    semantics = procedure.execution_semantics
    policy = semantics.ordered_replay_outcome_policies[0]
    group = policy.ordered_groups[0]
    clause = group.ordered_clauses[0]
    pair = replace(clause.ordered_comparisons[0], right=right)
    clause = replace(clause, ordered_comparisons=(pair,))
    group = replace(group, ordered_clauses=(clause,))
    policy = replace(policy, ordered_groups=(group,))
    semantics = replace(semantics, ordered_replay_outcome_policies=(policy,))
    procedures = list(value.ordered_procedures)
    procedures[procedure_index] = replace(procedure, execution_semantics=semantics)
    return replace(value, ordered_procedures=tuple(procedures))


@pytest.mark.parametrize(
    "source",
    [
        MssqlR1ComparisonSourceV1.SESSION_BINDING,
        MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
        MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
    ],
)
def test_aggregate_rejects_source_sensitive_coordinate_without_exact_authority(
    source: MssqlR1ComparisonSourceV1,
) -> None:
    value = descriptor()
    invalid = MssqlR1ComparisonCoordinateV1(
        source,
        _resource("dpone_sealed_effect_v3"),
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    with pytest.raises(MssqlR1V3ContractError, match="session binding|transition owner"):
        _replace_policy_right(value, 1, invalid)


def test_aggregate_rejects_same_migration_probe_identity_with_distinct_body() -> None:
    value = descriptor()
    probe = value.ordered_migration_probes[0]
    distinct = replace(
        probe,
        observation_kind=MssqlR1MigrationObservationKindV1.EXACT_SCHEMA2,
        disposition=MssqlR1MigrationDispositionV1.COEXIST,
    )
    probes = tuple(sorted((probe, distinct), key=lambda item: item.canonical_bytes))
    with pytest.raises(MssqlR1V3ContractError, match="semantically unique"):
        replace(value, ordered_migration_probes=probes)


def _replace_binding_policy(
    value: MssqlR1PhysicalSchemaDescriptorV1,
    policy_index: int,
    policy: MssqlR1ReplayOutcomePolicyV1,
) -> MssqlR1PhysicalSchemaDescriptorV1:
    module = value.ordered_binding_module_templates[0]
    policies = list(module.execution_semantics.ordered_replay_outcome_policies)
    policies[policy_index] = policy
    semantics = replace(module.execution_semantics, ordered_replay_outcome_policies=tuple(policies))
    modules = list(value.ordered_binding_module_templates)
    modules[0] = replace(module, execution_semantics=semantics)
    return replace(value, ordered_binding_module_templates=tuple(modules))


def _mutate_sealed_digest(mutation: str) -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    policy = value.ordered_binding_module_templates[0].execution_semantics.ordered_replay_outcome_policies[0]
    group = policy.ordered_groups[0]
    clause = group.ordered_clauses[0]
    pair = clause.ordered_comparisons[0]
    if mutation == "operator":
        pair = replace(pair, operator=MssqlR1ComparisonOperatorV1.NOT_EQUAL)
        group = replace(group, ordered_clauses=(replace(clause, ordered_comparisons=(pair,)),))
    elif mutation == "parameter":
        left = replace(pair.left, field_name="wrong_digest")
        pair = replace(pair, left=left)
        group = replace(group, ordered_clauses=(replace(clause, ordered_comparisons=(pair,)),))
    else:
        other = MssqlR1ComparisonPairV1(pair.left, MssqlR1ComparisonOperatorV1.EQUAL, pair.left)
        second = MssqlR1ReplayClauseV1(2, MssqlR1ReplayComparatorV1.EXACT_REQUEST, (other,), None, False)
        group = replace(group, boolean_operator=MssqlR1ReplayBooleanOperatorV1.ANY, ordered_clauses=(clause, second))
    policy = replace(policy, ordered_groups=(group,))
    return _replace_binding_policy(value, 0, policy)


@pytest.mark.parametrize("mutation", ["operator", "parameter", "dominance"])
def test_sealed_digest_mutations_fail_closed(mutation: str) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="digest|portable authority"):
        _mutate_sealed_digest(mutation)


def test_duplicate_distinct_revision_candidate_and_foreign_schema_fail_closed() -> None:
    value = descriptor()
    transition = value.ordered_procedures[0].execution_semantics.ordered_state_transitions[0]
    rule = transition.ordered_revision_rules[0]
    distinct = replace(rule, rule_kind=MssqlR1RevisionRuleKindV1.UNCHANGED, delta=0)
    with pytest.raises(MssqlR1V3ContractError, match="candidate coordinates"):
        replace(
            transition, ordered_revision_rules=tuple(sorted((rule, distinct), key=lambda item: item.canonical_bytes))
        )
    table = value.ordered_tables[0]
    foreign = replace(table.portable_object, schema_name="foreign_schema")
    with pytest.raises(MssqlR1V3ContractError, match="schema and name"):
        replace(value, ordered_tables=(replace(table, portable_object=foreign), *value.ordered_tables[1:]))


def _fresh_descendant_descriptor() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    procedure = value.ordered_procedures[0]
    semantics = procedure.execution_semantics
    receipt = _resource("dpone_effect_receipt_v3")
    selector = MssqlR1ResourceInstanceSelectorV1(MssqlR1ResourceInstanceSelectorKindV1.DECLARED_SINGLETON, (), False)
    existence = MssqlR1ResourceExistenceOperandV1(receipt, selector)
    row = MssqlR1ComparisonPairV1(existence, MssqlR1ComparisonOperatorV1.ROW_EXISTS, None)
    descendant = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.DESCENDANT_RECEIPT,
        receipt,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    parameter = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER,
        None,
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    equality = MssqlR1ComparisonPairV1(descendant, MssqlR1ComparisonOperatorV1.EQUAL, parameter)
    comparisons = tuple(sorted((row, equality), key=lambda item: item.canonical_bytes))
    clause = MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF, comparisons, receipt, True)
    group = MssqlR1ReplayClauseGroupV1(1, MssqlR1ReplayBooleanOperatorV1.ALL, (clause,))
    fresh = MssqlR1ReplayOutcomePolicyV1(
        "found", MssqlR1ExecutionPathV1.FRESH_MUTATION, MssqlR1ReplayBooleanOperatorV1.ALL, (group,)
    )
    lock = MssqlR1PhysicalLockStepV1(
        1,
        MssqlR1LockKindV1.PHYSICAL,
        receipt,
        MssqlR1LockMechanismV1.GUARDED_ROW,
        MssqlR1LockActionV1.ACQUIRE,
        MssqlR1LockModeV1.UPDATE,
        MssqlR1LockOwnerV1.TRANSACTION,
        MssqlR1LockCardinalityV1.ONE,
        selector,
        MssqlR1LockTimeoutPolicyV1.BOUNDED_ENVIRONMENT,
        True,
        True,
        False,
    )
    locks = tuple(
        replace(item, ordinal=index)
        for index, item in enumerate(
            sorted((*semantics.ordered_lock_steps, lock), key=lambda item: item.resource.canonical_bytes), 1
        )
    )
    reads = tuple(
        sorted(
            (*semantics.ordered_read_set, MssqlR1PhysicalResourceAccessV1(receipt, MssqlR1AccessKindV1.READ)),
            key=lambda item: item.canonical_bytes,
        )
    )
    policies = (fresh, semantics.ordered_replay_outcome_policies[1])
    changed = replace(
        semantics, ordered_lock_steps=locks, ordered_read_set=reads, ordered_replay_outcome_policies=policies
    )
    procedures = (replace(procedure, execution_semantics=changed), *value.ordered_procedures[1:])
    return replace(value, ordered_procedures=procedures)


def _mutate_descendant_proof(mutation: str) -> MssqlR1PhysicalSchemaDescriptorV1:
    value = _fresh_descendant_descriptor()
    procedure = value.ordered_procedures[0]
    semantics = procedure.execution_semantics
    fresh = semantics.ordered_replay_outcome_policies[0]
    clause = fresh.ordered_groups[0].ordered_clauses[0]
    receipt = _resource("dpone_effect_receipt_v3")
    if mutation == "lock":
        retained = tuple(item for item in semantics.ordered_lock_steps if item.resource != receipt)
        changed = replace(
            semantics,
            ordered_lock_steps=tuple(replace(item, ordinal=index) for index, item in enumerate(retained, 1)),
        )
    elif mutation == "read":
        changed = replace(
            semantics,
            ordered_read_set=tuple(item for item in semantics.ordered_read_set if item.resource != receipt),
        )
    else:
        row = next(
            item for item in clause.ordered_comparisons if isinstance(item.left, MssqlR1ResourceExistenceOperandV1)
        )
        operand = row.left
        assert isinstance(operand, MssqlR1ResourceExistenceOperandV1)
        if mutation == "row_absent":
            replacement = replace(row, operator=MssqlR1ComparisonOperatorV1.ROW_ABSENT)
        elif mutation == "resource":
            replacement = replace(row, left=replace(operand, resource=_resource("dpone_control_receipt_v3")))
        else:
            selector = replace(operand.instance_selector, allow_empty=True)
            replacement = replace(row, left=replace(operand, instance_selector=selector))
        comparisons = tuple(replacement if item is row else item for item in clause.ordered_comparisons)
        clause = replace(clause, ordered_comparisons=tuple(sorted(comparisons, key=lambda item: item.canonical_bytes)))
        group = replace(fresh.ordered_groups[0], ordered_clauses=(clause,))
        policies = (replace(fresh, ordered_groups=(group,)), semantics.ordered_replay_outcome_policies[1])
        changed = replace(semantics, ordered_replay_outcome_policies=policies)
    procedures = (replace(procedure, execution_semantics=changed), *value.ordered_procedures[1:])
    return replace(value, ordered_procedures=procedures)


@pytest.mark.parametrize("mutation", ["row_absent", "resource", "selector", "lock", "read"])
def test_descendant_fresh_proof_mutations_fail_closed(mutation: str) -> None:
    with pytest.raises(
        MssqlR1V3ContractError,
        match="lock selector|read edge|identity predicate|selector|row-exists|descendant receipt|noncanonical",
    ):
        _mutate_descendant_proof(mutation)


def _add_valid_migration_probe(value: MssqlR1PhysicalSchemaDescriptorV1) -> MssqlR1PhysicalSchemaDescriptorV1:
    probe = replace(
        value.ordered_migration_probes[0],
        probe_id="exact_schema2",
        observation_kind=MssqlR1MigrationObservationKindV1.EXACT_SCHEMA2,
        disposition=MssqlR1MigrationDispositionV1.COEXIST,
    )
    return replace(
        value,
        ordered_migration_probes=tuple(
            sorted((*value.ordered_migration_probes, probe), key=lambda item: item.canonical_bytes)
        ),
    )


def _change_valid_error_state(value: MssqlR1PhysicalSchemaDescriptorV1) -> MssqlR1PhysicalSchemaDescriptorV1:
    module = value.ordered_binding_module_templates[0]
    semantics = module.execution_semantics
    changed_error = replace(semantics.ordered_error_conditions[0], error_state=2)
    errors = tuple(
        sorted((changed_error, *semantics.ordered_error_conditions[1:]), key=lambda item: item.canonical_bytes)
    )
    modules = (
        replace(module, execution_semantics=replace(semantics, ordered_error_conditions=errors)),
        *value.ordered_binding_module_templates[1:],
    )
    return replace(value, ordered_binding_module_templates=modules)


@pytest.mark.parametrize("mutation", [_add_valid_migration_probe, _change_valid_error_state])
def test_valid_distinct_semantic_mutations_change_aggregate_digest(mutation: object) -> None:
    value = descriptor()
    changed = mutation(value)  # type: ignore[operator]
    assert changed.digest != value.digest
    assert MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(changed.canonical_bytes) == changed


MutationExpectation = Literal["reject", "digest_change"]
MutationFactory = Callable[[], object]
MutationCase = tuple[str, MutationFactory, MutationExpectation]


def _receipt_portable_mutation(
    mutation: Callable[[MssqlR1PortableSchemaObjectV3], MssqlR1PortableSchemaObjectV3],
) -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    receipt = value.ordered_tables[TABLE_NAMES.index(RECEIPT_TABLE_NAME)].portable_object
    return _replace_receipt_portable(value, mutation(receipt))


def _receipt_declaration_mutation(
    mutation: Callable[[MssqlR1PhysicalResourceDeclarationV1], MssqlR1PhysicalResourceDeclarationV1],
) -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    declarations = list(value.ordered_resource_declarations)
    index = next(index for index, item in enumerate(declarations) if item.resource == _resource(RECEIPT_TABLE_NAME))
    declarations[index] = mutation(declarations[index])
    declarations.sort(key=lambda item: item.canonical_bytes)
    return replace(value, ordered_resource_declarations=tuple(declarations))


def _receipt_wrapper_mutation(
    *,
    lifecycle: MssqlR1TableLifecycleV1 | None = None,
    mutation_policy: MssqlR1MutationPolicyV1 | None = None,
) -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    table = value.ordered_tables[index]
    replacement = replace(
        table,
        lifecycle=table.lifecycle if lifecycle is None else lifecycle,
        mutation_policy=table.mutation_policy if mutation_policy is None else mutation_policy,
    )
    tables = list(value.ordered_tables)
    tables[index] = replacement
    return replace(value, ordered_tables=tuple(tables))


def _remove_receipt_table() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    return replace(
        value,
        ordered_tables=tuple(
            table for table in value.ordered_tables if table.portable_object.object_name != RECEIPT_TABLE_NAME
        ),
    )


def _reorder_receipt_table() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    tables = list(value.ordered_tables)
    index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    tables[index - 1], tables[index] = tables[index], tables[index - 1]
    return replace(value, ordered_tables=tuple(tables))


def _old_descriptor_version() -> MssqlR1PhysicalSchemaDescriptorV1:
    values = list(decode_canonical_bytes(descriptor().canonical_bytes, DOMAIN, field_count=9))
    values[0] = "dpone-mssql-r1-v3-physical-schema-2"
    return MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(canonical_bytes(DOMAIN, tuple(values)))


def _descriptor_version_subclass() -> MssqlR1PhysicalSchemaDescriptorV1:
    class VersionSubclass(str):
        pass

    return replace(descriptor(), descriptor_version=VersionSubclass(VERSION))


def _receipt_column_type_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_portable_mutation(
        lambda table: replace(
            table,
            ordered_columns=(
                table.ordered_columns[0],
                replace(table.ordered_columns[1], sql_type="varbinary"),
                *table.ordered_columns[2:],
            ),
        )
    )


def _receipt_column_removal_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_portable_mutation(lambda table: replace(table, ordered_columns=table.ordered_columns[:-1]))


def _receipt_column_nullability_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_portable_mutation(
        lambda table: replace(
            table,
            ordered_columns=(replace(table.ordered_columns[0], nullable=True), *table.ordered_columns[1:]),
        )
    )


def _receipt_constraint_coverage_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_portable_mutation(lambda table: replace(table, ordered_constraints=table.ordered_constraints[:-1]))


def _receipt_constraint_columns_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    def mutate(table: MssqlR1PortableSchemaObjectV3) -> MssqlR1PortableSchemaObjectV3:
        constraint = table.ordered_constraints[0]
        changed = replace(constraint, ordered_columns=("request_digest",))
        return replace(
            table,
            ordered_constraints=tuple(
                sorted((changed, *table.ordered_constraints[1:]), key=lambda item: item.canonical_bytes)
            ),
        )

    return _receipt_portable_mutation(mutate)


def _receipt_constraint_flag_mutation(flag: str) -> MssqlR1PhysicalSchemaDescriptorV1:
    def mutate(table: MssqlR1PortableSchemaObjectV3) -> MssqlR1PortableSchemaObjectV3:
        constraint = table.ordered_constraints[0]
        changed = replace(constraint, **{flag: False})
        return replace(
            table,
            ordered_constraints=tuple(
                sorted((changed, *table.ordered_constraints[1:]), key=lambda item: item.canonical_bytes)
            ),
        )

    return _receipt_portable_mutation(mutate)


def _receipt_index_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    index = MssqlR1SchemaIndexV3(
        "synthetic_provider_receipt_index",
        False,
        False,
        (MssqlR1SchemaIndexKeyV3("payload_digest", MssqlR1IndexDirectionV3.ASC),),
        (),
        None,
        True,
    )
    return _receipt_portable_mutation(lambda table: replace(table, ordered_indexes=(index,)))


def _receipt_trigger_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    trigger = MssqlR1PortableTriggerV3(
        "dpone_authority",
        "synthetic_provider_receipt_trigger",
        "dpone_authority",
        RECEIPT_TABLE_NAME,
        "instead_of",
        ("update", "delete"),
        True,
        hashlib.sha256(b"synthetic provider receipt trigger").digest(),
        MssqlR1ModuleOptionsV3("caller", True, True, False, False, MssqlR1SignerProfileKindV3.NONE),
    )
    return _receipt_portable_mutation(lambda table: replace(table, ordered_triggers=(trigger,)))


def _receipt_extended_property_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    extended_property = MssqlR1ExtendedPropertyV3(
        "synthetic_provider_receipt_property",
        hashlib.sha256(b"synthetic provider receipt property").digest(),
    )
    return _receipt_portable_mutation(lambda table: replace(table, ordered_extended_properties=(extended_property,)))


def _receipt_comparison_field_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_declaration_mutation(
        lambda declaration: replace(declaration, ordered_fields=declaration.ordered_fields[:-1])
    )


def _receipt_instance_key_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_declaration_mutation(
        lambda declaration: replace(
            declaration,
            ordered_fields=(
                replace(declaration.ordered_fields[0], instance_key_ordinal=None),
                *declaration.ordered_fields[1:],
            ),
        )
    )


def _receipt_access_widening(access: MssqlR1AccessKindV1) -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_declaration_mutation(
        lambda declaration: replace(
            declaration,
            ordered_allowed_access_kinds=tuple(
                sorted((*declaration.ordered_allowed_access_kinds, access), key=lambda item: item.value)
            ),
        )
    )


def _receipt_declaration_lock_mutation(
    lock_kind: MssqlR1LockKindV1,
    lock_subrank: int,
    lock_cardinality: MssqlR1LockCardinalityV1,
) -> MssqlR1PhysicalSchemaDescriptorV1:
    return _receipt_declaration_mutation(
        lambda declaration: replace(
            declaration,
            allowed_lock_kind=lock_kind,
            lock_subrank=lock_subrank,
            lock_cardinality=lock_cardinality,
        )
    )


def _receipt_resource_kind_laundering() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    receipt = next(
        item for item in value.ordered_resource_declarations if item.resource == _resource(RECEIPT_TABLE_NAME)
    )
    laundering = replace(
        receipt,
        resource=MssqlR1PhysicalResourceRefV1(
            MssqlR1ResourceKindV1.DYNAMIC_STAGE,
            None,
            None,
            "provider_install_receipt_laundered",
        ),
    )
    declarations = tuple(
        sorted((*value.ordered_resource_declarations, laundering), key=lambda item: item.canonical_bytes)
    )
    return replace(value, ordered_resource_declarations=declarations)


def _receipt_declaration_removal() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    return replace(
        value,
        ordered_resource_declarations=tuple(
            item for item in value.ordered_resource_declarations if item.resource != _resource(RECEIPT_TABLE_NAME)
        ),
    )


def _receipt_lock_resource_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    tables = list(value.ordered_tables)
    tables[index] = replace(tables[index], lock_resource=_resource(TABLE_NAMES[0]))
    return replace(value, ordered_tables=tuple(tables))


def _receipt_constraint_metadata_mutation(attribute: str) -> MssqlR1PhysicalSchemaDescriptorV1:
    def mutate(table: MssqlR1PortableSchemaObjectV3) -> MssqlR1PortableSchemaObjectV3:
        constraint = table.ordered_constraints[0]
        changed = replace(
            constraint,
            **{
                attribute: "synthetic_distinct_constraint_name"
                if attribute == "name"
                else hashlib.sha256(b"synthetic distinct constraint digest").digest()
            },
        )
        constraints = tuple(sorted((changed, *table.ordered_constraints[1:]), key=lambda item: item.canonical_bytes))
        return replace(table, ordered_constraints=constraints)

    return _receipt_portable_mutation(mutate)


def _receipt_table_definition_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    index = TABLE_NAMES.index(RECEIPT_TABLE_NAME)
    table = value.ordered_tables[index]
    changed = replace(
        table,
        definition=MssqlR1DefinitionPayloadV1.create(
            MssqlR1DefinitionKindV1.TABLE_DDL,
            "synthetic distinct provider-install receipt definition\n",
        ),
    )
    tables = list(value.ordered_tables)
    tables[index] = changed
    return replace(value, ordered_tables=tuple(tables))


ProviderReceiptMutationExpectation = Literal["must_reject", "valid_distinct"]
ProviderReceiptMutationCase = tuple[str, MutationFactory, ProviderReceiptMutationExpectation]

PROVIDER_INSTALL_RECEIPT_MUTATION_REGISTRY: tuple[ProviderReceiptMutationCase, ...] = (
    ("table_removal", _remove_receipt_table, "must_reject"),
    ("inventory_reorder", _reorder_receipt_table, "must_reject"),
    ("old_descriptor_version", _old_descriptor_version, "must_reject"),
    ("descriptor_version_str_subclass", _descriptor_version_subclass, "must_reject"),
    ("receipt_column_removal", _receipt_column_removal_mutation, "must_reject"),
    ("receipt_column_type", _receipt_column_type_mutation, "must_reject"),
    ("receipt_column_nullability", _receipt_column_nullability_mutation, "must_reject"),
    ("constraint_kind_coverage", _receipt_constraint_coverage_mutation, "must_reject"),
    ("constraint_column_coverage", _receipt_constraint_columns_mutation, "must_reject"),
    ("constraint_trusted_false", lambda: _receipt_constraint_flag_mutation("trusted"), "must_reject"),
    ("constraint_enabled_false", lambda: _receipt_constraint_flag_mutation("enabled"), "must_reject"),
    ("additional_index", _receipt_index_mutation, "must_reject"),
    ("additional_trigger", _receipt_trigger_mutation, "must_reject"),
    ("additional_extended_property", _receipt_extended_property_mutation, "must_reject"),
    ("comparison_fields", _receipt_comparison_field_mutation, "must_reject"),
    ("instance_key", _receipt_instance_key_mutation, "must_reject"),
    (
        "lock_cardinality",
        lambda: _receipt_declaration_lock_mutation(
            MssqlR1LockKindV1.PHYSICAL,
            0,
            MssqlR1LockCardinalityV1.EXACT_REQUEST_SET,
        ),
        "must_reject",
    ),
    (
        "allowed_lock_kind_and_subrank",
        lambda: _receipt_declaration_lock_mutation(
            MssqlR1LockKindV1.ARTIFACT,
            1,
            MssqlR1LockCardinalityV1.ONE,
        ),
        "must_reject",
    ),
    ("static_declaration_removal", _receipt_declaration_removal, "must_reject"),
    ("table_lock_resource_rebinding", _receipt_lock_resource_mutation, "must_reject"),
    ("resource_kind_ref_laundering", _receipt_resource_kind_laundering, "must_reject"),
    (
        "delete_access_widening",
        lambda: _receipt_access_widening(MssqlR1AccessKindV1.DELETE),
        "must_reject",
    ),
    (
        "update_access_widening",
        lambda: _receipt_access_widening(MssqlR1AccessKindV1.UPDATE),
        "must_reject",
    ),
    (
        "lifecycle",
        lambda: _receipt_wrapper_mutation(lifecycle=MssqlR1TableLifecycleV1.APPEND_ONLY),
        "must_reject",
    ),
    (
        "mutation_policy",
        lambda: _receipt_wrapper_mutation(mutation_policy=MssqlR1MutationPolicyV1.GUARDED_PROCEDURE_ONLY),
        "must_reject",
    ),
    ("synthetic_constraint_name", lambda: _receipt_constraint_metadata_mutation("name"), "valid_distinct"),
    (
        "synthetic_constraint_definition_digest",
        lambda: _receipt_constraint_metadata_mutation("definition_digest"),
        "valid_distinct",
    ),
    ("synthetic_table_ddl", _receipt_table_definition_mutation, "valid_distinct"),
)


@pytest.mark.parametrize(
    ("case_id", "mutation_factory", "expected"),
    PROVIDER_INSTALL_RECEIPT_MUTATION_REGISTRY,
    ids=[case_id for case_id, _, _ in PROVIDER_INSTALL_RECEIPT_MUTATION_REGISTRY],
)
def test_provider_install_receipt_mutation_registry(
    case_id: str,
    mutation_factory: MutationFactory,
    expected: ProviderReceiptMutationExpectation,
) -> None:
    assert len({item[0] for item in PROVIDER_INSTALL_RECEIPT_MUTATION_REGISTRY}) == len(
        PROVIDER_INSTALL_RECEIPT_MUTATION_REGISTRY
    )
    if expected == "must_reject":
        messages = []
        for _ in range(2):
            with pytest.raises(MssqlR1V3ContractError) as raised:
                mutation_factory()
            messages.append(str(raised.value))
        assert messages[0] == messages[1], case_id
        return
    baseline = descriptor()
    changed = mutation_factory()
    assert isinstance(changed, MssqlR1PhysicalSchemaDescriptorV1), case_id
    assert changed.digest != baseline.digest
    assert MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(changed.canonical_bytes) == changed


def _parameter_casefold_mutation() -> None:
    source = MssqlR1ComparisonCoordinateV1(
        MssqlR1ComparisonSourceV1.REQUEST_FIELD,
        None,
        "effect_key",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    authority = replace(
        _request(),
        ordered_scalar_parameter_bindings=(MssqlR1ScalarParameterBindingV1("REQUEST_DIGEST", source),),
    )
    duplicate = MssqlR1SchemaProcedureParameterV3(
        4, "REQUEST_DIGEST", "binary", 32, 0, 0, MssqlR1ParameterDirectionV3.INPUT
    )
    validate_request_authority(authority, (*_params(), duplicate), (CODEC,))


def _result_casefold_mutation() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[0]
    result = procedure.portable_object.result_contract
    assert isinstance(result, MssqlR1FixedResultV3)
    duplicate = MssqlR1ResultColumnV3(6, "OUTCOME", "varchar", 32, 0, 0, False, "Latin1_General_100_BIN2")
    procedure.execution_semantics.validate_contract(
        procedure.portable_object.ordered_parameters,
        replace(result, ordered_columns=(*result.ordered_columns, duplicate)),
        value._declarations(),
        value.expected_schema_contract.ordered_supported_codecs,
    )


def _fresh_without_identity_mutation() -> None:
    value = descriptor()
    procedure = value.ordered_procedures[1]
    semantics = procedure.execution_semantics
    policy = semantics.ordered_replay_outcome_policies[0]
    group = policy.ordered_groups[0]
    clause = replace(
        group.ordered_clauses[0],
        comparator=MssqlR1ReplayComparatorV1.FRESH_COHERENT_PROOF,
        fresh_proof_resource=_resource(TABLE_NAMES[0]),
    )
    policy = replace(policy, ordered_groups=(replace(group, ordered_clauses=(clause,)),))
    replace(semantics, ordered_replay_outcome_policies=(policy,)).validate_contract(
        procedure.portable_object.ordered_parameters,
        procedure.portable_object.result_contract,  # type: ignore[arg-type]
        value._declarations(),
        value.expected_schema_contract.ordered_supported_codecs,
    )


def _scan_authority_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    index = PROCEDURE_NAMES.index("dpone_scan_stage_v3")
    scan = value.ordered_procedures[index]
    invalid = replace(
        value.ordered_procedures[0].execution_semantics,
        ordered_outcome_variants=(),
        ordered_replay_outcome_policies=(),
        ordered_error_conditions=(),
    )
    procedures = list(value.ordered_procedures)
    procedures[index] = replace(scan, execution_semantics=invalid)
    return replace(value, ordered_procedures=tuple(procedures))


def _source_scope_mutation(source: MssqlR1ComparisonSourceV1) -> MssqlR1PhysicalSchemaDescriptorV1:
    coordinate = MssqlR1ComparisonCoordinateV1(
        source,
        _resource("dpone_sealed_effect_v3"),
        "request_digest",
        MssqlR1ProjectionScalarKindV1.DIGEST,
        MssqlR1ValueCardinalityV1.SCALAR,
        False,
    )
    return _replace_policy_right(descriptor(), 1, coordinate)


def _duplicate_candidate_mutation() -> MssqlR1StateTransitionV1:
    transition = descriptor().ordered_procedures[0].execution_semantics.ordered_state_transitions[0]
    rule = transition.ordered_revision_rules[0]
    distinct = replace(rule, rule_kind=MssqlR1RevisionRuleKindV1.UNCHANGED, delta=0)
    return replace(
        transition, ordered_revision_rules=tuple(sorted((rule, distinct), key=lambda item: item.canonical_bytes))
    )


def _foreign_schema_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    table = value.ordered_tables[0]
    return replace(
        value,
        ordered_tables=(
            replace(table, portable_object=replace(table.portable_object, schema_name="foreign_schema")),
            *value.ordered_tables[1:],
        ),
    )


def _migration_identity_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    probe = value.ordered_migration_probes[0]
    distinct = replace(
        probe,
        observation_kind=MssqlR1MigrationObservationKindV1.EXACT_SCHEMA2,
        disposition=MssqlR1MigrationDispositionV1.COEXIST,
    )
    return replace(
        value, ordered_migration_probes=tuple(sorted((probe, distinct), key=lambda item: item.canonical_bytes))
    )


def _undeclared_lock_mutation() -> None:
    lock = descriptor().ordered_procedures[0].execution_semantics.ordered_lock_steps[0]
    require_lock_order((lock,), {})


def _malformed_candidate_mutation() -> MssqlR1RevisionRuleV1:
    rule = descriptor().ordered_procedures[0].execution_semantics.ordered_state_transitions[0].ordered_revision_rules[0]
    parameter = _integer_coordinate(MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER, "request_digest")
    return replace(
        rule,
        rule_kind=MssqlR1RevisionRuleKindV1.SET_CANDIDATE,
        expected_value=parameter,
        requested_candidate=object(),
        delta=0,
    )


def _reordered_inventory_mutation() -> MssqlR1PhysicalSchemaDescriptorV1:
    value = descriptor()
    return replace(value, ordered_tables=tuple(reversed(value.ordered_tables)))


def _resource_field_casefold_mutation() -> MssqlR1PhysicalResourceDeclarationV1:
    declaration = next(
        item for item in descriptor().ordered_resource_declarations if item.resource == _resource(TABLE_NAMES[0])
    )
    state, revision = declaration.ordered_fields
    return replace(declaration, ordered_fields=(state, replace(revision, name=state.name.upper())))


def _access_widening_mutation() -> MssqlR1PhysicalResourceDeclarationV1:
    declaration = next(
        item
        for item in descriptor().ordered_resource_declarations
        if item.resource.resource_kind is MssqlR1ResourceKindV1.CATALOG
    )
    return replace(
        declaration,
        ordered_allowed_access_kinds=(MssqlR1AccessKindV1.READ, MssqlR1AccessKindV1.UPDATE),
    )


def _nullable_equality_mutation() -> MssqlR1ReplayClauseV1:
    left = _integer_coordinate(MssqlR1ComparisonSourceV1.RESULT_COLUMN, "revision", nullable=True)
    right = _integer_coordinate(MssqlR1ComparisonSourceV1.PROCEDURE_PARAMETER, "expected_revision")
    pair = MssqlR1ComparisonPairV1(left, MssqlR1ComparisonOperatorV1.EQUAL, right)
    return MssqlR1ReplayClauseV1(1, MssqlR1ReplayComparatorV1.EXACT_REQUEST, (pair,), None, False)


def _invalid_selector_mutation(kind: MssqlR1ResourceInstanceSelectorKindV1) -> MssqlR1ResourceInstanceSelectorV1:
    scalar = _integer_coordinate(MssqlR1ComparisonSourceV1.REQUEST_FIELD, "key")
    coordinate = (
        replace(scalar, value_cardinality=MssqlR1ValueCardinalityV1.ORDERED_SET)
        if kind is MssqlR1ResourceInstanceSelectorKindV1.SINGLE_VALUE
        else scalar
    )
    return MssqlR1ResourceInstanceSelectorV1(kind, (coordinate,), False)


def _invalid_resource_mutation(kind: MssqlR1ResourceKindV1) -> MssqlR1PhysicalResourceRefV1:
    if kind is MssqlR1ResourceKindV1.STATIC_OBJECT:
        return MssqlR1PhysicalResourceRefV1(kind, None, None, "synthetic")
    return MssqlR1PhysicalResourceRefV1(kind, "dpone_authority", "synthetic", None)


def _invalid_access_mutation(
    kind: MssqlR1ResourceKindV1, access: MssqlR1AccessKindV1
) -> MssqlR1PhysicalResourceDeclarationV1:
    resource = _resource_for_kind(kind)
    return MssqlR1PhysicalResourceDeclarationV1(
        resource,
        resource if kind is MssqlR1ResourceKindV1.STATIC_OBJECT else None,
        (),
        (access,),
        None,
        0,
        MssqlR1LockCardinalityV1.ONE,
    )


BASE_MUTATION_CASES: tuple[MutationCase, ...] = (
    (
        "request_role_alias",
        lambda: validate_request_authority(
            replace(_request(), request_payload_parameter="request_digest"), _params(), (CODEC,)
        ),
        "reject",
    ),
    ("parameter_casefold_collision", _parameter_casefold_mutation, "reject"),
    ("result_casefold_collision", _result_casefold_mutation, "reject"),
    ("scan_mutation_authority", _scan_authority_mutation, "reject"),
    ("fresh_without_row_identity", _fresh_without_identity_mutation, "reject"),
    *(
        (f"sealed_digest_{name}", lambda name=name: _mutate_sealed_digest(name), "reject")
        for name in ("operator", "parameter", "dominance")
    ),
    *(
        (f"descendant_{name}", lambda name=name: _mutate_descendant_proof(name), "reject")
        for name in ("row_absent", "resource", "selector", "lock", "read")
    ),
    *(
        (f"source_scope_{source.value}", lambda source=source: _source_scope_mutation(source), "reject")
        for source in (
            MssqlR1ComparisonSourceV1.SESSION_BINDING,
            MssqlR1ComparisonSourceV1.PREDECESSOR_RESOURCE_FIELD,
            MssqlR1ComparisonSourceV1.CANDIDATE_RESOURCE_FIELD,
        )
    ),
    ("duplicate_revision_candidate", _duplicate_candidate_mutation, "reject"),
    ("foreign_inventory_schema", _foreign_schema_mutation, "reject"),
    ("migration_probe_identity_collision", _migration_identity_mutation, "reject"),
    ("undeclared_lock_resource", _undeclared_lock_mutation, "reject"),
    ("malformed_requested_candidate", _malformed_candidate_mutation, "reject"),
    ("inventory_reorder", _reordered_inventory_mutation, "reject"),
    ("resource_field_casefold_collision", _resource_field_casefold_mutation, "reject"),
    ("access_widening", _access_widening_mutation, "reject"),
    ("nullable_equality_without_guard", _nullable_equality_mutation, "reject"),
    ("additional_migration_probe", lambda: _add_valid_migration_probe(descriptor()), "digest_change"),
    ("changed_error_state", lambda: _change_valid_error_state(descriptor()), "digest_change"),
)
FIELD_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (
        f"field_type_{type(instance).__name__}_{field_name}",
        lambda instance=instance, field_name=field_name: replace(instance, **{field_name: object()}),
        "reject",
    )
    for instance in _supplemental_instances()
    for field_name in DESCRIPTOR_FIELD_REGISTRY[type(instance)]
)
ENUM_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (
        f"enum_raw_{type(instance).__name__}_{field.name}_{member.value}",
        lambda instance=instance, field=field, member=member: replace(instance, **{field.name: member.value}),
        "reject",
    )
    for instance in _supplemental_instances()
    for field in fields(instance)
    if type(getattr(instance, field.name)) in ENUM_BRANCH_REGISTRY
    for member in type(getattr(instance, field.name))
)
TUPLE_ENUM_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (
        f"enum_tuple_raw_{type(instance).__name__}_{field.name}_{member.value}",
        lambda instance=instance, field=field, member=member: replace(instance, **{field.name: (member.value,)}),
        "reject",
    )
    for instance in _supplemental_instances()
    for field in fields(instance)
    if type(getattr(instance, field.name)) is tuple
    and getattr(instance, field.name)
    and type(getattr(instance, field.name)[0]) in ENUM_BRANCH_REGISTRY
    for member in type(getattr(instance, field.name)[0])
)
LITERAL_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (
        f"literal_union_{kind.value}",
        lambda kind=kind, value=value: MssqlR1ComparisonLiteralV1(kind, value),
        "reject",
    )
    for kind, value in (
        (MssqlR1ProjectionScalarKindV1.TEXT, object()),
        (MssqlR1ProjectionScalarKindV1.BINARY, "bytes"),
        (MssqlR1ProjectionScalarKindV1.DIGEST, b"short"),
        (MssqlR1ProjectionScalarKindV1.UUID, "NOT-CANONICAL"),
        (MssqlR1ProjectionScalarKindV1.INTEGER, True),
        (MssqlR1ProjectionScalarKindV1.BOOLEAN, 1),
        (MssqlR1ProjectionScalarKindV1.UTC, "2026-01-01T00:00:00Z"),
    )
)
SELECTOR_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (f"selector_union_{kind.value}", lambda kind=kind: _invalid_selector_mutation(kind), "reject")
    for kind in MssqlR1ResourceInstanceSelectorKindV1
)
RESOURCE_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (f"resource_union_{kind.value}", lambda kind=kind: _invalid_resource_mutation(kind), "reject")
    for kind in MssqlR1ResourceKindV1
)
ACCESS_MUTATION_CASES: tuple[MutationCase, ...] = tuple(
    (
        f"access_matrix_{kind.value}_{access.value}",
        lambda kind=kind, access=access: _invalid_access_mutation(kind, access),
        "reject",
    )
    for kind in MssqlR1ResourceKindV1
    for access in MssqlR1AccessKindV1
    if access.value not in ACCESS_MATRIX[kind]
)
EXECUTABLE_MUTATION_REGISTRY = (
    BASE_MUTATION_CASES
    + FIELD_MUTATION_CASES
    + ENUM_MUTATION_CASES
    + TUPLE_ENUM_MUTATION_CASES
    + LITERAL_MUTATION_CASES
    + SELECTOR_MUTATION_CASES
    + RESOURCE_MUTATION_CASES
    + ACCESS_MUTATION_CASES
)


@pytest.mark.parametrize(
    ("case_id", "mutation_factory", "expected"),
    EXECUTABLE_MUTATION_REGISTRY,
    ids=[case_id for case_id, _, _ in EXECUTABLE_MUTATION_REGISTRY],
)
def test_executable_mutation_registry(
    case_id: str,
    mutation_factory: MutationFactory,
    expected: MutationExpectation,
) -> None:
    assert len({item[0] for item in EXECUTABLE_MUTATION_REGISTRY}) == len(EXECUTABLE_MUTATION_REGISTRY)
    if expected == "reject":
        messages = []
        for _ in range(2):
            with pytest.raises(MssqlR1V3ContractError) as raised:
                mutation_factory()
            messages.append(str(raised.value))
        assert messages[0] == messages[1], case_id
        return
    baseline = descriptor()
    changed = mutation_factory()
    assert isinstance(changed, MssqlR1PhysicalSchemaDescriptorV1), case_id
    assert changed.digest != baseline.digest
    assert MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(changed.canonical_bytes) == changed
