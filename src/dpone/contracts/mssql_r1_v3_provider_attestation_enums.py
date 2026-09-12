"""Closed enumerations for Provider Attestation Foundation V2."""

from __future__ import annotations

from dpone._compat import StrEnum


class MssqlR1MigrationStatementPhaseV1(StrEnum):
    PRE_DECISION_OBSERVE = "pre_decision_observe"
    POST_DECISION_RECEIPT_PRECHECK = "post_decision_receipt_precheck"
    POST_DECISION_MUTATE = "post_decision_mutate"
    POST_DECISION_ATTEST = "post_decision_attest"
    POST_DECISION_RECEIPT_APPEND = "post_decision_receipt_append"


class MssqlR1AttestationQueryKindV1(StrEnum):
    SCHEMA = "schema"
    TABLE = "table"
    MODULE = "module"
    CERTIFICATE = "certificate"
    PERMISSION = "permission"
    SIGNATURE = "signature"
    BINDING_PREFIX_INVENTORY = "binding_prefix_inventory"


class MssqlR1AttestationResultAuthorityKindV1(StrEnum):
    SCHEMA_INVENTORY = "schema_inventory"
    TABLE_INVENTORY = "table_inventory"
    MODULE_INVENTORY = "module_inventory"
    CERTIFICATE_INVENTORY = "certificate_inventory"
    PERMISSION_INVENTORY = "permission_inventory"
    SIGNATURE_INVENTORY = "signature_inventory"
    BINDING_PREFIX_INVENTORY = "binding_prefix_inventory"


class MssqlR1ProviderAttestationFailureReasonV2(StrEnum):
    ATTESTATION_DOMAIN_INVALID = "attestation_domain_invalid"
    ATTESTATION_BOUNDS_EXCEEDED = "attestation_bounds_exceeded"
    ATTESTATION_REGISTRY_MISMATCH = "attestation_registry_mismatch"
    ATTESTATION_RESULT_ARM_MISMATCH = "attestation_result_arm_mismatch"
    ATTESTATION_PROJECTION_MISMATCH = "attestation_projection_mismatch"
    ATTESTATION_AUTHORITY_SPLICE = "attestation_authority_splice"
    ATTESTATION_SCHEMA_INVENTORY_MISMATCH = "attestation_schema_inventory_mismatch"
    ATTESTATION_PERMISSION_CLOSURE_MISMATCH = "attestation_permission_closure_mismatch"
    ATTESTATION_CERTIFICATE_MISMATCH = "attestation_certificate_mismatch"
    ATTESTATION_BINDING_INVENTORY_MISMATCH = "attestation_binding_inventory_mismatch"
    ATTESTATION_TARGET_IDENTITY_MISMATCH = "attestation_target_identity_mismatch"


class MssqlR1ProviderAttestationRecoveryActionV2(StrEnum):
    REGENERATE_ATTESTATION_INPUT = "regenerate_attestation_input"
    REDUCE_OBSERVATION_SCOPE = "reduce_observation_scope"
    RECONCILE_RENDERER_REGISTRY = "reconcile_renderer_registry"
    REPAIR_OR_REBASELINE_PROVIDER = "repair_or_rebaseline_provider"
    SELECT_SUPPORTED_TARGET = "select_supported_target"


QUERY_KIND_ORDER = (
    MssqlR1AttestationQueryKindV1.SCHEMA,
    MssqlR1AttestationQueryKindV1.TABLE,
    MssqlR1AttestationQueryKindV1.MODULE,
    MssqlR1AttestationQueryKindV1.CERTIFICATE,
    MssqlR1AttestationQueryKindV1.PERMISSION,
    MssqlR1AttestationQueryKindV1.SIGNATURE,
    MssqlR1AttestationQueryKindV1.BINDING_PREFIX_INVENTORY,
)
RESULT_KIND_ORDER = (
    MssqlR1AttestationResultAuthorityKindV1.SCHEMA_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.TABLE_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.MODULE_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.CERTIFICATE_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.PERMISSION_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.SIGNATURE_INVENTORY,
    MssqlR1AttestationResultAuthorityKindV1.BINDING_PREFIX_INVENTORY,
)
QUERY_RESULT_PAIRS = tuple(zip(QUERY_KIND_ORDER, RESULT_KIND_ORDER, strict=True))

__all__ = (
    "MssqlR1AttestationQueryKindV1",
    "MssqlR1AttestationResultAuthorityKindV1",
    "MssqlR1MigrationStatementPhaseV1",
    "MssqlR1ProviderAttestationFailureReasonV2",
    "MssqlR1ProviderAttestationRecoveryActionV2",
    "QUERY_KIND_ORDER",
    "QUERY_RESULT_PAIRS",
    "RESULT_KIND_ORDER",
)
