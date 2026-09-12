"""Closed failures and allocation-free size policy for Binding V2."""

from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, NoReturn, cast
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_codec import decode_canonical_bytes
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1SubjectRoleV3

_MODEL_LENGTHS = {
    "MssqlR1AnyDatabaseRoleRefV1": (43, 0),
    "MssqlR1BindingModuleTemplateV1": (45, 0),
    "MssqlR1BindingSignerClassPrincipalRefV1": (46, 0),
    "MssqlR1BindingSignerLifecyclePolicyV2": (44, 0),
    "MssqlR1CertificateLifecycleTransitionV1": (54, 0),
    "MssqlR1ClosedTargetFeatureObservationV1": (52, 0),
    "MssqlR1ComparisonCoordinateV1": (43, 0),
    "MssqlR1ComparisonPairV1": (37, 0),
    "MssqlR1DefinitionPayloadV1": (32, 0),
    "MssqlR1EnvironmentPrincipalRefV1": (47, 0),
    "MssqlR1EphemeralSecretPolicyV1": (45, 0),
    "MssqlR1ExecutionSemanticsV1": (41, 0),
    "MssqlR1FixedResultV3": (41, 0),
    "MssqlR1ForbiddenRoleMembershipV1": (47, 0),
    "MssqlR1MigrationProbeV1": (37, 0),
    "MssqlR1ModuleOptionsV3": (43, 0),
    "MssqlR1OutcomeVariantV1": (37, 0),
    "MssqlR1PermissionClosurePolicyV1": (47, 0),
    "MssqlR1PermissionObservationPolicyV2": (51, 0),
    "MssqlR1PermissionRuleV3": (44, 0),
    "MssqlR1PhysicalEngineProfileV1": (36, 0),
    "MssqlR1PhysicalErrorConditionV1": (37, 0),
    "MssqlR1PhysicalLockStepV1": (31, 0),
    "MssqlR1PhysicalProcedureDescriptorV1": (42, 0),
    "MssqlR1PhysicalResourceAccessV1": (37, 0),
    "MssqlR1PhysicalResourceDeclarationV1": (42, 0),
    "MssqlR1PhysicalResourceRefV1": (34, 0),
    "MssqlR1PhysicalSchemaDescriptorV1": (39, 0),
    "MssqlR1PhysicalSessionProfileV1": (37, 0),
    "MssqlR1PhysicalTableDescriptorV1": (38, 0),
    "MssqlR1PortableSchemaObjectV3": (35, 0),
    "MssqlR1ProjectionFieldV1": (38, 0),
    "MssqlR1ProjectionGrammarV1": (40, 0),
    "MssqlR1RegisteredTargetCatalogV1": (44, 0),
    "MssqlR1RegisteredTargetColumnV1": (43, 0),
    "MssqlR1RegisteredTargetIndexV1": (42, 0),
    "MssqlR1ReplayClauseGroupV1": (41, 0),
    "MssqlR1ReplayClauseV1": (35, 0),
    "MssqlR1ReplayOutcomePolicyV1": (43, 0),
    "MssqlR1RequestAuthorityV1": (39, 0),
    "MssqlR1ResourceFieldV1": (36, 0),
    "MssqlR1ResourceInstanceSelectorV1": (48, 0),
    "MssqlR1ResultColumnV3": (42, 0),
    "MssqlR1RevisionRuleV1": (35, 0),
    "MssqlR1RotationStableTargetAuthorityV1": (51, 0),
    "MssqlR1SchemaColumnV3": (35, 0),
    "MssqlR1SchemaConstraintV3": (39, 0),
    "MssqlR1SchemaContractV3": (37, 0),
    "MssqlR1SchemaProcedureParameterV3": (48, 0),
    "MssqlR1SecretSqlTemplateV1": (41, 0),
    "MssqlR1SharedCertificateMetadataV1": (49, 0),
    "MssqlR1SharedInstallSecurityProfileV2": (44, 0),
    "MssqlR1SharedSignerInstallAuthorityV1": (53, 0),
    "MssqlR1SharedSignerPrincipalRefV1": (49, 0),
    "MssqlR1SharedSignerProfileRefV1": (47, 0),
    "MssqlR1SignerProfileV3": (43, 0),
    "MssqlR1StageScanTemplateV3": (44, 0),
    "MssqlR1StateChangeV1": (34, 0),
    "MssqlR1StateTransitionV1": (38, 0),
    "MssqlR1SupportedCodecEntryV3": (34, 0),
    "MssqlR1TransitionApplicabilityV1": (46, 0),
    "MssqlR1CanonicalTargetScalarShapeV1": (38, 0),
    "PostgresMssqlObservedSourceColumnV1": (47, 32),
    "PostgresMssqlSelectedRelationSchemaAuthorityV1": (59, 0),
    "PostgresMssqlSourceColumnRefV1": (42, 0),
    "PostgresMssqlSourceScalarShapeV1": (44, 0),
    "PostgresMssqlTypeDecisionAuthorityV1": (48, 0),
    "PostgresMssqlTypePolicyAuthorityV1": (46, 0),
    "PostgresMssqlValueAdmissionAuthorityV1": (50, 0),
}


def _utf8_length(value: str) -> int:
    total = 0
    for character in value:
        point = ord(character)
        if 0xD800 <= point <= 0xDFFF:
            raise ValueError("surrogate is not valid UTF-8")
        total += 1 if point <= 0x7F else 2 if point <= 0x7FF else 3 if point <= 0xFFFF else 4
    return total


def _encoded_length(value: object, ceiling: int) -> int:
    if value is None or type(value) is bool:
        return 1
    if isinstance(value, Enum):
        return _encoded_length(value.value, ceiling)
    if type(value) is UUID:
        return 17
    if type(value) is bytes:
        return min(9 + len(value), ceiling)
    if type(value) is str:
        return min(9 + _utf8_length(value), ceiling)
    if type(value) is int:
        return 9
    if type(value) is datetime:
        return _encoded_length(value.astimezone(timezone.utc).isoformat(), ceiling)  # noqa: UP017
    if type(value) is tuple:
        total = 1
        for item in value:
            total = min(total + 4 + _encoded_length(item, ceiling), ceiling)
        return total
    if is_dataclass(value):
        return min(9 + _model_length(value, ceiling), ceiling)
    raise TypeError("unsupported canonical leaf type")


def _model_length(value: object, ceiling: int) -> int:
    domain, trailer = _MODEL_LENGTHS[type(value).__name__]
    total = domain
    for field in fields(cast(Any, value)):
        total = min(total + 4 + _encoded_length(getattr(value, field.name), ceiling), ceiling)
    return min(total + trailer, ceiling)


def measure_canonical_lengths_without_encoding(values: tuple[object, ...], maximum: int) -> int:
    """Measure the pinned codec framing, saturating at ``maximum + 1``."""

    ceiling = maximum + 1
    total = 0
    for value in values:
        total = min(total + _model_length(value, ceiling), ceiling)
    return total


class MssqlR1BindingFailureReasonV2(StrEnum):
    EXACT_TYPE_VIOLATION = "exact_type_violation"
    CANONICAL_SIZE_EXCEEDED = "canonical_size_exceeded"
    WRONG_DOMAIN = "wrong_domain"
    WRONG_VERSION = "wrong_version"
    MALFORMED_CANONICAL_BYTES = "malformed_canonical_bytes"
    UNSUPPORTED_TARGET_PROFILE = "unsupported_target_profile"
    IDENTIFIER_INVALID = "identifier_invalid"
    COLUMN_COUNT_INVALID = "column_count_invalid"
    ORDINAL_INVALID = "ordinal_invalid"
    MAPPING_COVERAGE_INVALID = "mapping_coverage_invalid"
    DEPENDENCY_MISMATCH = "dependency_mismatch"
    AUTHORITY_SPLICE = "authority_splice"
    TARGET_KEY_INVALID = "target_key_invalid"
    STAGE_SHAPE_INVALID = "stage_shape_invalid"
    BUFFER_PLAN_INVALID = "buffer_plan_invalid"
    MODULE_SET_INVALID = "module_set_invalid"
    TEMPLATE_MISMATCH = "template_mismatch"
    SIGNER_POLICY_MISMATCH = "signer_policy_mismatch"
    PERMISSION_WIDENING = "permission_widening"
    SIGNATURE_MISMATCH = "signature_mismatch"
    INTERNAL_INVARIANT_VIOLATION = "internal_invariant_violation"


class MssqlR1BindingRecoveryClassV2(StrEnum):
    PERMANENT_INPUT_ERROR = "permanent_input_error"
    OPERATOR_INTERVENTION = "operator_intervention"


_MESSAGES = {
    "wrong_domain": "Binding input uses an unsupported canonical domain.",
    "wrong_version": "Binding input uses an unsupported contract version.",
    "malformed_canonical_bytes": "Binding input is not canonical.",
    "canonical_size_exceeded": "Binding input exceeds the bounded canonical profile.",
    "exact_type_violation": "Binding input uses an inexact model type.",
    "ordinal_invalid": "Binding ordinals are incomplete or reordered.",
    "column_count_invalid": "Binding column count is outside the supported range.",
    "identifier_invalid": "Binding identifier policy is not satisfied.",
    "dependency_mismatch": "Approved dependency revisions do not form one closure.",
    "authority_splice": "Independently valid route, source or target authorities do not share one identity.",
    "mapping_coverage_invalid": "Source-to-target mapping is not the exact R1 identity mapping.",
    "target_key_invalid": "Target key is outside the R1 profile.",
    "stage_shape_invalid": "Stage projection differs from descriptor/type authority.",
    "buffer_plan_invalid": "Stage scan envelope differs from the accepted ABI.",
    "module_set_invalid": "Binding module set is incomplete or reordered.",
    "template_mismatch": "Descriptor template authority does not match the binding.",
    "signer_policy_mismatch": "Binding signer lifecycle authority does not match Security V2.",
    "permission_widening": "Binding permissions exceed the exact derived closure.",
    "signature_mismatch": "Signature/template intent differs from the module set.",
    "unsupported_target_profile": "Target profile is not supported by R1.",
    "internal_invariant_violation": "Binding compiler invariant was not satisfied.",
}
_PERMANENT = {
    "wrong_domain",
    "wrong_version",
    "malformed_canonical_bytes",
    "canonical_size_exceeded",
    "exact_type_violation",
    "ordinal_invalid",
    "column_count_invalid",
    "identifier_invalid",
    "mapping_coverage_invalid",
    "target_key_invalid",
    "unsupported_target_profile",
}


class MssqlR1BindingContractErrorV2(Exception):
    """Stable failure without upstream payloads or exception linkage."""

    def __init__(self, reason: MssqlR1BindingFailureReasonV2) -> None:
        self.reason = reason
        self.recovery_class = (
            MssqlR1BindingRecoveryClassV2.PERMANENT_INPUT_ERROR
            if reason.value in _PERMANENT
            else MssqlR1BindingRecoveryClassV2.OPERATOR_INTERVENTION
        )
        super().__init__(_MESSAGES[reason.value])


def rejection(reason: str) -> MssqlR1BindingContractErrorV2:
    return MssqlR1BindingContractErrorV2(MssqlR1BindingFailureReasonV2(reason))


def fail(reason: str) -> NoReturn:
    raise rejection(reason) from None


def internal() -> NoReturn:
    error = rejection("internal_invariant_violation")
    error.__cause__ = error.__context__ = None
    raise error from None


def validate_authorities(
    descriptor: Any, security: Any, lifecycle: Any, source: Any, stable: Any, catalog: Any
) -> None:
    scan = next(
        (p for p in descriptor.ordered_procedures if p.portable_object.object_name == "dpone_scan_stage_v3"), None
    )
    names = tuple(c.name for c in catalog.ordered_columns)
    suffix = (
        () if scan is None else tuple(x.name for x in scan.portable_object.result_contract.ordered_fixed_suffix_columns)
    )
    import re

    if any(
        type(x) is not str or len(x.encode("utf-16-le")) > 256 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", x)
        for x in (*names, *suffix)
    ) or len({x.casefold() for x in (*names, *suffix)}) != len(names) + len(suffix):
        fail("identifier_invalid")
    if not 1 <= len(source.ordered_columns) == len(catalog.ordered_columns) <= 1024:
        fail("mapping_coverage_invalid")
    if any(
        a.projection_ordinal != i or b.ordinal != i or a.name != b.name
        for i, (a, b) in enumerate(zip(source.ordered_columns, catalog.ordered_columns, strict=True), 1)
    ):
        fail("mapping_coverage_invalid")
    used_shapes = {item.source_shape.canonical_bytes for item in source.ordered_columns}
    policy_shapes = {item.source_shape.canonical_bytes for item in source.type_policy_authority.ordered_decisions}
    if used_shapes != policy_shapes:
        fail("mapping_coverage_invalid")
    if (
        security.physical_schema_descriptor_digest != descriptor.digest
        or lifecycle.physical_schema_descriptor_digest != descriptor.digest
        or lifecycle.shared_install_security_profile_digest != security.digest
        or stable.catalog_contract_digest != catalog.digest
    ):
        fail("dependency_mismatch")
    if (
        stable.target_binding_uuid != catalog.target_binding_uuid
        or stable.target_object_uuid != catalog.target_object_uuid
        or stable.physical_generation_uuid != catalog.physical_generation_uuid
        or stable.route_source_authority_sha256 != source.selected_source_authority_sha256
        or lifecycle.secret_policy_digest != security.ephemeral_secret_policy.digest
        or lifecycle.certificate_owner.subject_role is not MssqlR1SubjectRoleV3.PROVISIONER
    ):
        fail("authority_splice")
    valid = True
    try:
        security.validate_binding_policy(descriptor, lifecycle)
    except Exception:
        valid = False
    if not valid:
        internal()
    key = catalog.primary_key.ordered_key_column_ordinals
    if len(key) != 1 or catalog.ordered_columns[key[0] - 1].nullable or source.ordered_columns[key[0] - 1].nullable:
        fail("target_key_invalid")
    decisions = {x.source_shape.canonical_bytes: x for x in source.type_policy_authority.ordered_decisions}
    for ordinal, (a, b) in enumerate(zip(source.ordered_columns, catalog.ordered_columns, strict=True), 1):
        decision = decisions.get(a.source_shape.canonical_bytes)
        if decision is None or (ordinal in key and b.scalar_shape != decision.target_shape):
            fail("target_key_invalid")
        if ordinal not in key and (b.scalar_shape != decision.target_shape or b.nullable != a.nullable):
            fail("authority_splice")
    if scan is None:
        fail("buffer_plan_invalid")
    params = tuple((p.name, p.sql_type, p.maximum_length) for p in scan.portable_object.ordered_parameters)
    auth = scan.execution_semantics.request_authority
    grammar_version = f"{auth.codec.codec_id.replace('.', '-')}-v{auth.codec.codec_version}"
    if (
        (auth.binding_kind.value if hasattr(auth.binding_kind, "value") else auth.binding_kind) != "payload_projection"
        or params
        != (("request_payload", "varbinary", -1), ("request_digest", "binary", 32), ("projection_json", "nvarchar", -1))
        or (auth.request_payload_parameter, auth.request_digest_parameter, auth.projection_parameter)
        != ("request_payload", "request_digest", "projection_json")
        or auth.ordered_scalar_parameter_bindings
        or auth.projection_grammar.grammar_version != grammar_version
    ):
        fail("buffer_plan_invalid")


def bounded_shape(body: bytes, maximum_elements: int, maximum_depth: int) -> tuple[int, int]:
    stack = [(0, len(body), 0)]
    count = depth_seen = 0
    while stack:
        cursor, end, parent_depth = stack.pop()
        while cursor < end:
            if cursor + 4 > end:
                return count, depth_seen
            size = int.from_bytes(body[cursor : cursor + 4], "big")
            start, cursor = cursor + 4, cursor + 4 + size
            if cursor > end or start >= cursor:
                return count, depth_seen
            count += 1
            if count > maximum_elements:
                return count, depth_seen
            if body[start : start + 1] == b"q":
                depth = parent_depth + 1
                depth_seen = max(depth_seen, depth)
                if depth > maximum_depth:
                    return count, depth_seen
                stack.append((start + 1, cursor, depth))
    return count, depth_seen


def classify_derived_binding(values: "tuple[Any, ...]", expected: "Any") -> "Any":
    """Classify a decoded portable-identity mismatch without exposing payloads."""
    domain = b"dpone-mssql-r1-binding-portable-identity-v2\x00"
    actual: Any = decode_canonical_bytes(values[7], domain, field_count=7)
    wanted: Any = decode_canonical_bytes(expected.portable_identity.canonical_bytes, domain, field_count=7)
    mapping_domain = b"dpone-mssql-r1-binding-target-mapping-v2\x00"
    mapping: Any = decode_canonical_bytes(actual[1], mapping_domain, field_count=6)
    wanted_mapping: Any = decode_canonical_bytes(wanted[1], mapping_domain, field_count=6)
    if mapping != wanted_mapping:
        column_domain = b"dpone-mssql-r1-business-column-mapping-v2\x00"
        if all(decode_canonical_bytes(item, column_domain, field_count=6)[5] is None for item in mapping[5]):
            fail("target_key_invalid")
        fail("authority_splice")
    for index, reason in (
        (2, "stage_shape_invalid"),
        (3, "module_set_invalid"),
        (4, "signer_policy_mismatch"),
        (5, "permission_widening"),
    ):
        if actual[index] == wanted[index]:
            continue
        if index == 3:
            module_domain = b"dpone-mssql-r1-instantiated-binding-module-v2\x00"

            def decode_module(item: "bytes") -> "tuple[object, ...]":
                return decode_canonical_bytes(item, module_domain, field_count=8)

            observed = tuple(map(decode_module, actual[3]))
            expected_modules = tuple(map(decode_module, wanted[3]))
            by_kind = {item[1]: item for item in observed}
            if any(
                (
                    by_kind.get(kind, (None,) * 8)[6] != item[6]
                    for kind, item in {entry[1]: entry for entry in expected_modules}.items()
                )
            ):
                fail("buffer_plan_invalid")
            if tuple(item[1] for item in observed) != tuple(item[1] for item in expected_modules):
                fail("module_set_invalid")
            if any((left[4] != right[4] for left, right in zip(observed, expected_modules, strict=True))):
                fail("template_mismatch")
        if index == 5:
            permission_domain = b"dpone-mssql-r1-binding-permission-v2\x00"
            actual_permission = decode_canonical_bytes(actual[5], permission_domain, field_count=5)
            expected_permission = decode_canonical_bytes(wanted[5], permission_domain, field_count=5)
            if actual_permission[:4] == expected_permission[:4]:
                fail("signature_mismatch")
        fail(reason)
    fail("internal_invariant_violation")
