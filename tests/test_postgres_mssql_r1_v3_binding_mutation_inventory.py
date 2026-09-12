"""Reader precedence, structural mutation, and bounded-codec inventory for Binding V2."""

from __future__ import annotations

import hashlib
import importlib
from dataclasses import fields
from typing import Any, Literal, cast

from dpone.contracts.mssql_r1_v3_codec import canonical_bytes, decode_canonical_bytes
from tests.test_postgres_mssql_r1_v3_binding_contract import (
    ELEMENT_LIMIT,
    PACK_LIMIT,
    RECOVERY_AND_MESSAGE,
    _baseline,
    _independent_complete_pack_shape,
)
from tests.test_postgres_mssql_r1_v3_binding_semantic_inventory import (
    PHASE_REASONS,
    InputObserver,
    bind_phase_pair_scenario,
)

PACK_DOMAIN = b"dpone-mssql-r1-binding-pack-v2\0"
STABLE_DOMAIN = b"dpone-mssql-r1-rotation-stable-target-authority-v1\0"
CATALOG_DOMAIN = b"dpone-mssql-r1-registered-target-catalog-v1\0"
COLUMN_DOMAIN = b"dpone-mssql-r1-registered-target-column-v1\0"
INDEX_DOMAIN = b"dpone-mssql-r1-registered-target-index-v1\0"
IDENTITY_DOMAIN = b"dpone-mssql-r1-binding-portable-identity-v2\0"
MAPPING_DOMAIN = b"dpone-mssql-r1-binding-target-mapping-v2\0"
MAPPING_COLUMN_DOMAIN = b"dpone-mssql-r1-business-column-mapping-v2\0"
MODULE_DOMAIN = b"dpone-mssql-r1-instantiated-binding-module-v2\0"
PERMISSION_DOMAIN = b"dpone-mssql-r1-binding-permission-v2\0"
SIGNER_DOMAIN = b"dpone-mssql-r1-binding-signer-identity-v2\0"


def _api() -> tuple[Any, Any]:
    pack = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_pack")
    validation = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_validation")
    return pack, validation


def _values(payload: bytes, domain: bytes, count: int) -> list[object]:
    return list(decode_canonical_bytes(payload, domain, field_count=count))


def _pack_values(payload: bytes) -> list[object]:
    return _values(payload, PACK_DOMAIN, 8)


def _repack(values: list[object]) -> bytes:
    return canonical_bytes(PACK_DOMAIN, tuple(values))


def _frame(encoded: bytes) -> bytes:
    return len(encoded).to_bytes(4, "big") + encoded


def _hostile_element_payload(total_elements: int) -> bytes:
    """Build a quota fixture by byte multiplication, never a multi-million tuple."""

    assert total_elements > 1
    nested = b"q" + _frame(b"n") * (total_elements - 1)
    return PACK_DOMAIN + _frame(nested)


def _hostile_depth_payload(depth: int) -> bytes:
    nested = b"n"
    for _ in range(depth):
        nested = b"q" + _frame(nested)
    return PACK_DOMAIN + _frame(nested)


def _mutate_stable(payload: bytes, index: int, value: object) -> bytes:
    outer = _pack_values(payload)
    nested = _values(outer[4], STABLE_DOMAIN, 25)  # type: ignore[arg-type]
    nested[index] = value
    outer[4] = canonical_bytes(STABLE_DOMAIN, tuple(nested))
    return _repack(outer)


def _mutate_catalog(payload: bytes, transform: Any) -> bytes:
    outer = _pack_values(payload)
    nested = _values(outer[5], CATALOG_DOMAIN, 15)  # type: ignore[arg-type]
    transform(nested)
    outer[5] = canonical_bytes(CATALOG_DOMAIN, tuple(nested))
    return _repack(outer)


def _mutate_identity(payload: bytes, transform: Any) -> bytes:
    outer = _pack_values(payload)
    nested = _values(outer[7], IDENTITY_DOMAIN, 7)  # type: ignore[arg-type]
    transform(nested)
    outer[7] = canonical_bytes(IDENTITY_DOMAIN, tuple(nested))
    return _repack(outer)


def _phase_mutation(payload: bytes, phase: str) -> bytes | bytearray:
    if phase == "01":
        return bytearray(payload)
    if phase == "02":
        return payload + b"\0" * (PACK_LIMIT - len(payload) + 1)
    if phase == "03":
        return b"foreign-binding-family\0" + payload[len(PACK_DOMAIN) :]
    if phase == "04":
        values = _pack_values(payload)
        values[0] = "dpone-mssql-r1-binding-pack-1"
        return _repack(values)
    if phase == "05":
        return payload + b"\0"
    if phase == "06":
        return _mutate_stable(payload, 4, "unsupported_target_profile")
    if phase == "07":

        def invalid_identifier(catalog: list[object]) -> None:
            columns = list(cast(tuple[bytes, ...], catalog[11]))
            column = _values(columns[1], COLUMN_DOMAIN, 18)
            column[1] = "bad name"
            columns[1] = canonical_bytes(COLUMN_DOMAIN, tuple(column))
            catalog[11] = tuple(columns)

        return _mutate_catalog(payload, invalid_identifier)
    if phase == "08":

        def too_many(catalog: list[object]) -> None:
            columns = cast(tuple[bytes, ...], catalog[11])
            catalog[11] = columns + (columns[-1],) * (1025 - len(columns))

        return _mutate_catalog(payload, too_many)
    if phase == "09":

        def reorder(catalog: list[object]) -> None:
            catalog[11] = tuple(reversed(cast(tuple[bytes, ...], catalog[11])))

        return _mutate_catalog(payload, reorder)
    if phase == "10":

        def different_name(catalog: list[object]) -> None:
            columns = list(cast(tuple[bytes, ...], catalog[11]))
            column = _values(columns[1], COLUMN_DOMAIN, 18)
            column[1] = "different_name"
            columns[1] = canonical_bytes(COLUMN_DOMAIN, tuple(column))
            catalog[11] = tuple(columns)

        return _mutate_catalog(payload, different_name)
    if phase == "11":
        return _mutate_stable(payload, 23, b"d" * 32)
    if phase == "12":
        return _mutate_stable(payload, 8, __import__("uuid").UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
    if phase == "13":

        def no_mapping_key(identity: list[object]) -> None:
            mapping = _values(identity[1], MAPPING_DOMAIN, 6)  # type: ignore[arg-type]
            columns = list(cast(tuple[bytes, ...], mapping[5]))
            changed = []
            for raw in columns:
                item = _values(raw, MAPPING_COLUMN_DOMAIN, 6)
                item[5] = None
                changed.append(canonical_bytes(MAPPING_COLUMN_DOMAIN, tuple(item)))
            mapping[5] = tuple(changed)
            identity[1] = canonical_bytes(MAPPING_DOMAIN, tuple(mapping))

        return _mutate_identity(payload, no_mapping_key)
    if phase == "14":

        def foreign_target_ref(identity: list[object]) -> None:
            mapping = _values(identity[1], MAPPING_DOMAIN, 6)  # type: ignore[arg-type]
            columns = list(cast(tuple[bytes, ...], mapping[5]))
            item = _values(columns[1], MAPPING_COLUMN_DOMAIN, 6)
            item[4] = "foreign-decision"
            columns[1] = canonical_bytes(MAPPING_COLUMN_DOMAIN, tuple(item))
            mapping[5] = tuple(columns)
            identity[1] = canonical_bytes(MAPPING_DOMAIN, tuple(mapping))

        return _mutate_identity(payload, foreign_target_ref)
    if phase == "15":

        def bad_projection(identity: list[object]) -> None:
            projections = list(cast(tuple[bytes, ...], identity[2]))
            projections[0] += b"\0"
            identity[2] = tuple(projections)

        return _mutate_identity(payload, bad_projection)
    if phase == "16":

        def bad_buffer(identity: list[object]) -> None:
            modules = list(cast(tuple[bytes, ...], identity[3]))
            module = _values(modules[0], MODULE_DOMAIN, 8)
            module[6] += b"\0"  # type: ignore[operator]
            modules[0] = canonical_bytes(MODULE_DOMAIN, tuple(module))
            identity[3] = tuple(modules)

        return _mutate_identity(payload, bad_buffer)
    if phase == "17":
        return _mutate_identity(payload, lambda identity: identity.__setitem__(3, tuple(reversed(identity[3]))))
    if phase == "18":

        def bad_template(identity: list[object]) -> None:
            modules = list(cast(tuple[bytes, ...], identity[3]))
            module = _values(modules[0], MODULE_DOMAIN, 8)
            module[4] = b"t" * 32
            modules[0] = canonical_bytes(MODULE_DOMAIN, tuple(module))
            identity[3] = tuple(modules)

        return _mutate_identity(payload, bad_template)
    if phase == "19":

        def bad_signer(identity: list[object]) -> None:
            signer = _values(identity[4], SIGNER_DOMAIN, 12)  # type: ignore[arg-type]
            signer[3] = "foreign_cert"
            identity[4] = canonical_bytes(SIGNER_DOMAIN, tuple(signer))

        return _mutate_identity(payload, bad_signer)
    if phase == "20":

        def bad_permission(identity: list[object]) -> None:
            permission = _values(identity[5], PERMISSION_DOMAIN, 5)  # type: ignore[arg-type]
            permission[1] = "widened"
            identity[5] = canonical_bytes(PERMISSION_DOMAIN, tuple(permission))

        return _mutate_identity(payload, bad_permission)
    if phase == "21":

        def bad_signature(identity: list[object]) -> None:
            permission = _values(identity[5], PERMISSION_DOMAIN, 5)  # type: ignore[arg-type]
            permission[4] = tuple(reversed(cast(tuple[bytes, ...], permission[4])))
            identity[5] = canonical_bytes(PERMISSION_DOMAIN, tuple(permission))

        return _mutate_identity(payload, bad_signature)
    raise AssertionError(f"unsupported reader phase: {phase}")


def _assert_reader_reason(pack_module: Any, validation: Any, payload: bytes | bytearray, phase: str) -> None:
    try:
        pack_module.MssqlR1BindingModulePackV2.from_canonical_bytes(payload)
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
    raise AssertionError(f"reader phase {phase} must reject")


def _reader_pair(earlier: str, later: str, observe_inputs: InputObserver) -> Literal["typed_rejection"]:
    baseline = _independent_complete_pack_shape(_baseline())
    later_payload = _phase_mutation(baseline, later)
    # A trailing-byte phase-05 payload is intentionally not decodable, so the
    # only pair whose earlier mutation needs the top-level fields is composed
    # in the opposite order. Both defects remain present and phase 04 wins.
    combined = (
        _phase_mutation(bytes(_phase_mutation(baseline, earlier)), later)
        if (earlier, later) == ("04", "05")
        else _phase_mutation(bytes(later_payload), earlier)
    )
    earlier_payload = _phase_mutation(baseline, earlier)
    observe_inputs(
        tuple(
            (name, hashlib.sha256(bytes(payload)).hexdigest())
            for name, payload in (
                ("combined", combined),
                ("later_only", later_payload),
                ("earlier_only", earlier_payload),
            )
        )
    )
    pack_module, validation = _api()
    _assert_reader_reason(pack_module, validation, combined, earlier)
    _assert_reader_reason(pack_module, validation, later_payload, later)
    _assert_reader_reason(pack_module, validation, earlier_payload, earlier)
    return "typed_rejection"


bind_phase_pair_scenario("reader", _reader_pair)


def test_binding_model_inventory_is_closed_and_slotted() -> None:
    modules = tuple(
        importlib.import_module(name)
        for name in (
            "dpone.contracts.mssql_r1_v3_binding_modules",
            "dpone.contracts.mssql_r1_v3_binding_permissions",
            "dpone.contracts.mssql_r1_v3_binding_pack",
            "dpone.contracts.mssql_r1_v3_binding_validation",
        )
    )
    models = {
        value.__name__: value
        for module in modules
        for value in vars(module).values()
        if isinstance(value, type) and value.__module__ == module.__name__ and hasattr(value, "__dataclass_fields__")
    }
    expected_fields = {
        "MssqlR1RegisteredTargetRefV2": (
            "contract_version",
            "target_binding_uuid",
            "target_object_uuid",
            "resource_ref",
            "stable_target_authority_digest",
            "target_catalog_digest",
        ),
        "MssqlR1BusinessColumnMappingV2": (
            "contract_version",
            "ordinal",
            "source_column_ref",
            "target_column_ref",
            "type_decision_id",
            "key_ordinal",
        ),
        "MssqlR1BindingTargetMappingV2": (
            "contract_version",
            "registered_target",
            "source_schema_authority_digest",
            "route_source_authority_sha256",
            "type_policy_authority_digest",
            "ordered_columns",
        ),
        "MssqlR1StageScanProjectionV2": (
            "contract_version",
            "artifact_kind",
            "core_scan_procedure_ref",
            "stage_scan_template_digest",
            "ordered_business_columns",
            "ordered_result_columns",
        ),
        "MssqlR1StageScanInvocationV2": (
            "contract_version",
            "ordinal",
            "artifact_kind",
            "buffer_symbol",
            "projection_digest",
            "scan_procedure_digest",
            "ordered_envelope_sources",
        ),
        "MssqlR1ModuleStageBufferPlanV2": ("contract_version", "module_kind", "ordered_inputs"),
        "MssqlR1InstantiatedBindingModuleV2": (
            "contract_version",
            "module_kind",
            "schema_name",
            "object_name",
            "descriptor_template_digest",
            "target_mapping_digest",
            "buffer_plan",
            "ordered_target_access_intents",
        ),
        "MssqlR1BindingSignerIdentityV2": (
            "contract_version",
            "target_binding_uuid",
            "lifecycle_policy",
            "certificate_name",
            "certificate_user_name",
            "certificate_subject",
            "certificate_owner",
            "start_date_yyyymmdd",
            "expiry_date_yyyymmdd",
            "certificate_creation_profile",
            "signature_algorithm",
            "secret_policy_digest",
        ),
        "MssqlR1BindingSignatureIntentV2": (
            "contract_version",
            "ordinal",
            "module_kind",
            "semantic_module_digest",
            "signer_identity_digest",
        ),
        "MssqlR1BindingAccessPermissionProjectionV2": ("contract_version", "source_access", "permission_path"),
        "MssqlR1BindingPermissionContractV2": (
            "contract_version",
            "permission_profile",
            "ordered_access_projections",
            "ordered_runtime_execute_paths",
            "ordered_signature_intents",
        ),
        "MssqlR1ExpectedBindingInventoryV2": (
            "contract_version",
            "target_binding_uuid",
            "ordered_module_names",
            "certificate_name",
            "certificate_user_name",
            "ordered_semantic_module_digests",
            "ordered_permission_path_digests",
            "ordered_signature_intent_digests",
            "permission_contract_digest",
        ),
        "MssqlR1BindingInstantiationContractV2": (
            "contract_version",
            "physical_descriptor_digest",
            "shared_security_profile_digest",
            "source_schema_authority_digest",
            "type_policy_authority_digest",
            "stable_target_authority_digest",
            "target_catalog_digest",
            "ordered_binding_template_digests",
            "stage_scan_template_digest",
            "stage_scan_request_authority_digest",
            "stage_scan_suffix_digest",
            "buffer_matrix_digest",
            "signer_lifecycle_policy",
            "permission_projection_profile",
            "compiler_policy",
        ),
        "MssqlR1BindingPortableIdentityV2": (
            "contract_version",
            "target_mapping",
            "ordered_stage_projections",
            "ordered_modules",
            "signer_identity",
            "permission_contract",
            "expected_inventory",
        ),
        "MssqlR1BindingModulePackV2": (
            "contract_version",
            "physical_descriptor_payload",
            "shared_security_profile_payload",
            "source_schema_authority_payload",
            "stable_target_authority_payload",
            "target_catalog_payload",
            "binding_instantiation_contract_payload",
            "portable_identity",
        ),
    }
    assert set(models) == set(expected_fields)
    assert {name: tuple(item.name for item in fields(model)) for name, model in models.items()} == expected_fields
    assert all(hasattr(model, "__slots__") for model in models.values())
    enums = importlib.import_module("dpone.contracts.mssql_r1_v3_binding_modules")
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
    # Closed union/optional inventory: each arm is also exercised by the
    # successful two-column pack and the three phase-specific mutations.
    assert {
        "MssqlR1BusinessColumnMappingV2.key_ordinal": (None, 1),
        "MssqlR1StageScanProjectionV2.artifact_kind": (
            "batch_payload",
            "xmin_delta",
            "xmin_complete_keys",
        ),
        "MssqlR1StageScanInvocationV2.buffer_symbol": (
            "batch_payload",
            "xmin_delta",
            "xmin_complete_keys",
        ),
        "MssqlR1InstantiatedBindingModuleV2.target_resource_arm": ("registered_target", "static_object"),
        "MssqlR1BindingAccessPermissionProjectionV2.permission_arm": ("SELECT", "INSERT", "UPDATE", "DELETE"),
        "MssqlR1BindingPermissionContractV2.beneficiary_arm": ("binding_signer_instance", "runtime"),
    } == {
        "MssqlR1BusinessColumnMappingV2.key_ordinal": (None, 1),
        "MssqlR1StageScanProjectionV2.artifact_kind": tuple(item.value for item in enums.MssqlR1StageBufferSymbolV2),
        "MssqlR1StageScanInvocationV2.buffer_symbol": tuple(item.value for item in enums.MssqlR1StageBufferSymbolV2),
        "MssqlR1InstantiatedBindingModuleV2.target_resource_arm": ("registered_target", "static_object"),
        "MssqlR1BindingAccessPermissionProjectionV2.permission_arm": ("SELECT", "INSERT", "UPDATE", "DELETE"),
        "MssqlR1BindingPermissionContractV2.beneficiary_arm": ("binding_signer_instance", "runtime"),
    }


def test_reader_safety_constants_are_exact_and_reader_only() -> None:
    pack_module, _ = _api()
    assert pack_module.MAX_BINDING_PACK_CANONICAL_BYTES_V2 == PACK_LIMIT
    assert pack_module.MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2 == ELEMENT_LIMIT


def test_reader_depth_and_element_quota_boundaries_are_allocation_safe() -> None:
    exact_elements = _hostile_element_payload(ELEMENT_LIMIT)
    plus_one_element = _hostile_element_payload(ELEMENT_LIMIT + 1)
    assert len(plus_one_element) == len(PACK_DOMAIN) + 5 * ELEMENT_LIMIT + 5
    assert len(plus_one_element) < PACK_LIMIT

    exact_depth = _hostile_depth_payload(16)
    plus_one_depth = _hostile_depth_payload(17)
    assert len(plus_one_depth) < 128
    pack_module, validation = _api()
    _assert_reader_reason(pack_module, validation, exact_elements, "01")
    _assert_reader_reason(pack_module, validation, plus_one_element, "02")
    _assert_reader_reason(pack_module, validation, exact_depth, "01")
    _assert_reader_reason(pack_module, validation, plus_one_depth, "02")

    exact_byte_limit = PACK_DOMAIN + b"\0" * (PACK_LIMIT - len(PACK_DOMAIN))
    assert len(exact_byte_limit) == PACK_LIMIT
    _assert_reader_reason(pack_module, validation, exact_byte_limit, "05")
    plus_one_byte = exact_byte_limit + b"\0"
    del exact_byte_limit
    assert len(plus_one_byte) == PACK_LIMIT + 1
    _assert_reader_reason(pack_module, validation, plus_one_byte, "02")
