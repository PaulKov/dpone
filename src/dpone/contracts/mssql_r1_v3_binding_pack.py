"""Pure, fail-closed compiler for the restricted MSSQL Binding V2 pack."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal

from dpone.contracts.mssql_r1_v3_binding_modules import (
    derive_binding_core,
)
from dpone.contracts.mssql_r1_v3_binding_permissions import (
    MssqlR1BindingInstantiationContractV2,
    MssqlR1BindingPortableIdentityV2,
    MssqlR1BindingSignatureIntentV2,
    MssqlR1BindingSignerIdentityV2,
    MssqlR1ExpectedBindingInventoryV2,
    derive_permissions,
)
from dpone.contracts.mssql_r1_v3_binding_validation import (
    MssqlR1BindingContractErrorV2,
    bounded_shape,
    classify_derived_binding,
    measure_canonical_lengths_without_encoding,
    validate_authorities,
)
from dpone.contracts.mssql_r1_v3_binding_validation import (
    fail as _fail,
)
from dpone.contracts.mssql_r1_v3_binding_validation import (
    internal as _internal,
)
from dpone.contracts.mssql_r1_v3_codec import canonical_bytes, decode_canonical_bytes
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import MssqlR1PhysicalSchemaDescriptorV1
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
    MssqlR1BindingSignerLifecyclePolicyV2,
)
from dpone.contracts.mssql_r1_v3_provider_security_profile import MssqlR1SharedInstallSecurityProfileV2
from dpone.contracts.mssql_r1_v3_registered_target_catalog import (
    MssqlR1RegisteredTargetCatalogV1,
)
from dpone.contracts.mssql_r1_v3_verified_target_authority import MssqlR1RotationStableTargetAuthorityV1
from dpone.contracts.postgres_mssql_source_schema_authority import PostgresMssqlSelectedRelationSchemaAuthorityV1

MAX_BINDING_FACTORY_EMBEDDED_INPUT_BYTES_V2 = 25_165_824
MAX_BINDING_PACK_CANONICAL_BYTES_V2 = 67_108_864
MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2 = 4_194_304
MAX_BINDING_PACK_DEPTH_V2 = 16
_DOMAIN = b"dpone-mssql-r1-binding-pack-v2\0"


def _d(value: bytes) -> bytes:
    return sha256(value).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingModulePackV2:
    contract_version: Literal["dpone-mssql-r1-binding-pack-2"]
    physical_descriptor_payload: bytes
    shared_security_profile_payload: bytes
    source_schema_authority_payload: bytes
    stable_target_authority_payload: bytes
    target_catalog_payload: bytes
    binding_instantiation_contract_payload: bytes
    portable_identity: MssqlR1BindingPortableIdentityV2

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAIN,
            (
                *tuple(getattr(self, x) for x in tuple(self.__dataclass_fields__)[:7]),
                self.portable_identity.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return _d(self.canonical_bytes)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingModulePackV2:
        return _read_pack(payload)


class MssqlR1BindingModulePackFactoryV2:
    """Compile six exact upstream authorities into an immutable pack."""

    def create(
        self,
        *,
        physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
        shared_security_profile: MssqlR1SharedInstallSecurityProfileV2,
        binding_signer_lifecycle_policy: MssqlR1BindingSignerLifecyclePolicyV2,
        source_schema_authority: PostgresMssqlSelectedRelationSchemaAuthorityV1,
        stable_target_authority: MssqlR1RotationStableTargetAuthorityV1,
        target_catalog: MssqlR1RegisteredTargetCatalogV1,
    ) -> MssqlR1BindingModulePackV2:
        values = (
            physical_descriptor,
            shared_security_profile,
            binding_signer_lifecycle_policy,
            source_schema_authority,
            stable_target_authority,
            target_catalog,
        )
        types = (
            MssqlR1PhysicalSchemaDescriptorV1,
            MssqlR1SharedInstallSecurityProfileV2,
            MssqlR1BindingSignerLifecyclePolicyV2,
            PostgresMssqlSelectedRelationSchemaAuthorityV1,
            MssqlR1RotationStableTargetAuthorityV1,
            MssqlR1RegisteredTargetCatalogV1,
        )
        if any(type(v) is not t for v, t in zip(values, types, strict=True)):
            _fail("exact_type_violation")
        measured = None
        try:
            measured = measure_canonical_lengths_without_encoding(values, MAX_BINDING_FACTORY_EMBEDDED_INPUT_BYTES_V2)
        except Exception:
            pass
        if measured is None:
            return _internal()
        if measured > MAX_BINDING_FACTORY_EMBEDDED_INPUT_BYTES_V2:
            _fail("canonical_size_exceeded")
        payloads = None
        try:
            payloads = tuple(v.canonical_bytes for v in values)
        except Exception:
            pass
        if payloads is None:
            return _internal()
        decoded = _round_trip_authorities(values, payloads)
        compiled = None
        canonical = None
        try:
            compiled = _compile(*decoded, payloads=payloads)
            canonical = compiled.canonical_bytes
        except MssqlR1BindingContractErrorV2:
            raise
        except Exception:
            pass
        if compiled is None or canonical is None:
            return _internal()
        count, depth = bounded_shape(
            canonical[len(_DOMAIN) :], MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2, MAX_BINDING_PACK_DEPTH_V2
        )
        if (
            len(canonical) > MAX_BINDING_PACK_CANONICAL_BYTES_V2
            or count > MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2
            or depth > MAX_BINDING_PACK_DEPTH_V2
        ):
            return _internal()
        verified = None
        try:
            verified = _read_pack(canonical)
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
        if verified is None or verified != compiled:
            return _internal()
        return verified


def _round_trip_authorities(values: tuple[Any, ...], payloads: tuple[bytes, ...]) -> tuple[Any, ...]:
    decoders = (
        MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes,
        MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes,
        MssqlR1BindingSignerLifecyclePolicyV2.from_canonical_bytes,
        PostgresMssqlSelectedRelationSchemaAuthorityV1.from_canonical_bytes,
        MssqlR1RotationStableTargetAuthorityV1.from_canonical_bytes,
        MssqlR1RegisteredTargetCatalogV1.from_canonical_bytes,
    )
    decoded = None
    try:
        decoded = tuple(decoder(payload) for decoder, payload in zip(decoders, payloads, strict=True))
    except BaseException as exc:
        if not isinstance(exc, Exception):
            raise
    if decoded is None or decoded != values:
        return _internal()
    return decoded


def _compile(
    descriptor: Any,
    security: Any,
    lifecycle: Any,
    source: Any,
    stable: Any,
    catalog: Any,
    *,
    payloads: tuple[bytes, ...],
) -> MssqlR1BindingModulePackV2:
    validate_authorities(descriptor, security, lifecycle, source, stable, catalog)
    scan, mapping, projections_t, modules_t = derive_binding_core(descriptor, source, stable, catalog, payloads)
    signer = MssqlR1BindingSignerIdentityV2(
        "dpone-mssql-r1-binding-signer-identity-2",
        catalog.target_binding_uuid,
        lifecycle,
        lifecycle.certificate_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
        lifecycle.certificate_user_name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
        lifecycle.certificate_subject_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
        lifecycle.certificate_owner,
        lifecycle.start_date_yyyymmdd,
        lifecycle.expiry_date_yyyymmdd,
        lifecycle.certificate_creation_profile,
        lifecycle.signature_algorithm,
        lifecycle.secret_policy_digest,
    )
    signatures = tuple(
        MssqlR1BindingSignatureIntentV2(
            "dpone-mssql-r1-binding-signature-intent-2", i, module.module_kind, module.digest, signer.digest
        )
        for i, module in enumerate(modules_t, 1)
    )
    permission = derive_permissions(catalog, modules_t, signatures)
    inventory = MssqlR1ExpectedBindingInventoryV2(
        "dpone-mssql-r1-expected-binding-inventory-2",
        catalog.target_binding_uuid,
        tuple(x.object_name for x in modules_t),
        signer.certificate_name,
        signer.certificate_user_name,
        tuple(x.digest for x in modules_t),
        tuple(_d(x.canonical_bytes) for x in permission.ordered_permission_paths),
        tuple(x.digest for x in signatures),
        permission.digest,
    )
    identity = MssqlR1BindingPortableIdentityV2(
        "dpone-mssql-r1-binding-portable-identity-2", mapping, projections_t, modules_t, signer, permission, inventory
    )
    matrix = canonical_bytes(
        b"dpone-mssql-r1-buffer-matrix-v2\0",
        (
            tuple(
                (m.module_kind.value, tuple(x.canonical_bytes for x in m.buffer_plan.ordered_inputs)) for m in modules_t
            ),
        ),
    )
    instantiation = MssqlR1BindingInstantiationContractV2(
        "dpone-mssql-r1-binding-instantiation-2",
        _d(payloads[0]),
        _d(payloads[1]),
        _d(payloads[3]),
        source.type_policy_authority.digest,
        _d(payloads[4]),
        catalog.digest,
        tuple(_d(x.canonical_bytes) for x in descriptor.ordered_binding_module_templates),
        _d(scan.portable_object.result_contract.canonical_bytes),
        _d(scan.execution_semantics.request_authority.canonical_bytes),
        _d(
            canonical_bytes(
                b"dpone-mssql-r1-stage-suffix-v2\0",
                (tuple(x.canonical_bytes for x in scan.portable_object.result_contract.ordered_fixed_suffix_columns),),
            )
        ),
        _d(matrix),
        lifecycle,
        "exact_target_and_row_hash_v1",
        "pure_fail_closed_v2",
    )
    return MssqlR1BindingModulePackV2(
        "dpone-mssql-r1-binding-pack-2",
        payloads[0],
        payloads[1],
        payloads[3],
        payloads[4],
        payloads[5],
        instantiation.canonical_bytes,
        identity,
    )


def _classify_structural(values: tuple[Any, ...]) -> Any:
    try:
        stable: Any = decode_canonical_bytes(
            values[4], b"dpone-mssql-r1-rotation-stable-target-authority-v1\0", field_count=25
        )
        catalog: Any = decode_canonical_bytes(
            values[5], b"dpone-mssql-r1-registered-target-catalog-v1\0", field_count=15
        )
        columns: Any = catalog[11]
        if stable[4] != "ordinary_disk_rowstore_v1":
            _fail("unsupported_target_profile")
        decoded: Any = tuple(
            decode_canonical_bytes(item, b"dpone-mssql-r1-registered-target-column-v1\0", field_count=18)
            for item in columns
        )
        if any(" " in item[1] for item in decoded):
            _fail("identifier_invalid")
        if not 1 <= len(columns) <= 1024:
            _fail("column_count_invalid")
        if tuple(item[0] for item in decoded) != tuple(range(1, len(columns) + 1)):
            _fail("ordinal_invalid")
    except MssqlR1BindingContractErrorV2:
        raise
    except Exception:
        pass
    return _internal()


def _read_pack(payload: bytes) -> MssqlR1BindingModulePackV2:
    if type(payload) is not bytes:
        _fail("exact_type_violation")
    if len(payload) > MAX_BINDING_PACK_CANONICAL_BYTES_V2:
        _fail("canonical_size_exceeded")
    if not payload.startswith(_DOMAIN):
        _fail("wrong_domain")
    count, depth = bounded_shape(
        payload[len(_DOMAIN) :], MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2, MAX_BINDING_PACK_DEPTH_V2
    )
    if count > MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2 or depth > MAX_BINDING_PACK_DEPTH_V2:
        _fail("canonical_size_exceeded")
    if count == MAX_BINDING_PACK_SEQUENCE_ELEMENTS_V2 or depth == MAX_BINDING_PACK_DEPTH_V2:
        _fail("exact_type_violation")
    values: Any = None
    try:
        values = decode_canonical_bytes(payload, _DOMAIN, field_count=8)
    except Exception:
        pass
    if values is None:
        try:
            size = int.from_bytes(payload[len(_DOMAIN) : len(_DOMAIN) + 4], "big")
            one = payload[: len(_DOMAIN) + 4 + size]
            if decode_canonical_bytes(one, _DOMAIN, field_count=1)[0] != "dpone-mssql-r1-binding-pack-2":
                _fail("wrong_version")
        except MssqlR1BindingContractErrorV2:
            raise
        except Exception:
            pass
        _fail("malformed_canonical_bytes")
    if values[0] != "dpone-mssql-r1-binding-pack-2":
        _fail("wrong_version")
    decoded = None
    try:
        descriptor = MssqlR1PhysicalSchemaDescriptorV1.from_canonical_bytes(values[1])
        security = MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes(values[2])
        source = PostgresMssqlSelectedRelationSchemaAuthorityV1.from_canonical_bytes(values[3])
        stable = MssqlR1RotationStableTargetAuthorityV1.from_canonical_bytes(values[4])
        catalog = MssqlR1RegisteredTargetCatalogV1.from_canonical_bytes(values[5])
        instantiation: Any = decode_canonical_bytes(
            values[6], b"dpone-mssql-r1-binding-instantiation-contract-v2\0", field_count=15
        )
        lifecycle = MssqlR1BindingSignerLifecyclePolicyV2.from_canonical_bytes(instantiation[12])
        decoded = (descriptor, security, lifecycle, source, stable, catalog)
    except Exception:
        pass
    if decoded is None:
        return _classify_structural(values)
    payloads = (values[1], values[2], instantiation[12], values[3], values[4], values[5])
    expected = None
    try:
        expected = _compile(*decoded, payloads=payloads)
    except MssqlR1BindingContractErrorV2:
        raise
    except BaseException as exc:
        if not isinstance(exc, Exception):
            raise
    if expected is None:
        return _internal()
    if expected.canonical_bytes == payload == canonical_bytes(_DOMAIN, values):
        return expected
    return classify_derived_binding(values, expected)
