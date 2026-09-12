"""Observed binding-catalog leaves for Provider Attestation Foundation V2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_provider_attestation_identity import (
    Reason,
    attestation_fail,
    bound_imported_graph,
    decode_attestation_model,
    decode_nested_bytes,
    emit_canonical,
    expect_attestation_enum,
    require_bounded_text,
    require_digest32,
    require_nonzero_uuid,
    require_sql_positive,
)
from dpone.contracts.mssql_r1_v3_provider_security_certificate import MssqlR1CertificateCatalogObservationV1
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1ModuleOptionsV3

_MODULE = b"dpone-mssql-r1-observed-binding-module-v1\0"
_SIGNATURE = b"dpone-mssql-r1-observed-binding-signature-v1\0"
_PREFIX = b"dpone-mssql-r1-observed-binding-prefix-object-v1\0"
_SIGNER = b"dpone-mssql-r1-observed-binding-signer-v2\0"
PREFIX_TYPES = ("procedure", "certificate", "database_principal")
CAP_LEAF = 262144


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedBindingModuleV1:
    module_kind: MssqlR1BindingModuleKindV1
    schema_name: str
    object_name: str
    object_id: int
    normalized_definition_bytes: bytes
    persisted_options: MssqlR1ModuleOptionsV3

    def __post_init__(self) -> None:
        if not isinstance(self.module_kind, MssqlR1BindingModuleKindV1):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_bounded_text(self.schema_name, "schema_name")
        require_bounded_text(self.object_name, "object_name")
        require_sql_positive(self.object_id, "object_id")
        raw = self.normalized_definition_bytes
        if not isinstance(raw, bytes) or not raw or len(raw) > 131072:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        if type(self.persisted_options) is not MssqlR1ModuleOptionsV3:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        bound_imported_graph(self.persisted_options)
        emit_canonical(_MODULE, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.module_kind,
            self.schema_name,
            self.object_name,
            self.object_id,
            self.normalized_definition_bytes,
            self.persisted_options.canonical_bytes,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_MODULE, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedBindingModuleV1:
        values = list(decode_attestation_model(payload, _MODULE, 6, CAP_LEAF))
        return cls(
            expect_attestation_enum(MssqlR1BindingModuleKindV1, values[0], "module_kind"),
            values[1],  # type: ignore[arg-type]
            values[2],  # type: ignore[arg-type]
            values[3],  # type: ignore[arg-type]
            values[4],  # type: ignore[arg-type]
            decode_nested_bytes(values[5], MssqlR1ModuleOptionsV3.from_canonical_bytes, "options"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedBindingSignatureV1:
    signature_intent_digest: bytes
    module_object_id: int
    certificate_id: int
    crypt_property_bytes: bytes

    def __post_init__(self) -> None:
        require_digest32(self.signature_intent_digest, "signature_intent_digest")
        require_sql_positive(self.module_object_id, "module_object_id")
        require_sql_positive(self.certificate_id, "certificate_id")
        raw = self.crypt_property_bytes
        if not isinstance(raw, bytes) or not raw or len(raw) > 4096:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_SIGNATURE, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (self.signature_intent_digest, self.module_object_id, self.certificate_id, self.crypt_property_bytes)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_SIGNATURE, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedBindingSignatureV1:
        return cls(*decode_attestation_model(payload, _SIGNATURE, 4, CAP_LEAF))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedBindingPrefixObjectV1:
    schema_name: str
    object_name: str
    object_type: Literal["procedure", "certificate", "database_principal"]
    catalog_id: int

    def __post_init__(self) -> None:
        require_bounded_text(self.schema_name, "schema_name")
        require_bounded_text(self.object_name, "object_name")
        if self.object_type not in PREFIX_TYPES:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_sql_positive(self.catalog_id, "catalog_id")
        emit_canonical(_PREFIX, self._fields(), CAP_LEAF)

    def inventory_key(self) -> tuple[str, str, str, int]:
        return (self.schema_name, self.object_type, self.object_name, self.catalog_id)

    def _fields(self) -> tuple[object, ...]:
        return (self.schema_name, self.object_name, self.object_type, self.catalog_id)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_PREFIX, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedBindingPrefixObjectV1:
        return cls(*decode_attestation_model(payload, _PREFIX, 4, CAP_LEAF))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedBindingSignerV2:
    target_binding_uuid: object
    certificate_observation: MssqlR1CertificateCatalogObservationV1
    certificate_id: int
    certificate_owner_principal_id: int
    certificate_user_principal_id: int

    def __post_init__(self) -> None:
        require_nonzero_uuid(self.target_binding_uuid, "target_binding_uuid")
        if type(self.certificate_observation) is not MssqlR1CertificateCatalogObservationV1:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        bound_imported_graph(self.certificate_observation)
        require_sql_positive(self.certificate_id, "certificate_id")
        require_sql_positive(self.certificate_owner_principal_id, "certificate_owner_principal_id")
        require_sql_positive(self.certificate_user_principal_id, "certificate_user_principal_id")
        emit_canonical(_SIGNER, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.target_binding_uuid,
            self.certificate_observation.canonical_bytes,
            self.certificate_id,
            self.certificate_owner_principal_id,
            self.certificate_user_principal_id,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_SIGNER, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedBindingSignerV2:
        values = list(decode_attestation_model(payload, _SIGNER, 5, CAP_LEAF))
        values[1] = decode_nested_bytes(
            values[1], MssqlR1CertificateCatalogObservationV1.from_canonical_bytes, "certificate"
        )
        return cls(*values)  # type: ignore[arg-type]


__all__ = (
    "MssqlR1ObservedBindingModuleV1",
    "MssqlR1ObservedBindingPrefixObjectV1",
    "MssqlR1ObservedBindingSignatureV1",
    "MssqlR1ObservedBindingSignerV2",
)
