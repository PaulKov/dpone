"""Seven closed query-result arms and the post-install statement result."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_provider_attestation_binding_observation import (  # noqa: F401
    MssqlR1ObservedBindingModuleV1,
    MssqlR1ObservedBindingPrefixObjectV1,
    MssqlR1ObservedBindingSignatureV1,
    MssqlR1ObservedBindingSignerV2,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_identity import (  # noqa: F401
    QUERY_KIND_ORDER,
    QUERY_RESULT_PAIRS,
    MssqlR1AttestationQueryKindV1,
    MssqlR1AttestationResultAuthorityKindV1,
    MssqlR1CertifiedServerBuildProfileV1,
    MssqlR1MigrationStatementPhaseV1,
    MssqlR1MigrationStatementRefV1,
    MssqlR1MigrationTargetRefV1,
    MssqlR1ObservedDatabaseIdentityV1,
    MssqlR1ProviderAttestationStatementRegistryV2,
    Reason,
    attestation_fail,
    bound_imported_graph,
    canonical_encoded_size_v1,
    decode_attestation_model,
    decode_nested_bytes,
    decode_nested_tuple,
    emit_canonical,
    expect_attestation_enum,
    preflight_provider_attestation_canonical_v2,
    require_canonical_unique,
    require_digest32,
    require_nonzero_uuid,
    require_sql_positive,
)
from dpone.contracts.mssql_r1_v3_provider_security_certificate import (  # noqa: F401
    MssqlR1CertificateCatalogObservationV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (  # noqa: F401
    MssqlR1ResolvedPermissionPathV2,
    project_schema_permission_rule,
)
from dpone.contracts.mssql_r1_v3_schema_observation import (  # noqa: F401
    MssqlR1ModuleSignatureObservationV3,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedRoleMembershipV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
)

CAP_QUERY = 3145728
CAP_POST = 4194304
_SCHEMA = b"dpone-r1-schema-query-result-v1\0"
_TABLE = b"dpone-r1-table-query-result-v1\0"
_MODULE = b"dpone-r1-module-query-result-v1\0"
_SIGNATURE = b"dpone-r1-signature-query-result-v1\0"
_PREFIX = b"dpone-r1-binding-prefix-query-result-v1\0"
_CERT = b"dpone-r1-certificate-query-result-v2\0"
_PERM = b"dpone-r1-permission-query-result-v2\0"
_POST = b"dpone-r1-post-install-statement-result-v2\0"


def object_coordinate(item: MssqlR1ObservedSchemaObjectV3) -> tuple[bytes, bytes]:
    portable = item.portable_object
    return (portable.schema_name.encode(), portable.object_name.encode())


def _leaf_tuple(values: object, loader, field: str, maximum: int, exact: int | None = None, key=None):
    decoded = decode_nested_tuple(values, loader, field, maximum=maximum, exact=exact, key=key)
    for item in decoded:
        bound_imported_graph(item)
    return decoded


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaQueryResultV1:
    database_identity: MssqlR1ObservedDatabaseIdentityV1
    ordered_schemas: tuple[MssqlR1ObservedSchemaV3, ...]
    ordered_principals: tuple[MssqlR1ObservedPrincipalV3, ...]
    ordered_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...]

    def __post_init__(self) -> None:
        if type(self.database_identity) is not MssqlR1ObservedDatabaseIdentityV1:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        if (
            len(self.ordered_schemas) > 8
            or len(self.ordered_principals) > 32
            or len(self.ordered_role_memberships) > 32
        ):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in (*self.ordered_schemas, *self.ordered_principals, *self.ordered_role_memberships):
            bound_imported_graph(item)
        require_canonical_unique(self.ordered_schemas, lambda item: item.schema_name.encode(), "schemas")
        require_canonical_unique(self.ordered_principals, lambda item: item.canonical_bytes, "principals")
        require_canonical_unique(self.ordered_role_memberships, lambda item: item.canonical_bytes, "memberships")
        emit_canonical(_SCHEMA, self._fields(), CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.database_identity.canonical_bytes,
            tuple(item.canonical_bytes for item in self.ordered_schemas),
            tuple(item.canonical_bytes for item in self.ordered_principals),
            tuple(item.canonical_bytes for item in self.ordered_role_memberships),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_SCHEMA, self._fields(), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaQueryResultV1:
        identity, schemas, principals, memberships = decode_attestation_model(payload, _SCHEMA, 4, CAP_QUERY)
        return cls(
            decode_nested_bytes(identity, MssqlR1ObservedDatabaseIdentityV1.from_canonical_bytes, "identity"),
            _leaf_tuple(schemas, MssqlR1ObservedSchemaV3.from_canonical_bytes, "schemas", 8),
            _leaf_tuple(principals, MssqlR1ObservedPrincipalV3.from_canonical_bytes, "principals", 32),
            _leaf_tuple(memberships, MssqlR1ObservedRoleMembershipV3.from_canonical_bytes, "memberships", 32),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1TableQueryResultV1:
    ordered_table_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...]

    def __post_init__(self) -> None:
        if len(self.ordered_table_objects) > 32:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in self.ordered_table_objects:
            bound_imported_graph(item)
        require_canonical_unique(self.ordered_table_objects, object_coordinate, "tables")
        emit_canonical(_TABLE, (tuple(item.canonical_bytes for item in self.ordered_table_objects),), CAP_QUERY)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_TABLE, (tuple(item.canonical_bytes for item in self.ordered_table_objects),), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1TableQueryResultV1:
        (objects,) = decode_attestation_model(payload, _TABLE, 1, CAP_QUERY)
        return cls(_leaf_tuple(objects, MssqlR1ObservedSchemaObjectV3.from_canonical_bytes, "tables", 32))


@dataclass(frozen=True, slots=True)
class MssqlR1ModuleQueryResultV1:
    ordered_core_module_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...]
    ordered_binding_modules: tuple[MssqlR1ObservedBindingModuleV1, ...]

    def __post_init__(self) -> None:
        if len(self.ordered_core_module_objects) > 32 or len(self.ordered_binding_modules) != 6:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in self.ordered_core_module_objects:
            bound_imported_graph(item)
        require_canonical_unique(self.ordered_core_module_objects, object_coordinate, "core modules")
        if any(type(item) is not MssqlR1ObservedBindingModuleV1 for item in self.ordered_binding_modules):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_MODULE, self._fields(), CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            tuple(item.canonical_bytes for item in self.ordered_core_module_objects),
            tuple(item.canonical_bytes for item in self.ordered_binding_modules),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_MODULE, self._fields(), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ModuleQueryResultV1:
        core, binding = decode_attestation_model(payload, _MODULE, 2, CAP_QUERY)
        return cls(
            _leaf_tuple(core, MssqlR1ObservedSchemaObjectV3.from_canonical_bytes, "core modules", 32),
            decode_nested_tuple(
                binding, MssqlR1ObservedBindingModuleV1.from_canonical_bytes, "binding", maximum=6, exact=6
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1SignatureQueryResultV1:
    ordered_core_signatures: tuple[MssqlR1ModuleSignatureObservationV3, ...]
    ordered_binding_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...]

    def __post_init__(self) -> None:
        if len(self.ordered_core_signatures) > 32 or len(self.ordered_binding_signatures) != 6:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in self.ordered_core_signatures:
            bound_imported_graph(item)
        require_canonical_unique(self.ordered_core_signatures, lambda item: item.module_object_id, "core signatures")
        if any(type(item) is not MssqlR1ObservedBindingSignatureV1 for item in self.ordered_binding_signatures):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_SIGNATURE, self._fields(), CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            tuple(item.canonical_bytes for item in self.ordered_core_signatures),
            tuple(item.canonical_bytes for item in self.ordered_binding_signatures),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_SIGNATURE, self._fields(), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SignatureQueryResultV1:
        core, binding = decode_attestation_model(payload, _SIGNATURE, 2, CAP_QUERY)
        return cls(
            _leaf_tuple(core, MssqlR1ModuleSignatureObservationV3.from_canonical_bytes, "core signatures", 32),
            decode_nested_tuple(
                binding,
                MssqlR1ObservedBindingSignatureV1.from_canonical_bytes,
                "binding signatures",
                maximum=6,
                exact=6,
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1BindingPrefixInventoryQueryResultV1:
    ordered_binding_prefix_inventory: tuple[MssqlR1ObservedBindingPrefixObjectV1, ...]

    def __post_init__(self) -> None:
        items = self.ordered_binding_prefix_inventory
        if len(items) > 64 or any(type(item) is not MssqlR1ObservedBindingPrefixObjectV1 for item in items):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_canonical_unique(items, lambda item: item.inventory_key(), "prefix inventory")
        emit_canonical(_PREFIX, (tuple(item.canonical_bytes for item in items),), CAP_QUERY)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(
            _PREFIX, (tuple(item.canonical_bytes for item in self.ordered_binding_prefix_inventory),), CAP_QUERY
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingPrefixInventoryQueryResultV1:
        (items,) = decode_attestation_model(payload, _PREFIX, 1, CAP_QUERY)
        return cls(
            decode_nested_tuple(items, MssqlR1ObservedBindingPrefixObjectV1.from_canonical_bytes, "prefix", maximum=64)
        )


@dataclass(frozen=True, slots=True)
class MssqlR1CertificateQueryResultV2:
    ordered_shared_certificates: tuple[MssqlR1CertificateCatalogObservationV1, ...]
    binding_signer: MssqlR1ObservedBindingSignerV2

    def __post_init__(self) -> None:
        certs = self.ordered_shared_certificates
        if len(certs) != 2 or any(type(item) is not MssqlR1CertificateCatalogObservationV1 for item in certs):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in certs:
            bound_imported_graph(item)
        if type(self.binding_signer) is not MssqlR1ObservedBindingSignerV2:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_CERT, self._fields(), CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            tuple(item.canonical_bytes for item in self.ordered_shared_certificates),
            self.binding_signer.canonical_bytes,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_CERT, self._fields(), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CertificateQueryResultV2:
        certs, signer = decode_attestation_model(payload, _CERT, 2, CAP_QUERY)
        return cls(
            _leaf_tuple(certs, MssqlR1CertificateCatalogObservationV1.from_canonical_bytes, "certificates", 2, 2),
            decode_nested_bytes(signer, MssqlR1ObservedBindingSignerV2.from_canonical_bytes, "signer"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionQueryResultV2:
    ordered_observed_permission_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]

    def __post_init__(self) -> None:
        paths = self.ordered_observed_permission_paths
        if len(paths) > 256 or any(type(item) is not MssqlR1ResolvedPermissionPathV2 for item in paths):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        for item in paths:
            bound_imported_graph(item)
        require_canonical_unique(paths, lambda item: item.canonical_bytes, "permission paths")
        emit_canonical(_PERM, (tuple(item.canonical_bytes for item in paths),), CAP_QUERY)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(
            _PERM, (tuple(item.canonical_bytes for item in self.ordered_observed_permission_paths),), CAP_QUERY
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionQueryResultV2:
        (paths,) = decode_attestation_model(payload, _PERM, 1, CAP_QUERY)
        return cls(_leaf_tuple(paths, MssqlR1ResolvedPermissionPathV2.from_canonical_bytes, "paths", 256))


RESULT_TYPES = {
    MssqlR1AttestationQueryKindV1.SCHEMA: MssqlR1SchemaQueryResultV1,
    MssqlR1AttestationQueryKindV1.TABLE: MssqlR1TableQueryResultV1,
    MssqlR1AttestationQueryKindV1.MODULE: MssqlR1ModuleQueryResultV1,
    MssqlR1AttestationQueryKindV1.CERTIFICATE: MssqlR1CertificateQueryResultV2,
    MssqlR1AttestationQueryKindV1.PERMISSION: MssqlR1PermissionQueryResultV2,
    MssqlR1AttestationQueryKindV1.SIGNATURE: MssqlR1SignatureQueryResultV1,
    MssqlR1AttestationQueryKindV1.BINDING_PREFIX_INVENTORY: MssqlR1BindingPrefixInventoryQueryResultV1,
}


@dataclass(frozen=True, slots=True)
class MssqlR1PostInstallStatementResultV2:
    statement_ref: MssqlR1MigrationStatementRefV1
    attestation_kind: MssqlR1AttestationQueryKindV1
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1
    query_definition_digest: bytes
    typed_result: object

    def __post_init__(self) -> None:
        if type(self.statement_ref) is not MssqlR1MigrationStatementRefV1:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        if self.statement_ref.phase is not MssqlR1MigrationStatementPhaseV1.POST_DECISION_ATTEST:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        if (self.attestation_kind, self.result_authority_kind) not in QUERY_RESULT_PAIRS:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        require_digest32(self.query_definition_digest, "query_definition_digest")
        expected = RESULT_TYPES[self.attestation_kind]
        if type(self.typed_result) is not expected:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        emit_canonical(_POST, self._fields(), CAP_POST)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.statement_ref.canonical_bytes,
            self.attestation_kind,
            self.result_authority_kind,
            self.query_definition_digest,
            self.typed_result.canonical_bytes,  # type: ignore[attr-defined]
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_POST, self._fields(), CAP_POST)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PostInstallStatementResultV2:
        ref, kind, result_kind, digest, typed = decode_attestation_model(payload, _POST, 5, CAP_POST)
        query_kind = expect_attestation_enum(MssqlR1AttestationQueryKindV1, kind, "kind")
        return cls(
            decode_nested_bytes(ref, MssqlR1MigrationStatementRefV1.from_canonical_bytes, "statement_ref"),
            query_kind,
            expect_attestation_enum(MssqlR1AttestationResultAuthorityKindV1, result_kind, "result"),
            digest,  # type: ignore[arg-type]
            decode_nested_bytes(typed, RESULT_TYPES[query_kind].from_canonical_bytes, "typed_result"),  # type: ignore[attr-defined]
        )


__all__ = (
    "MssqlR1BindingPrefixInventoryQueryResultV1",
    "MssqlR1CertificateQueryResultV2",
    "MssqlR1ModuleQueryResultV1",
    "MssqlR1PermissionQueryResultV2",
    "MssqlR1PostInstallStatementResultV2",
    "MssqlR1SchemaQueryResultV1",
    "MssqlR1SignatureQueryResultV1",
    "MssqlR1TableQueryResultV1",
    "MssqlR1CertifiedServerBuildProfileV1",
    "MssqlR1MigrationStatementRefV1",
    "MssqlR1MigrationTargetRefV1",
    "MssqlR1ObservedBindingModuleV1",
    "MssqlR1ObservedBindingPrefixObjectV1",
    "MssqlR1ObservedBindingSignatureV1",
    "MssqlR1ObservedBindingSignerV2",
    "MssqlR1ObservedDatabaseIdentityV1",
    "MssqlR1ProviderAttestationStatementRegistryV2",
    "MssqlR1CertificateCatalogObservationV1",
    "MssqlR1ResolvedPermissionPathV2",
    "QUERY_KIND_ORDER",
    "project_schema_permission_rule",
    "MssqlR1ModuleSignatureObservationV3",
    "MssqlR1ObservedPrincipalV3",
    "MssqlR1ObservedRoleMembershipV3",
    "MssqlR1ObservedSchemaObjectV3",
    "MssqlR1ObservedSchemaV3",
    "RESULT_TYPES",
    "Reason",
    "attestation_fail",
    "bound_imported_graph",
    "canonical_encoded_size_v1",
    "decode_attestation_model",
    "decode_nested_bytes",
    "decode_nested_tuple",
    "emit_canonical",
    "object_coordinate",
    "preflight_provider_attestation_canonical_v2",
    "require_canonical_unique",
    "require_digest32",
    "require_nonzero_uuid",
    "require_sql_positive",
)
