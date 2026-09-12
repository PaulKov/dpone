"""Target, statement and renderer-neutral registry authority for Attestation V2."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
    QUERY_KIND_ORDER,
    QUERY_RESULT_PAIRS,
    MssqlR1AttestationQueryKindV1,
    MssqlR1AttestationResultAuthorityKindV1,
    MssqlR1MigrationStatementPhaseV1,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_errors import (  # noqa: F401
    CAP_LEAF,
    CAP_REGISTRY,
    CAP_STATEMENT,
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
    require_bounded_text,
    require_canonical_unique,
    require_digest32,
    require_nonzero_uuid,
    require_opaque_utf8,
    require_sql_positive,
)
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1ModuleOptionsV3

PROFILE_ID = "sqlserver-2022-standalone-r1"
TARGET_CONTRACT = "dpone-mssql-target-physical-identity-1"
STATEMENT_CONTRACT = "dpone-r1-provider-attestation-statement-2"
REGISTRY_CONTRACT = "dpone-r1-provider-attestation-registry-2"
_PROFILE = b"dpone-r1-certified-server-build-profile-v1\0"
_TARGET = b"dpone-r1-provider-migration-target-ref-v1\0"
_IDENTITY = b"dpone-r1-observed-database-identity-v1\0"
_PROJECTION = b"dpone-r1-provider-target-database-identity-v1\0"
_STATEMENT_REF = b"dpone-r1-provider-migration-statement-ref-v1\0"
_MODULE_DEF = b"dpone-r1-provider-attestation-module-definition-v2\0"
_STATEMENT = b"dpone-r1-provider-attestation-statement-v2\0"
_REGISTRY = b"dpone-r1-provider-attestation-registry-v2\0"
MODULE_KIND_ORDER = tuple(MssqlR1BindingModuleKindV1)


_enum = expect_attestation_enum


@dataclass(frozen=True, slots=True)
class MssqlR1CertifiedServerBuildProfileV1:
    profile_id: Literal["sqlserver-2022-standalone-r1"]
    product_major_version: Literal[16]
    product_version_prefix: Literal["16."]
    database_compatibility_level: Literal[160]
    ordered_allowed_product_levels: tuple[str, ...]
    ordered_allowed_engine_editions: tuple[int, ...]

    def __post_init__(self) -> None:
        if (
            self.profile_id != PROFILE_ID
            or self.product_major_version != 16
            or self.product_version_prefix != "16."
            or self.database_compatibility_level != 160
        ):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        levels = tuple(require_bounded_text(item, "product level") for item in self.ordered_allowed_product_levels)
        editions = tuple(require_sql_positive(item, "engine edition") for item in self.ordered_allowed_engine_editions)
        if not levels or not editions or len(levels) > 16 or len(editions) > 16:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_canonical_unique(levels, lambda item: item.encode(), "product levels")
        require_canonical_unique(editions, lambda item: item, "engine editions")
        emit_canonical(_PROFILE, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.profile_id,
            self.product_major_version,
            self.product_version_prefix,
            self.database_compatibility_level,
            self.ordered_allowed_product_levels,
            self.ordered_allowed_engine_editions,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_PROFILE, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CertifiedServerBuildProfileV1:
        values = list(decode_attestation_model(payload, _PROFILE, 6, CAP_LEAF))
        return cls(
            values[0],  # type: ignore[arg-type]
            values[1],  # type: ignore[arg-type]
            values[2],  # type: ignore[arg-type]
            values[3],  # type: ignore[arg-type]
            tuple(values[4]),  # type: ignore[arg-type]
            tuple(values[5]),  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class MssqlR1MigrationTargetRefV1:
    identity_contract_version: Literal["dpone-mssql-target-physical-identity-1"]
    target_database_identity_digest: bytes

    def __post_init__(self) -> None:
        if self.identity_contract_version != TARGET_CONTRACT:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_digest32(self.target_database_identity_digest, "target digest")
        emit_canonical(_TARGET, (self.identity_contract_version, self.target_database_identity_digest), CAP_LEAF)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_TARGET, (self.identity_contract_version, self.target_database_identity_digest), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1MigrationTargetRefV1:
        version, digest = decode_attestation_model(payload, _TARGET, 2, CAP_LEAF)
        return cls(version, digest)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedDatabaseIdentityV1:
    server_instance_identity_sha256: bytes
    database_id: int
    database_guid: object
    database_family_guid: object
    recovery_fork_guid: object
    database_name: str
    database_name_digest: bytes
    collation_name: str
    compatibility_level: int
    product_version: str
    product_level: str
    engine_edition: int
    certified_server_build_profile: MssqlR1CertifiedServerBuildProfileV1

    def __post_init__(self) -> None:
        profile = self.certified_server_build_profile
        if type(profile) is not MssqlR1CertifiedServerBuildProfileV1:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_digest32(self.server_instance_identity_sha256, "server identity")
        require_sql_positive(self.database_id, "database_id")
        require_nonzero_uuid(self.database_guid, "database_guid")
        require_nonzero_uuid(self.database_family_guid, "database_family_guid")
        require_nonzero_uuid(self.recovery_fork_guid, "recovery_fork_guid")
        name = require_bounded_text(self.database_name, "database_name")
        require_digest32(self.database_name_digest, "database_name_digest")
        if self.database_name_digest != hashlib.sha256(name.encode("utf-8")).digest():
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        require_bounded_text(self.collation_name, "collation_name")
        version = require_bounded_text(self.product_version, "product_version")
        level = require_bounded_text(self.product_level, "product_level")
        edition = require_sql_positive(self.engine_edition, "engine_edition")
        if (
            self.compatibility_level != profile.database_compatibility_level
            or not version.startswith(profile.product_version_prefix)
            or level not in profile.ordered_allowed_product_levels
            or edition not in profile.ordered_allowed_engine_editions
        ):
            attestation_fail(Reason.ATTESTATION_TARGET_IDENTITY_MISMATCH)
        emit_canonical(_IDENTITY, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.server_instance_identity_sha256,
            self.database_id,
            self.database_guid,
            self.database_family_guid,
            self.recovery_fork_guid,
            self.database_name,
            self.database_name_digest,
            self.collation_name,
            self.compatibility_level,
            self.product_version,
            self.product_level,
            self.engine_edition,
            self.certified_server_build_profile.canonical_bytes,
        )

    def projected_target_digest(self) -> bytes:
        return hashlib.sha256(
            emit_canonical(
                _PROJECTION,
                (
                    self.server_instance_identity_sha256,
                    self.database_guid,
                    self.database_family_guid,
                    self.recovery_fork_guid,
                    self.database_name_digest,
                ),
                CAP_LEAF,
            )
        ).digest()

    def matches_target_ref(self, target: MssqlR1MigrationTargetRefV1) -> bool:
        return type(target) is MssqlR1MigrationTargetRefV1 and self.projected_target_digest() == (
            target.target_database_identity_digest
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_IDENTITY, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedDatabaseIdentityV1:
        values = list(decode_attestation_model(payload, _IDENTITY, 13, CAP_LEAF))
        values[12] = decode_nested_bytes(
            values[12], MssqlR1CertifiedServerBuildProfileV1.from_canonical_bytes, "profile"
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1MigrationStatementRefV1:
    statement_id: str
    statement_spec_digest: bytes
    phase: MssqlR1MigrationStatementPhaseV1

    def __post_init__(self) -> None:
        require_bounded_text(self.statement_id, "statement_id")
        require_digest32(self.statement_spec_digest, "statement_spec_digest")
        if not isinstance(self.phase, MssqlR1MigrationStatementPhaseV1):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_STATEMENT_REF, (self.statement_id, self.statement_spec_digest, self.phase), CAP_LEAF)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_STATEMENT_REF, (self.statement_id, self.statement_spec_digest, self.phase), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1MigrationStatementRefV1:
        statement_id, digest, phase = decode_attestation_model(payload, _STATEMENT_REF, 3, CAP_LEAF)
        return cls(
            statement_id,  # type: ignore[arg-type]
            digest,  # type: ignore[arg-type]
            _enum(MssqlR1MigrationStatementPhaseV1, phase, "phase"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ProviderAttestationModuleDefinitionAuthorityV2:
    module_kind: MssqlR1BindingModuleKindV1
    semantic_module_digest: bytes
    normalized_definition_bytes: bytes
    persisted_options: MssqlR1ModuleOptionsV3

    def __post_init__(self) -> None:
        if not isinstance(self.module_kind, MssqlR1BindingModuleKindV1):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        require_digest32(self.semantic_module_digest, "semantic_module_digest")
        raw = self.normalized_definition_bytes
        if not isinstance(raw, bytes) or not raw or len(raw) > 131072:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        if type(self.persisted_options) is not MssqlR1ModuleOptionsV3:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        bound_imported_graph(self.persisted_options)
        emit_canonical(_MODULE_DEF, self._fields(), CAP_LEAF)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.module_kind,
            self.semantic_module_digest,
            self.normalized_definition_bytes,
            self.persisted_options.canonical_bytes,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_MODULE_DEF, self._fields(), CAP_LEAF)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProviderAttestationModuleDefinitionAuthorityV2:
        kind, digest, definition, options = decode_attestation_model(payload, _MODULE_DEF, 4, CAP_LEAF)
        return cls(
            _enum(MssqlR1BindingModuleKindV1, kind, "module_kind"),
            digest,  # type: ignore[arg-type]
            definition,  # type: ignore[arg-type]
            decode_nested_bytes(options, MssqlR1ModuleOptionsV3.from_canonical_bytes, "options"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ProviderAttestationStatementAuthorityV2:
    contract_version: Literal["dpone-r1-provider-attestation-statement-2"]
    ordinal: int
    statement_ref: MssqlR1MigrationStatementRefV1
    attestation_kind: MssqlR1AttestationQueryKindV1
    result_authority_kind: MssqlR1AttestationResultAuthorityKindV1
    query_definition_utf8: bytes
    ordered_module_definitions: tuple[MssqlR1ProviderAttestationModuleDefinitionAuthorityV2, ...]

    def __post_init__(self) -> None:
        if self.contract_version != STATEMENT_CONTRACT or not 1 <= self.ordinal <= 7:
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        if type(self.statement_ref) is not MssqlR1MigrationStatementRefV1:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        if self.statement_ref.phase is not MssqlR1MigrationStatementPhaseV1.POST_DECISION_ATTEST:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        expected_kind, expected_result = QUERY_RESULT_PAIRS[self.ordinal - 1]
        if self.attestation_kind is not expected_kind or self.result_authority_kind is not expected_result:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        require_opaque_utf8(self.query_definition_utf8, "query_definition_utf8", maximum=131072)
        definitions = self.ordered_module_definitions
        if self.attestation_kind is MssqlR1AttestationQueryKindV1.MODULE:
            if len(definitions) != 6 or tuple(item.module_kind for item in definitions) != MODULE_KIND_ORDER:
                attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        elif definitions:
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        if any(type(item) is not MssqlR1ProviderAttestationModuleDefinitionAuthorityV2 for item in definitions):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_STATEMENT, self._fields(), CAP_STATEMENT)

    def query_definition_digest(self) -> bytes:
        return hashlib.sha256(self.query_definition_utf8).digest()

    def _fields(self) -> tuple[object, ...]:
        return (
            self.contract_version,
            self.ordinal,
            self.statement_ref.canonical_bytes,
            self.attestation_kind,
            self.result_authority_kind,
            self.query_definition_utf8,
            tuple(item.canonical_bytes for item in self.ordered_module_definitions),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_STATEMENT, self._fields(), CAP_STATEMENT)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProviderAttestationStatementAuthorityV2:
        values = list(decode_attestation_model(payload, _STATEMENT, 7, CAP_STATEMENT))
        return cls(
            values[0],  # type: ignore[arg-type]
            values[1],  # type: ignore[arg-type]
            decode_nested_bytes(values[2], MssqlR1MigrationStatementRefV1.from_canonical_bytes, "statement_ref"),
            _enum(MssqlR1AttestationQueryKindV1, values[3], "kind"),
            _enum(MssqlR1AttestationResultAuthorityKindV1, values[4], "result"),
            values[5],  # type: ignore[arg-type]
            decode_nested_tuple(
                values[6],
                MssqlR1ProviderAttestationModuleDefinitionAuthorityV2.from_canonical_bytes,
                "module definitions",
                maximum=6,
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ProviderAttestationStatementRegistryV2:
    contract_version: Literal["dpone-r1-provider-attestation-registry-2"]
    renderer_registry_digest: bytes
    ordered_statements: tuple[MssqlR1ProviderAttestationStatementAuthorityV2, ...]

    def __post_init__(self) -> None:
        if self.contract_version != REGISTRY_CONTRACT:
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        require_digest32(self.renderer_registry_digest, "renderer_registry_digest")
        statements = self.ordered_statements
        if len(statements) != 7 or any(
            type(item) is not MssqlR1ProviderAttestationStatementAuthorityV2 for item in statements
        ):
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        if tuple(item.ordinal for item in statements) != tuple(range(1, 8)):
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        if tuple(item.attestation_kind for item in statements) != QUERY_KIND_ORDER:
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        emit_canonical(_REGISTRY, self._fields(), CAP_REGISTRY)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.contract_version,
            self.renderer_registry_digest,
            tuple(item.canonical_bytes for item in self.ordered_statements),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_REGISTRY, self._fields(), CAP_REGISTRY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProviderAttestationStatementRegistryV2:
        version, digest, statements = decode_attestation_model(payload, _REGISTRY, 3, CAP_REGISTRY)
        return cls(
            version,  # type: ignore[arg-type]
            digest,  # type: ignore[arg-type]
            decode_nested_tuple(
                statements,
                MssqlR1ProviderAttestationStatementAuthorityV2.from_canonical_bytes,
                "statements",
                maximum=7,
                exact=7,
            ),
        )


__all__ = (
    "MssqlR1CertifiedServerBuildProfileV1",
    "MssqlR1MigrationStatementRefV1",
    "MssqlR1MigrationTargetRefV1",
    "MssqlR1ObservedDatabaseIdentityV1",
    "MssqlR1ProviderAttestationModuleDefinitionAuthorityV2",
    "MssqlR1ProviderAttestationStatementAuthorityV2",
    "MssqlR1ProviderAttestationStatementRegistryV2",
    "QUERY_KIND_ORDER",
    "QUERY_RESULT_PAIRS",
    "MssqlR1AttestationQueryKindV1",
    "MssqlR1AttestationResultAuthorityKindV1",
    "MssqlR1MigrationStatementPhaseV1",
    "Reason",
    "attestation_fail",
    "bound_imported_graph",
    "canonical_encoded_size_v1",
    "decode_attestation_model",
    "decode_nested_bytes",
    "decode_nested_tuple",
    "emit_canonical",
    "expect_attestation_enum",
    "preflight_provider_attestation_canonical_v2",
    "require_bounded_text",
    "require_canonical_unique",
    "require_digest32",
    "require_nonzero_uuid",
    "require_opaque_utf8",
    "require_sql_positive",
)
