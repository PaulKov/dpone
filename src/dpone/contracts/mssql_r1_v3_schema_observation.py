"""Environment-bound schema and security observations for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1PortableSchemaObjectV3
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1SignerProfileKindV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    decode_members,
    expect_bytes,
    expect_enum,
    require_canonical_set,
    require_digest,
    require_schema_identifier,
    require_sql_int,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    ENVIRONMENT_ROLES,
    MODULE_ROLE_BY_PROFILE,
    MssqlR1AuthenticationTypeV3,
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1PrincipalTypeV3,
    MssqlR1SubjectRoleV3,
    require_permission_name,
    validate_permission_coordinates,
)

_PREFIX = b"dpone-r1-schema-observed-"
_SUFFIX = b"-v3-schema-2\0"
_DOMAINS = {
    name: _PREFIX + name.encode() + _SUFFIX
    for name in ("schema", "object", "trigger", "principal", "membership", "permission")
}
_SIGNATURE = b"dpone-r1-schema-signature-observation-v3-schema-2\0"


class MssqlR1ObservedPrincipalAuthorityKindV3(StrEnum):
    ENVIRONMENT = "environment"
    SIGNER = "signer"
    DATABASE_ROLE = "database_role"
    PUBLIC = "public"


def raw_thumbprint_digest(raw_thumbprint: bytes) -> bytes:
    return _raw_digest(raw_thumbprint, b"dpone-r1-schema-raw-thumbprint-v3-schema-2\0", "thumbprint")


def raw_crypt_property_digest(raw_crypt_property: bytes) -> bytes:
    return _raw_digest(
        raw_crypt_property,
        b"dpone-r1-schema-raw-crypt-property-v3-schema-2\0",
        "crypt property",
    )


def _raw_digest(value: bytes, domain: bytes, field: str) -> bytes:
    if not isinstance(value, bytes) or not value:
        raise MssqlR1V3ContractError(f"raw {field} must be nonempty bytes")
    return hashlib.sha256(canonical_bytes(domain, (value,))).digest()


def _optional_sql_int(value: int | None, field: str) -> None:
    if value is not None:
        require_sql_int(value, field)


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedSchemaV3:
    schema_name: str
    schema_id: int
    declared_owner_principal_id: int
    effective_owner_principal_id: int
    effective_owner_sid_digest: bytes

    def __post_init__(self) -> None:
        require_schema_identifier(self.schema_name, "observed schema name")
        for name in ("schema_id", "declared_owner_principal_id", "effective_owner_principal_id"):
            require_sql_int(getattr(self, name), name)
        require_digest(self.effective_owner_sid_digest, "effective owner SID digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["schema"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedSchemaV3:
        return cls(*decode_canonical_bytes(payload, _DOMAINS["schema"], field_count=5))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedTriggerIdentityV3:
    portable_trigger_digest: bytes
    schema_id: int
    object_id: int
    parent_object_id: int

    def __post_init__(self) -> None:
        require_digest(self.portable_trigger_digest, "portable trigger digest")
        for name in ("schema_id", "object_id", "parent_object_id"):
            require_sql_int(getattr(self, name), name)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["trigger"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedTriggerIdentityV3:
        return cls(*decode_canonical_bytes(payload, _DOMAINS["trigger"], field_count=4))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ModuleSignatureObservationV3:
    signer_profile: MssqlR1SignerProfileKindV3
    module_object_id: int
    certificate_name: str
    certificate_id: int
    certificate_thumbprint_digest: bytes
    certificate_user_principal_id: int
    certificate_user_sid_digest: bytes
    crypt_property_digest: bytes
    private_key_present: bool

    def __post_init__(self) -> None:
        if not isinstance(self.signer_profile, MssqlR1SignerProfileKindV3) or (
            self.signer_profile not in MODULE_ROLE_BY_PROFILE
        ):
            raise MssqlR1V3ContractError("signature signer profile is unsupported")
        for name in ("module_object_id", "certificate_id", "certificate_user_principal_id"):
            require_sql_int(getattr(self, name), name)
        require_schema_identifier(self.certificate_name, "certificate name")
        for name in ("certificate_thumbprint_digest", "certificate_user_sid_digest", "crypt_property_digest"):
            require_digest(getattr(self, name), name)
        if self.private_key_present is not False:
            raise MssqlR1V3ContractError("installed signing certificate must not retain a private key")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_SIGNATURE, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ModuleSignatureObservationV3:
        values = list(decode_canonical_bytes(payload, _SIGNATURE, field_count=9))
        values[0] = expect_enum(MssqlR1SignerProfileKindV3, values[0], "signer profile")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedSchemaObjectV3:
    portable_object: MssqlR1PortableSchemaObjectV3
    schema_id: int
    object_id: int
    declared_owner_principal_id: int | None
    effective_owner_principal_id: int
    effective_owner_sid_digest: bytes
    ordered_trigger_identities: tuple[MssqlR1ObservedTriggerIdentityV3, ...]
    ordered_signatures: tuple[MssqlR1ModuleSignatureObservationV3, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.portable_object, MssqlR1PortableSchemaObjectV3):
            raise MssqlR1V3ContractError("observed object requires a portable object")
        for name in ("schema_id", "object_id", "effective_owner_principal_id"):
            require_sql_int(getattr(self, name), name)
        if self.declared_owner_principal_id is not None:
            raise MssqlR1V3ContractError("R1 forbids explicit per-object owners")
        require_digest(self.effective_owner_sid_digest, "effective owner SID digest")
        require_canonical_set(
            self.ordered_signatures,
            MssqlR1ModuleSignatureObservationV3,
            "module signatures",
        )
        if not isinstance(self.ordered_trigger_identities, tuple) or not all(
            isinstance(item, MssqlR1ObservedTriggerIdentityV3) for item in self.ordered_trigger_identities
        ):
            raise MssqlR1V3ContractError("observed trigger identities must be a typed tuple")
        order = tuple((item.portable_trigger_digest, item.object_id) for item in self.ordered_trigger_identities)
        if order != tuple(sorted(order)) or len(set(order)) != len(order):
            raise MssqlR1V3ContractError("observed triggers must use strict digest/object order")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAINS["object"],
            (
                self.portable_object.canonical_bytes,
                self.schema_id,
                self.object_id,
                self.declared_owner_principal_id,
                self.effective_owner_principal_id,
                self.effective_owner_sid_digest,
                tuple(item.canonical_bytes for item in self.ordered_trigger_identities),
                tuple(item.canonical_bytes for item in self.ordered_signatures),
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedSchemaObjectV3:
        values = list(decode_canonical_bytes(payload, _DOMAINS["object"], field_count=8))
        values[0] = MssqlR1PortableSchemaObjectV3.from_canonical_bytes(expect_bytes(values[0], "portable object"))
        values[6] = decode_members(values[6], MssqlR1ObservedTriggerIdentityV3, "trigger identity")
        values[7] = decode_members(values[7], MssqlR1ModuleSignatureObservationV3, "signature")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedPrincipalV3:
    authority_kind: MssqlR1ObservedPrincipalAuthorityKindV3
    subject_role: MssqlR1SubjectRoleV3 | None
    principal_name: str
    principal_id: int
    database_sid_digest: bytes
    server_sid_digest: bytes | None
    principal_type: MssqlR1PrincipalTypeV3
    authentication_type: MssqlR1AuthenticationTypeV3
    owning_principal_id: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.authority_kind, MssqlR1ObservedPrincipalAuthorityKindV3):
            raise MssqlR1V3ContractError("principal authority kind is unsupported")
        if self.subject_role is not None and not isinstance(self.subject_role, MssqlR1SubjectRoleV3):
            raise MssqlR1V3ContractError("principal subject role is unsupported")
        if not isinstance(self.principal_type, MssqlR1PrincipalTypeV3) or not isinstance(
            self.authentication_type, MssqlR1AuthenticationTypeV3
        ):
            raise MssqlR1V3ContractError("principal type/authentication discriminator is unsupported")
        require_schema_identifier(self.principal_name, "principal name")
        require_sql_int(self.principal_id, "principal_id")
        require_digest(self.database_sid_digest, "database SID digest")
        if self.server_sid_digest is not None:
            require_digest(self.server_sid_digest, "server SID digest")
        _optional_sql_int(self.owning_principal_id, "owning_principal_id")
        self._validate_discriminator()

    def _validate_discriminator(self) -> None:
        pair = (self.principal_type, self.authentication_type)
        if self.authority_kind is MssqlR1ObservedPrincipalAuthorityKindV3.ENVIRONMENT:
            if self.subject_role not in ENVIRONMENT_ROLES or pair not in {
                (MssqlR1PrincipalTypeV3.SQL_USER, MssqlR1AuthenticationTypeV3.INSTANCE),
                (MssqlR1PrincipalTypeV3.SQL_USER, MssqlR1AuthenticationTypeV3.DATABASE),
                (MssqlR1PrincipalTypeV3.WINDOWS_USER, MssqlR1AuthenticationTypeV3.WINDOWS),
                (MssqlR1PrincipalTypeV3.EXTERNAL_USER, MssqlR1AuthenticationTypeV3.EXTERNAL),
            }:
                raise MssqlR1V3ContractError("environment principal discriminator is inconsistent")
            needs_server = self.authentication_type in {
                MssqlR1AuthenticationTypeV3.INSTANCE,
                MssqlR1AuthenticationTypeV3.WINDOWS,
            }
            if needs_server != (self.server_sid_digest is not None):
                raise MssqlR1V3ContractError("environment principal server SID is inconsistent")
        elif self.authority_kind is MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER:
            if (
                self.subject_role not in MODULE_ROLE_BY_PROFILE.values()
                or pair != (MssqlR1PrincipalTypeV3.CERTIFICATE, MssqlR1AuthenticationTypeV3.NONE)
                or self.server_sid_digest is not None
            ):
                raise MssqlR1V3ContractError("signer principal discriminator is inconsistent")
        elif (
            self.subject_role is not None
            or pair != (MssqlR1PrincipalTypeV3.DATABASE_ROLE, MssqlR1AuthenticationTypeV3.NONE)
            or self.server_sid_digest is not None
        ):
            raise MssqlR1V3ContractError("database role/public principal discriminator is inconsistent")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["principal"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedPrincipalV3:
        values = list(decode_canonical_bytes(payload, _DOMAINS["principal"], field_count=9))
        values[0] = expect_enum(MssqlR1ObservedPrincipalAuthorityKindV3, values[0], "authority kind")
        if values[1] is not None:
            values[1] = expect_enum(MssqlR1SubjectRoleV3, values[1], "subject role")
        values[6] = expect_enum(MssqlR1PrincipalTypeV3, values[6], "principal type")
        values[7] = expect_enum(MssqlR1AuthenticationTypeV3, values[7], "authentication type")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedRoleMembershipV3:
    member_principal_id: int
    member_sid_digest: bytes
    role_principal_id: int
    role_sid_digest: bytes
    role_name: str

    def __post_init__(self) -> None:
        require_sql_int(self.member_principal_id, "member principal ID")
        require_digest(self.member_sid_digest, "member SID digest")
        require_sql_int(self.role_principal_id, "role principal ID")
        require_digest(self.role_sid_digest, "role SID digest")
        require_schema_identifier(self.role_name, "role name")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["membership"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedRoleMembershipV3:
        return cls(*decode_canonical_bytes(payload, _DOMAINS["membership"], field_count=5))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ObservedPermissionV3:
    source: MssqlR1PermissionSourceV3
    source_role_name: str | None
    grantee_principal_id: int
    grantee_sid_digest: bytes
    grantor_principal_id: int
    grantor_sid_digest: bytes
    scope: MssqlR1PermissionScopeV3
    schema_name: str | None
    object_name: str | None
    column_name: str | None
    permission: str
    effect: MssqlR1PermissionEffectV3
    grant_option: bool

    def __post_init__(self) -> None:
        if not isinstance(self.source, MssqlR1PermissionSourceV3):
            raise MssqlR1V3ContractError("permission source is unsupported")
        if (self.source is MssqlR1PermissionSourceV3.DATABASE_ROLE) != (self.source_role_name is not None):
            raise MssqlR1V3ContractError("permission source role is inconsistent")
        if self.source_role_name is not None:
            require_schema_identifier(self.source_role_name, "permission source role")
        for name in ("grantee_principal_id", "grantor_principal_id"):
            require_sql_int(getattr(self, name), name)
        require_digest(self.grantee_sid_digest, "grantee SID digest")
        require_digest(self.grantor_sid_digest, "grantor SID digest")
        if not isinstance(self.scope, MssqlR1PermissionScopeV3) or not isinstance(
            self.effect, MssqlR1PermissionEffectV3
        ):
            raise MssqlR1V3ContractError("permission discriminator is unsupported")
        validate_permission_coordinates(self.scope, self.schema_name, self.object_name, self.column_name)
        require_permission_name(self.permission)
        if not isinstance(self.grant_option, bool):
            raise MssqlR1V3ContractError("grant_option must be boolean")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["permission"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ObservedPermissionV3:
        values = list(decode_canonical_bytes(payload, _DOMAINS["permission"], field_count=13))
        for index, enum, field in (
            (0, MssqlR1PermissionSourceV3, "permission source"),
            (6, MssqlR1PermissionScopeV3, "permission scope"),
            (11, MssqlR1PermissionEffectV3, "permission effect"),
        ):
            values[index] = expect_enum(enum, values[index], field)
        return cls(*values)  # type: ignore[arg-type]


__all__ = [name for name in tuple(globals()) if name.startswith("MssqlR1")] + [
    "raw_crypt_property_digest",
    "raw_thumbprint_digest",
]
