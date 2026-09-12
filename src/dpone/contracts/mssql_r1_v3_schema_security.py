"""Portable security intent and registration-bound principal authority."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1SignerProfileKindV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    decode_members,
    expect_enum,
    require_canonical_set,
    require_digest,
    require_schema_identifier,
)

_PERMISSION_RULE = b"dpone-r1-schema-permission-rule-v3-schema-2\0"
_SIGNER_PROFILE = b"dpone-r1-schema-signer-profile-v3-schema-2\0"
_PRINCIPAL_BINDING = b"dpone-r1-schema-principal-binding-v3-schema-2\0"
_PRINCIPAL_SET = b"dpone-r1-schema-principal-set-v3-schema-2\0"
_RAW_SID = b"dpone-r1-schema-raw-sid-v3-schema-2\0"
_PERMISSION_NAME = re.compile(r"[A-Z]+(?: [A-Z]+)*\Z")


class MssqlR1SubjectRoleV3(StrEnum):
    PROVISIONER = "provisioner"
    RUNTIME = "runtime"
    LOADER = "loader"
    OBSERVER = "observer"
    STAGE_OWNER_MODULE = "stage_owner_module"
    ATTESTATION_MODULE = "attestation_module"


class MssqlR1PermissionSourceV3(StrEnum):
    DIRECT = "direct"
    DATABASE_ROLE = "database_role"
    PUBLIC = "public"


class MssqlR1PermissionScopeV3(StrEnum):
    DATABASE = "database"
    SCHEMA = "schema"
    OBJECT = "object"
    COLUMN = "column"


class MssqlR1PermissionEffectV3(StrEnum):
    GRANT = "grant"
    DENY = "deny"


class MssqlR1PrincipalTypeV3(StrEnum):
    SQL_USER = "sql_user"
    WINDOWS_USER = "windows_user"
    EXTERNAL_USER = "external_user"
    CERTIFICATE = "certificate"
    DATABASE_ROLE = "database_role"


class MssqlR1AuthenticationTypeV3(StrEnum):
    INSTANCE = "instance"
    DATABASE = "database"
    WINDOWS = "windows"
    EXTERNAL = "external"
    NONE = "none"


ENVIRONMENT_ROLES = (
    MssqlR1SubjectRoleV3.LOADER,
    MssqlR1SubjectRoleV3.OBSERVER,
    MssqlR1SubjectRoleV3.PROVISIONER,
    MssqlR1SubjectRoleV3.RUNTIME,
)
MODULE_ROLE_BY_PROFILE = {
    MssqlR1SignerProfileKindV3.ATTESTOR: MssqlR1SubjectRoleV3.ATTESTATION_MODULE,
    MssqlR1SignerProfileKindV3.STAGE_OWNER: MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE,
}
_ALLOWED_USER_PAIRS = {
    (MssqlR1PrincipalTypeV3.SQL_USER, MssqlR1AuthenticationTypeV3.INSTANCE),
    (MssqlR1PrincipalTypeV3.SQL_USER, MssqlR1AuthenticationTypeV3.DATABASE),
    (MssqlR1PrincipalTypeV3.WINDOWS_USER, MssqlR1AuthenticationTypeV3.WINDOWS),
    (MssqlR1PrincipalTypeV3.EXTERNAL_USER, MssqlR1AuthenticationTypeV3.EXTERNAL),
}


def raw_sid_digest(raw_sid: bytes) -> bytes:
    if not isinstance(raw_sid, bytes) or not raw_sid:
        raise MssqlR1V3ContractError("raw SID must be nonempty bytes")
    return hashlib.sha256(canonical_bytes(_RAW_SID, (raw_sid,))).digest()


def require_permission_name(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or value != value.upper()
        or len(value.encode()) > 128
        or _PERMISSION_NAME.fullmatch(value) is None
    ):
        raise MssqlR1V3ContractError("permission must be bounded uppercase ASCII")
    return value


def validate_permission_coordinates(
    scope: MssqlR1PermissionScopeV3,
    schema_name: str | None,
    object_name: str | None,
    column_name: str | None,
) -> None:
    expected = {
        MssqlR1PermissionScopeV3.DATABASE: (False, False, False),
        MssqlR1PermissionScopeV3.SCHEMA: (True, False, False),
        MssqlR1PermissionScopeV3.OBJECT: (True, True, False),
        MssqlR1PermissionScopeV3.COLUMN: (True, True, True),
    }[scope]
    values = (schema_name, object_name, column_name)
    if tuple(value is not None for value in values) != expected:
        raise MssqlR1V3ContractError("permission scope coordinates are inconsistent")
    for value, field in zip(values, ("schema_name", "object_name", "column_name"), strict=True):
        if value is not None:
            require_schema_identifier(value, field)


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionRuleV3:
    subject_role: MssqlR1SubjectRoleV3
    grantor_role: MssqlR1SubjectRoleV3
    source: MssqlR1PermissionSourceV3
    scope: MssqlR1PermissionScopeV3
    schema_name: str | None
    object_name: str | None
    column_name: str | None
    permission: str
    effect: MssqlR1PermissionEffectV3
    grant_option: bool

    def __post_init__(self) -> None:
        if not isinstance(self.subject_role, MssqlR1SubjectRoleV3):
            raise MssqlR1V3ContractError("permission subject role is unsupported")
        if not isinstance(self.grantor_role, MssqlR1SubjectRoleV3) or (
            self.grantor_role is not MssqlR1SubjectRoleV3.PROVISIONER
        ):
            raise MssqlR1V3ContractError("permission grantor must be provisioner")
        if (
            not isinstance(self.source, MssqlR1PermissionSourceV3)
            or self.source is not MssqlR1PermissionSourceV3.DIRECT
        ):
            raise MssqlR1V3ContractError("portable permissions must be direct")
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
        return canonical_bytes(_PERMISSION_RULE, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionRuleV3:
        values = list(decode_canonical_bytes(payload, _PERMISSION_RULE, field_count=10))
        for index, enum, field in (
            (0, MssqlR1SubjectRoleV3, "subject role"),
            (1, MssqlR1SubjectRoleV3, "grantor role"),
            (2, MssqlR1PermissionSourceV3, "permission source"),
            (3, MssqlR1PermissionScopeV3, "permission scope"),
            (8, MssqlR1PermissionEffectV3, "permission effect"),
        ):
            values[index] = expect_enum(enum, values[index], field)
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SignerProfileV3:
    signer_profile: MssqlR1SignerProfileKindV3
    certificate_name: str
    certificate_user_name: str

    def __post_init__(self) -> None:
        if not isinstance(self.signer_profile, MssqlR1SignerProfileKindV3) or (
            self.signer_profile not in MODULE_ROLE_BY_PROFILE
        ):
            raise MssqlR1V3ContractError("signer profile is unsupported")
        require_schema_identifier(self.certificate_name, "certificate name")
        require_schema_identifier(self.certificate_user_name, "certificate user name")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SIGNER_PROFILE,
            (self.signer_profile, self.certificate_name, self.certificate_user_name),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SignerProfileV3:
        profile, certificate, user = decode_canonical_bytes(payload, _SIGNER_PROFILE, field_count=3)
        return cls(expect_enum(MssqlR1SignerProfileKindV3, profile, "signer profile"), certificate, user)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PrincipalBindingV3:
    subject_role: MssqlR1SubjectRoleV3
    database_principal_name: str
    database_principal_sid_digest: bytes
    principal_type: MssqlR1PrincipalTypeV3
    authentication_type: MssqlR1AuthenticationTypeV3
    server_principal_sid_digest: bytes | None
    ordered_allowed_database_roles: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.subject_role, MssqlR1SubjectRoleV3) or self.subject_role not in ENVIRONMENT_ROLES:
            raise MssqlR1V3ContractError("principal binding role is unsupported")
        if not isinstance(self.principal_type, MssqlR1PrincipalTypeV3) or not isinstance(
            self.authentication_type, MssqlR1AuthenticationTypeV3
        ):
            raise MssqlR1V3ContractError("principal type/authentication discriminator is unsupported")
        require_schema_identifier(self.database_principal_name, "database principal name")
        require_digest(self.database_principal_sid_digest, "database principal SID digest")
        if (self.principal_type, self.authentication_type) not in _ALLOWED_USER_PAIRS:
            raise MssqlR1V3ContractError("principal type/authentication pair is unsupported")
        needs_server_sid = self.authentication_type in {
            MssqlR1AuthenticationTypeV3.INSTANCE,
            MssqlR1AuthenticationTypeV3.WINDOWS,
        }
        if needs_server_sid != (self.server_principal_sid_digest is not None):
            raise MssqlR1V3ContractError("server principal SID presence is inconsistent")
        if self.server_principal_sid_digest is not None:
            require_digest(self.server_principal_sid_digest, "server principal SID digest")
        roles = self.ordered_allowed_database_roles
        if not isinstance(roles, tuple):
            raise MssqlR1V3ContractError("allowed database roles must be a tuple")
        for role in roles:
            require_schema_identifier(role, "allowed database role")
        if roles != tuple(sorted(roles, key=lambda value: value.encode())) or len(set(roles)) != len(roles):
            raise MssqlR1V3ContractError("allowed database roles must use strict UTF-8 order")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_PRINCIPAL_BINDING, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PrincipalBindingV3:
        values = list(decode_canonical_bytes(payload, _PRINCIPAL_BINDING, field_count=7))
        values[0] = expect_enum(MssqlR1SubjectRoleV3, values[0], "subject role")
        values[3] = expect_enum(MssqlR1PrincipalTypeV3, values[3], "principal type")
        values[4] = expect_enum(MssqlR1AuthenticationTypeV3, values[4], "authentication type")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1PrincipalAuthoritySetV3:
    registration_payload_digest: bytes
    resolved_profile_digest: bytes
    ordered_bindings: tuple[MssqlR1PrincipalBindingV3, ...]

    def __post_init__(self) -> None:
        require_digest(self.registration_payload_digest, "registration payload digest")
        require_digest(self.resolved_profile_digest, "resolved profile digest")
        require_canonical_set(self.ordered_bindings, MssqlR1PrincipalBindingV3, "principal bindings")
        if tuple(sorted(binding.subject_role for binding in self.ordered_bindings)) != tuple(sorted(ENVIRONMENT_ROLES)):
            raise MssqlR1V3ContractError("principal bindings must contain every environment role exactly once")
        names = tuple(binding.database_principal_name for binding in self.ordered_bindings)
        sids = tuple(binding.database_principal_sid_digest for binding in self.ordered_bindings)
        if len(set(names)) != 4 or len(set(sids)) != 4:
            raise MssqlR1V3ContractError("principal binding names and database SIDs must be pairwise distinct")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PRINCIPAL_SET,
            (
                self.registration_payload_digest,
                self.resolved_profile_digest,
                tuple(binding.canonical_bytes for binding in self.ordered_bindings),
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def create(
        cls,
        verification: MssqlTargetRegistrationVerificationV1,
        ordered_bindings: tuple[MssqlR1PrincipalBindingV3, ...],
    ) -> MssqlR1PrincipalAuthoritySetV3:
        if not isinstance(verification, MssqlTargetRegistrationVerificationV1):
            raise MssqlR1V3ContractError("principal authority requires verified registration")
        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(verification.payload_bytes)
        if registration.canonical_bytes != verification.payload_bytes:
            raise MssqlR1V3ContractError("principal authority registration is noncanonical")
        return cls(verification.registration_payload_digest, registration.resolved_profile_digest, ordered_bindings)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PrincipalAuthoritySetV3:
        registration, profile, bindings = decode_canonical_bytes(payload, _PRINCIPAL_SET, field_count=3)
        return cls(
            registration,  # type: ignore[arg-type]
            profile,  # type: ignore[arg-type]
            decode_members(bindings, MssqlR1PrincipalBindingV3, "principal binding"),
        )


__all__ = [name for name in tuple(globals()) if name.startswith("MssqlR1")] + [
    "ENVIRONMENT_ROLES",
    "MODULE_ROLE_BY_PROFILE",
    "raw_sid_digest",
    "validate_permission_coordinates",
]
