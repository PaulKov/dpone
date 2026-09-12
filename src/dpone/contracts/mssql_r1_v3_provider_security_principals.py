"""Exact principals, permission origins and secret policy for MSSQL R1."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TypeAlias, cast
from uuid import UUID

from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MssqlR1PermissionTargetScopeV1,
    MssqlR1SecretGeneratorProfileV1,
    MssqlR1SecretLifetimePolicyV1,
    MssqlR1SecretLoggingPolicyV1,
    MssqlR1SecretPersistencePolicyV1,
    MssqlR1SecurityCanonicalModel,
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    require_exact_digest,
    require_exact_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SignerProfileKindV3, require_schema_identifier
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1SignerProfileV3, MssqlR1SubjectRoleV3

_DOMAINS = {
    "environment": b"dpone-r1-security-environment-principal-ref-v1\0",
    "shared": b"dpone-r1-security-shared-signer-principal-ref-v1\0",
    "binding_class": b"dpone-r1-security-binding-signer-class-ref-v1\0",
    "binding_instance": b"dpone-r1-security-binding-signer-instance-ref-v1\0",
    "public": b"dpone-r1-security-public-principal-ref-v1\0",
    "named_role": b"dpone-r1-security-named-database-role-ref-v1\0",
    "any_role": b"dpone-r1-security-any-database-role-ref-v1\0",
    "direct_origin": b"dpone-r1-security-direct-permission-origin-v1\0",
    "role_origin": b"dpone-r1-security-role-permission-origin-v1\0",
    "public_origin": b"dpone-r1-security-public-permission-origin-v1\0",
    "signer_profile": b"dpone-r1-security-shared-signer-profile-ref-v1\0",
    "secret": b"dpone-r1-security-ephemeral-secret-policy-v1\0",
    "target": b"dpone-r1-security-permission-target-v1\0",
    "membership": b"dpone-r1-security-forbidden-role-membership-v1\0",
}
ENVIRONMENT_SUBJECT_ROLES = (
    MssqlR1SubjectRoleV3.PROVISIONER,
    MssqlR1SubjectRoleV3.RUNTIME,
    MssqlR1SubjectRoleV3.LOADER,
    MssqlR1SubjectRoleV3.OBSERVER,
)
SHARED_SIGNER_PROFILES = (MssqlR1SignerProfileKindV3.ATTESTOR, MssqlR1SignerProfileKindV3.STAGE_OWNER)
SECRET_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&*+-=?@^_"
SECRET_CLASSES = ("uppercase", "lowercase", "digit", "symbol")
SECRET_PLACEHOLDER = "{{DPONE_EPHEMERAL_CERT_PASSWORD_V1}}"


def require_exact_identifier(value: object, field: str) -> str:
    if type(value) is not str:
        raise MssqlR1V3ContractError(f"{field} must use the exact text type")
    return require_schema_identifier(value, field)


@dataclass(frozen=True, slots=True)
class MssqlR1EnvironmentPrincipalRefV1(MssqlR1SecurityCanonicalModel):
    subject_role: MssqlR1SubjectRoleV3

    def __post_init__(self) -> None:
        if type(self.subject_role) is not MssqlR1SubjectRoleV3 or self.subject_role not in ENVIRONMENT_SUBJECT_ROLES:
            raise MssqlR1V3ContractError("environment principal role is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["environment"], (self.subject_role,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EnvironmentPrincipalRefV1:
        (value,) = decode_canonical_bytes(payload, _DOMAINS["environment"], field_count=1)
        return cls(expect_enum(MssqlR1SubjectRoleV3, value, "environment role"))


@dataclass(frozen=True, slots=True)
class MssqlR1SharedSignerPrincipalRefV1(MssqlR1SecurityCanonicalModel):
    signer_profile: MssqlR1SignerProfileKindV3

    def __post_init__(self) -> None:
        if (
            type(self.signer_profile) is not MssqlR1SignerProfileKindV3
            or self.signer_profile not in SHARED_SIGNER_PROFILES
        ):
            raise MssqlR1V3ContractError("shared signer profile is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["shared"], (self.signer_profile,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SharedSignerPrincipalRefV1:
        (value,) = decode_canonical_bytes(payload, _DOMAINS["shared"], field_count=1)
        return cls(expect_enum(MssqlR1SignerProfileKindV3, value, "shared signer"))


@dataclass(frozen=True, slots=True)
class MssqlR1SharedSignerProfileRefV1(MssqlR1SecurityCanonicalModel):
    signer_profile: MssqlR1SignerProfileKindV3
    schema2_signer_profile_digest: bytes

    def __post_init__(self) -> None:
        MssqlR1SharedSignerPrincipalRefV1(self.signer_profile)
        require_exact_digest(self.schema2_signer_profile_digest, "schema-2 signer profile digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["signer_profile"], (self.signer_profile, self.schema2_signer_profile_digest))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SharedSignerProfileRefV1:
        kind, digest = decode_canonical_bytes(payload, _DOMAINS["signer_profile"], field_count=2)
        return cls(expect_enum(MssqlR1SignerProfileKindV3, kind, "signer profile"), expect_bytes(digest, "digest"))

    def resolve(self, signer: MssqlR1SignerProfileV3) -> None:
        if (
            type(signer) is not MssqlR1SignerProfileV3
            or signer.signer_profile is not self.signer_profile
            or (hashlib.sha256(signer.canonical_bytes).digest() != self.schema2_signer_profile_digest)
        ):
            raise MssqlR1V3ContractError("shared signer reference does not resolve")


@dataclass(frozen=True, slots=True)
class MssqlR1BindingSignerClassPrincipalRefV1(MssqlR1SecurityCanonicalModel):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["binding_class"], ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingSignerClassPrincipalRefV1:
        decode_canonical_bytes(payload, _DOMAINS["binding_class"], field_count=0)
        return cls()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingSignerInstancePrincipalRefV1(MssqlR1SecurityCanonicalModel):
    target_binding_uuid: UUID

    def __post_init__(self) -> None:
        if type(self.target_binding_uuid) is not UUID:
            raise MssqlR1V3ContractError("target binding UUID has an inexact type")
        require_uuid(self.target_binding_uuid, "target binding UUID")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["binding_instance"], (self.target_binding_uuid,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingSignerInstancePrincipalRefV1:
        (value,) = decode_canonical_bytes(payload, _DOMAINS["binding_instance"], field_count=1)
        return cls(require_uuid(value, "target binding UUID"))


@dataclass(frozen=True, slots=True)
class MssqlR1PublicPrincipalRefV1(MssqlR1SecurityCanonicalModel):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["public"], ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PublicPrincipalRefV1:
        decode_canonical_bytes(payload, _DOMAINS["public"], field_count=0)
        return cls()


@dataclass(frozen=True, slots=True)
class MssqlR1NamedDatabaseRoleRefV1(MssqlR1SecurityCanonicalModel):
    role_name: str

    def __post_init__(self) -> None:
        require_exact_identifier(self.role_name, "database role")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["named_role"], (self.role_name,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1NamedDatabaseRoleRefV1:
        (value,) = decode_canonical_bytes(payload, _DOMAINS["named_role"], field_count=1)
        return cls(value)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1AnyDatabaseRoleRefV1(MssqlR1SecurityCanonicalModel):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["any_role"], ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1AnyDatabaseRoleRefV1:
        decode_canonical_bytes(payload, _DOMAINS["any_role"], field_count=0)
        return cls()


MssqlR1SecurityPrincipalRefV1: TypeAlias = (
    MssqlR1EnvironmentPrincipalRefV1
    | MssqlR1SharedSignerPrincipalRefV1
    | MssqlR1BindingSignerClassPrincipalRefV1
    | MssqlR1BindingSignerInstancePrincipalRefV1
    | MssqlR1PublicPrincipalRefV1
    | MssqlR1NamedDatabaseRoleRefV1
    | MssqlR1AnyDatabaseRoleRefV1
)
MSSQL_R1_SECURITY_PRINCIPAL_ARMS = tuple(
    (_DOMAINS[name], contract)
    for name, contract in (
        ("environment", MssqlR1EnvironmentPrincipalRefV1),
        ("shared", MssqlR1SharedSignerPrincipalRefV1),
        ("binding_class", MssqlR1BindingSignerClassPrincipalRefV1),
        ("binding_instance", MssqlR1BindingSignerInstancePrincipalRefV1),
        ("public", MssqlR1PublicPrincipalRefV1),
        ("named_role", MssqlR1NamedDatabaseRoleRefV1),
        ("any_role", MssqlR1AnyDatabaseRoleRefV1),
    )
)


def decode_security_principal(payload: bytes) -> MssqlR1SecurityPrincipalRefV1:
    if type(payload) is bytes:
        for domain, contract in MSSQL_R1_SECURITY_PRINCIPAL_ARMS:
            if payload.startswith(domain):
                return contract.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("security principal uses an unknown union arm")


@dataclass(frozen=True, slots=True)
class MssqlR1DirectPermissionOriginV1(MssqlR1SecurityCanonicalModel):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["direct_origin"], ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1DirectPermissionOriginV1:
        decode_canonical_bytes(payload, _DOMAINS["direct_origin"], field_count=0)
        return cls()


@dataclass(frozen=True, slots=True)
class MssqlR1RolePermissionOriginV1(MssqlR1SecurityCanonicalModel):
    role: MssqlR1NamedDatabaseRoleRefV1 | MssqlR1AnyDatabaseRoleRefV1

    def __post_init__(self) -> None:
        if type(self.role) not in {MssqlR1NamedDatabaseRoleRefV1, MssqlR1AnyDatabaseRoleRefV1}:
            raise MssqlR1V3ContractError("role origin requires an exact role selector")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["role_origin"], (self.role.canonical_bytes,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RolePermissionOriginV1:
        (value,) = decode_canonical_bytes(payload, _DOMAINS["role_origin"], field_count=1)
        role = decode_security_principal(expect_bytes(value, "role origin"))
        if type(role) not in {MssqlR1NamedDatabaseRoleRefV1, MssqlR1AnyDatabaseRoleRefV1}:
            raise MssqlR1V3ContractError("role origin has a non-role arm")
        return cls(cast(MssqlR1NamedDatabaseRoleRefV1 | MssqlR1AnyDatabaseRoleRefV1, role))


@dataclass(frozen=True, slots=True)
class MssqlR1PublicPermissionOriginV1(MssqlR1SecurityCanonicalModel):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["public_origin"], ())

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PublicPermissionOriginV1:
        decode_canonical_bytes(payload, _DOMAINS["public_origin"], field_count=0)
        return cls()


MssqlR1PermissionOriginV1: TypeAlias = (
    MssqlR1DirectPermissionOriginV1 | MssqlR1RolePermissionOriginV1 | MssqlR1PublicPermissionOriginV1
)


def decode_permission_origin(payload: bytes) -> MssqlR1PermissionOriginV1:
    if type(payload) is bytes:
        for key, contract in (
            ("direct_origin", MssqlR1DirectPermissionOriginV1),
            ("role_origin", MssqlR1RolePermissionOriginV1),
            ("public_origin", MssqlR1PublicPermissionOriginV1),
        ):
            if payload.startswith(_DOMAINS[key]):
                return contract.from_canonical_bytes(payload)
    raise MssqlR1V3ContractError("permission origin uses an unknown union arm")


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionTargetV1(MssqlR1SecurityCanonicalModel):
    scope: MssqlR1PermissionTargetScopeV1
    schema_name: str | None
    object_name: str | None
    column_name: str | None
    include_descendants: bool

    def __post_init__(self) -> None:
        if type(self.scope) is not MssqlR1PermissionTargetScopeV1 or type(self.include_descendants) is not bool:
            raise MssqlR1V3ContractError("permission target discriminator is inexact")
        coordinates = (self.schema_name, self.object_name, self.column_name)
        expected = {
            "database": (False, False, False),
            "schema": (True, False, False),
            "object": (True, True, False),
            "column": (True, True, True),
        }[self.scope.value]
        if tuple(value is not None for value in coordinates) != expected:
            raise MssqlR1V3ContractError("permission target coordinates differ from scope")
        for value in coordinates:
            if value is not None:
                require_exact_identifier(value, "permission target coordinate")
        if self.include_descendants and self.scope in {
            MssqlR1PermissionTargetScopeV1.DATABASE,
            MssqlR1PermissionTargetScopeV1.COLUMN,
        }:
            raise MssqlR1V3ContractError("permission target descendants are unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_DOMAINS["target"], tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionTargetV1:
        values = list(decode_canonical_bytes(payload, _DOMAINS["target"], field_count=5))
        values[0] = expect_enum(MssqlR1PermissionTargetScopeV1, values[0], "target scope")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ForbiddenRoleMembershipV1(MssqlR1SecurityCanonicalModel):
    ordinal: int
    member: MssqlR1SecurityPrincipalRefV1
    forbidden_role: MssqlR1NamedDatabaseRoleRefV1 | MssqlR1AnyDatabaseRoleRefV1

    def __post_init__(self) -> None:
        require_exact_positive(self.ordinal, "membership ordinal")
        if type(self.member) not in {
            MssqlR1EnvironmentPrincipalRefV1,
            MssqlR1SharedSignerPrincipalRefV1,
            MssqlR1BindingSignerClassPrincipalRefV1,
        } or type(self.forbidden_role) not in {
            MssqlR1NamedDatabaseRoleRefV1,
            MssqlR1AnyDatabaseRoleRefV1,
        }:
            raise MssqlR1V3ContractError("forbidden membership has an inexact union arm")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAINS["membership"],
            (self.ordinal, self.member.canonical_bytes, self.forbidden_role.canonical_bytes),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ForbiddenRoleMembershipV1:
        ordinal, member, role = decode_canonical_bytes(payload, _DOMAINS["membership"], field_count=3)
        decoded_role = decode_security_principal(expect_bytes(role, "forbidden role"))
        if type(decoded_role) not in {MssqlR1NamedDatabaseRoleRefV1, MssqlR1AnyDatabaseRoleRefV1}:
            raise MssqlR1V3ContractError("forbidden role is not a role arm")
        return cls(ordinal, decode_security_principal(expect_bytes(member, "membership member")), decoded_role)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1EphemeralSecretPolicyV1(MssqlR1SecurityCanonicalModel):
    generator_profile: MssqlR1SecretGeneratorProfileV1
    minimum_entropy_bits: int
    output_length: int
    alphabet_ascii: str
    required_character_classes: tuple[str, ...]
    placeholder_ascii: str
    placeholder_occurrences_per_template: int
    persistence_policy: MssqlR1SecretPersistencePolicyV1
    logging_policy: MssqlR1SecretLoggingPolicyV1
    lifetime_policy: MssqlR1SecretLifetimePolicyV1

    @staticmethod
    def _exact_values() -> tuple[object, ...]:
        return (
            MssqlR1SecretGeneratorProfileV1.CRYPTOGRAPHIC_RANDOM_ASCII_256BIT_V1,
            256,
            64,
            SECRET_ALPHABET,
            SECRET_CLASSES,
            SECRET_PLACEHOLDER,
            1,
            MssqlR1SecretPersistencePolicyV1.MEMORY_ONLY,
            MssqlR1SecretLoggingPolicyV1.DPONE_LOGGING_FORBIDDEN,
            MssqlR1SecretLifetimePolicyV1.INSTALLER_TRANSACTION_ONLY,
        )

    def __post_init__(self) -> None:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        exact = self._exact_values()
        exact_types = tuple(type(value) for value in exact)
        if tuple(type(value) for value in values) != exact_types or values != exact:
            raise MssqlR1V3ContractError("ephemeral secret policy differs from the exact R1 profile")

    @classmethod
    def exact(cls) -> MssqlR1EphemeralSecretPolicyV1:
        return cls(*cls._exact_values())  # type: ignore[arg-type]

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DOMAINS["secret"],
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EphemeralSecretPolicyV1:
        values = list(decode_canonical_bytes(payload, _DOMAINS["secret"], field_count=10))
        for index, enum in (
            (0, MssqlR1SecretGeneratorProfileV1),
            (7, MssqlR1SecretPersistencePolicyV1),
            (8, MssqlR1SecretLoggingPolicyV1),
            (9, MssqlR1SecretLifetimePolicyV1),
        ):
            values[index] = expect_enum(enum, values[index], "secret policy")
        return cls(*values)  # type: ignore[arg-type]
