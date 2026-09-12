"""Foundational discriminators and exact values for MSSQL R1 security."""

import hashlib
from dataclasses import dataclass
from typing import TypeVar

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
    require_digest,
    require_positive,
)
from dpone.contracts.mssql_r1_v3_identity import (
    require_uuid as require_uuid,
)

T = TypeVar("T")


class MssqlR1SecurityCanonicalModel:
    """Shared digest behavior for one immutable security authority value."""

    __slots__ = ()

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()  # type: ignore[attr-defined]


def require_exact_digest(value: object, field: str) -> bytes:
    if type(value) is not bytes:
        raise MssqlR1V3ContractError(f"{field} must use the exact bytes type")
    return require_digest(value, field)


def require_exact_positive(value: object, field: str) -> int:
    if type(value) is not int:
        raise MssqlR1V3ContractError(f"{field} must use the exact integer type")
    return require_positive(value, field)


def decode_exact_members(value: object, contract: type[T], field: str) -> tuple[T, ...]:
    return tuple(
        contract.from_canonical_bytes(expect_bytes(item, field))  # type: ignore[attr-defined]
        for item in expect_tuple(value, field)
    )


class MssqlR1EnvironmentPrincipalProfileV1(StrEnum):
    CONTAINED_SQL_USERS_V1 = "contained_sql_users_v1"


class MssqlR1CertificateCreationProfileV1(StrEnum):
    SQLSERVER_SELF_SIGNED_SHA2_256_V1 = "sqlserver_self_signed_sha2_256_v1"


class MssqlR1CertificateSignatureAlgorithmV1(StrEnum):
    SHA2_256 = "sha2_256"


class MssqlR1SecretGeneratorProfileV1(StrEnum):
    CRYPTOGRAPHIC_RANDOM_ASCII_256BIT_V1 = "cryptographic_random_ascii_256bit_v1"


class MssqlR1SecretPersistencePolicyV1(StrEnum):
    MEMORY_ONLY = "memory_only"


class MssqlR1SecretLoggingPolicyV1(StrEnum):
    DPONE_LOGGING_FORBIDDEN = "dpone_logging_forbidden"


class MssqlR1SecretLifetimePolicyV1(StrEnum):
    INSTALLER_TRANSACTION_ONLY = "installer_transaction_only"


class MssqlR1PrivateKeyDispositionV1(StrEnum):
    REMOVE_AFTER_ALL_SIGNATURES = "remove_after_all_signatures"


class MssqlR1ReinstallPolicyV1(StrEnum):
    OBSERVE_EXACT_OR_BLOCK = "observe_exact_or_block"


class MssqlR1BindingSignerNamingProfileV1(StrEnum):
    LOWERCASE_BINDING_UUID_V1 = "lowercase_binding_uuid_v1"


class MssqlR1BindingPermissionProfileV1(StrEnum):
    EXACT_TARGET_AND_ROW_HASH_V1 = "exact_target_and_row_hash_v1"


class MssqlR1CertificateLifecycleStateV1(StrEnum):
    ABSENT = "absent"
    PRIVATE_KEY_PRESENT = "private_key_present"
    CERTIFICATE_USER_READY = "certificate_user_ready"
    PERMISSIONS_READY = "permissions_ready"
    SIGNATURES_COMPLETE = "signatures_complete"
    INSTALLED_PUBLIC_KEY_ONLY = "installed_public_key_only"
    CONFLICT = "conflict"


class MssqlR1CertificateLifecycleActionV1(StrEnum):
    CREATE_CERTIFICATE = "create_certificate"
    CREATE_CERTIFICATE_USER = "create_certificate_user"
    GRANT_EXACT_PERMISSIONS = "grant_exact_permissions"
    SIGN_EXACT_MODULES = "sign_exact_modules"
    REMOVE_PRIVATE_KEY = "remove_private_key"
    OBSERVE_REPLAY = "observe_replay"


class MssqlR1PermissionTargetScopeV1(StrEnum):
    DATABASE = "database"
    SCHEMA = "schema"
    OBJECT = "object"
    COLUMN = "column"


class MssqlR1ForbiddenPermissionEffectV1(StrEnum):
    GRANT = "grant"
    DENY = "deny"
    GRANT_WITH_GRANT_OPTION = "grant_with_grant_option"


class MssqlR1SecretTemplateKindV1(StrEnum):
    CREATE_CERTIFICATE = "create_certificate"
    ADD_SIGNATURE = "add_signature"


MSSQL_R1_SECURITY_ENUM_REGISTRY = (
    MssqlR1EnvironmentPrincipalProfileV1,
    MssqlR1CertificateCreationProfileV1,
    MssqlR1CertificateSignatureAlgorithmV1,
    MssqlR1SecretGeneratorProfileV1,
    MssqlR1SecretPersistencePolicyV1,
    MssqlR1SecretLoggingPolicyV1,
    MssqlR1SecretLifetimePolicyV1,
    MssqlR1PrivateKeyDispositionV1,
    MssqlR1ReinstallPolicyV1,
    MssqlR1BindingSignerNamingProfileV1,
    MssqlR1BindingPermissionProfileV1,
    MssqlR1CertificateLifecycleStateV1,
    MssqlR1CertificateLifecycleActionV1,
    MssqlR1PermissionTargetScopeV1,
    MssqlR1ForbiddenPermissionEffectV1,
    MssqlR1SecretTemplateKindV1,
)
SHARED_SIGNER_SUBJECTS = {"attestor": "dpone R1 V3 attestor", "stage_owner": "dpone R1 V3 stage owner"}

_TRANSITION = b"dpone-r1-security-certificate-lifecycle-transition-v1\0"
_TRANSITION_SET = b"dpone-r1-security-certificate-lifecycle-transition-set-v2\0"


@dataclass(frozen=True, slots=True)
class MssqlR1CertificateLifecycleTransitionV1:
    ordinal: int
    from_state: MssqlR1CertificateLifecycleStateV1
    action: MssqlR1CertificateLifecycleActionV1
    to_state: MssqlR1CertificateLifecycleStateV1

    def __post_init__(self) -> None:
        require_exact_positive(self.ordinal, "lifecycle ordinal")
        if (
            type(self.from_state) is not MssqlR1CertificateLifecycleStateV1
            or type(self.action) is not MssqlR1CertificateLifecycleActionV1
            or type(self.to_state) is not MssqlR1CertificateLifecycleStateV1
        ):
            raise MssqlR1V3ContractError("lifecycle discriminator is inexact")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_TRANSITION, (self.ordinal, self.from_state, self.action, self.to_state))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> "MssqlR1CertificateLifecycleTransitionV1":
        values = list(decode_canonical_bytes(payload, _TRANSITION, field_count=4))
        values[1] = expect_enum(MssqlR1CertificateLifecycleStateV1, values[1], "from state")
        values[2] = expect_enum(MssqlR1CertificateLifecycleActionV1, values[2], "action")
        values[3] = expect_enum(MssqlR1CertificateLifecycleStateV1, values[3], "to state")
        return cls(*values)  # type: ignore[arg-type]


def exact_lifecycle_transitions() -> tuple[MssqlR1CertificateLifecycleTransitionV1, ...]:
    state, action = MssqlR1CertificateLifecycleStateV1, MssqlR1CertificateLifecycleActionV1
    edges = (
        (state.ABSENT, action.CREATE_CERTIFICATE, state.PRIVATE_KEY_PRESENT),
        (state.PRIVATE_KEY_PRESENT, action.CREATE_CERTIFICATE_USER, state.CERTIFICATE_USER_READY),
        (state.CERTIFICATE_USER_READY, action.GRANT_EXACT_PERMISSIONS, state.PERMISSIONS_READY),
        (state.PERMISSIONS_READY, action.SIGN_EXACT_MODULES, state.SIGNATURES_COMPLETE),
        (state.SIGNATURES_COMPLETE, action.REMOVE_PRIVATE_KEY, state.INSTALLED_PUBLIC_KEY_ONLY),
        (state.INSTALLED_PUBLIC_KEY_ONLY, action.OBSERVE_REPLAY, state.INSTALLED_PUBLIC_KEY_ONLY),
    )
    return tuple(MssqlR1CertificateLifecycleTransitionV1(index, *edge) for index, edge in enumerate(edges, 1))


def exact_lifecycle_transition_set_digest() -> bytes:
    values = tuple(item.canonical_bytes for item in exact_lifecycle_transitions())
    return hashlib.sha256(canonical_bytes(_TRANSITION_SET, values)).digest()


def lifecycle_transition_set_digest(transitions: tuple[MssqlR1CertificateLifecycleTransitionV1, ...]) -> bytes:
    if type(transitions) is not tuple or transitions != exact_lifecycle_transitions():
        raise MssqlR1V3ContractError("certificate lifecycle transition set is incomplete")
    return exact_lifecycle_transition_set_digest()
