"""Pure canonical identities for the unreleased PostgreSQL-to-MSSQL R1 V3 contract."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from dpone.contracts.mssql_r1_v3_errors import (
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utf8_fields,
    decode_canonical_bytes,
    decode_canonical_utf8_fields,
    encode_artifact_rows,
    expect_bool,
    expect_bytes,
    expect_enum,
    expect_int,
    expect_text,
    expect_tuple,
    expect_uuid,
    validate_detached_command_binding,
    validate_signed_command_bytes,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

EFFECT_CONTRACT_VERSION = "mssql_effect_receipt_v3"
REQUEST_CODEC_VERSION = "dpone.mssql-effect-request.v3"
QUALITY_CODEC_VERSION = "dpone-r1-quality-v3"
AUTHORITY_SET_CODEC_VERSION = "dpone-r1-generation-authority-set-v2"
MAX_SQL_BIGINT = 2**63 - 1
MAX_SQL_INT = 2**31 - 1
_UTC_TIMEZONE = timezone(timedelta(0))
_EFFECT_CONTRACT_DOMAIN = b"dpone-r1-effect-contract-v3\0"
_EFFECT_IDENTITY_DOMAIN = b"dpone-r1-effect-identity-v3\0"
_PHYSICAL_COORDINATE_DOMAIN = b"dpone-mssql-physical-object-coordinate-v1\0"
_REGISTERED_AUTHORITY_DOMAIN = b"dpone-mssql-registered-physical-authority-v1\0"
_PREDECESSOR_HEAD_DOMAIN = b"dpone-r1-writer-head-predecessor-v1\0"


@dataclass(frozen=True, slots=True)
class MssqlR1EffectContractV3:
    """Closed codec identity; callers cannot substitute a legacy wire contract."""

    effect_contract_version: str = EFFECT_CONTRACT_VERSION
    request_codec: str = REQUEST_CODEC_VERSION
    quality_codec: str = QUALITY_CODEC_VERSION
    authority_set_codec: str = AUTHORITY_SET_CODEC_VERSION

    def __post_init__(self) -> None:
        expected = (
            EFFECT_CONTRACT_VERSION,
            REQUEST_CODEC_VERSION,
            QUALITY_CODEC_VERSION,
            AUTHORITY_SET_CODEC_VERSION,
        )
        if tuple(getattr(self, name) for name in self.__dataclass_fields__) != expected:
            raise MssqlR1V3ContractError("effect contract must use the exact V3 codec identity")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_EFFECT_CONTRACT_DOMAIN, expected_contract_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectContractV3:
        values = decode_canonical_bytes(payload, _EFFECT_CONTRACT_DOMAIN, field_count=4)
        if not all(isinstance(value, str) for value in values):
            raise MssqlR1V3ContractError("effect contract fields must be canonical UTF-8 text")
        return cls(*values)  # type: ignore[arg-type]

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()


def expected_contract_values(contract: MssqlR1EffectContractV3) -> tuple[object, ...]:
    return (
        contract.effect_contract_version,
        contract.request_codec,
        contract.quality_codec,
        contract.authority_set_codec,
    )


@dataclass(frozen=True, slots=True)
class MssqlR1EffectIdentityV3:
    """Retry-stable business-effect identity with explicit V3 domain separation."""

    route_identity_sha256: bytes
    invocation_identity: str
    source_mode: SourceMode
    target_binding_uuid: UUID

    def __post_init__(self) -> None:
        require_digest(self.route_identity_sha256, "route_identity_sha256")
        require_canonical_text(self.invocation_identity, "invocation_identity", maximum_bytes=512)
        if not isinstance(self.source_mode, SourceMode):
            raise MssqlR1V3ContractError("source_mode is unsupported")
        require_uuid(self.target_binding_uuid, "target_binding_uuid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _EFFECT_IDENTITY_DOMAIN,
            (self.route_identity_sha256, self.invocation_identity, self.source_mode, self.target_binding_uuid),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectIdentityV3:
        route, invocation, source_mode, binding = decode_canonical_bytes(
            payload, _EFFECT_IDENTITY_DOMAIN, field_count=4
        )
        return cls(
            require_digest(route, "route_identity_sha256"),
            require_canonical_text(invocation, "invocation_identity", maximum_bytes=512),
            expect_enum(SourceMode, source_mode, "source_mode"),
            require_uuid(binding, "target_binding_uuid"),
        )

    @property
    def operation_key(self) -> bytes:
        return canonical_digest(
            b"dpone-r1-operation-key-v3\0",
            (
                EFFECT_CONTRACT_VERSION,
                self.route_identity_sha256,
                self.invocation_identity,
                self.source_mode,
                self.target_binding_uuid,
            ),
        )

    @property
    def effect_key(self) -> bytes:
        return canonical_digest(
            b"dpone-r1-effect-key-v3\0",
            (EFFECT_CONTRACT_VERSION, self.target_binding_uuid, self.source_mode, self.operation_key),
        )


@dataclass(frozen=True, slots=True)
class MssqlTargetPhysicalIdentityV1:
    """Signed physical target facts with distinct coordinate and authority identities."""

    target_object_uuid: UUID
    server_instance_identity_sha256: bytes
    database_guid: UUID
    database_family_guid: UUID
    recovery_fork_guid: UUID
    recovery_domain_uuid: UUID
    recovery_domain_epoch: int
    database_name: str
    database_name_digest: bytes
    schema_name: str
    schema_name_digest: bytes
    object_name: str
    object_name_digest: bytes
    object_id: int
    physical_generation_uuid: UUID
    catalog_contract_digest: bytes
    target_contract_revision: int

    def __post_init__(self) -> None:
        for name in (
            "target_object_uuid",
            "database_guid",
            "database_family_guid",
            "recovery_fork_guid",
            "recovery_domain_uuid",
            "physical_generation_uuid",
        ):
            require_uuid(getattr(self, name), name)
        for name in ("server_instance_identity_sha256", "catalog_contract_digest"):
            require_digest(getattr(self, name), name)
        require_positive(self.recovery_domain_epoch, "recovery_domain_epoch")
        require_sql_int(self.object_id, "object_id")
        require_positive(self.target_contract_revision, "target_contract_revision")
        for name in ("database", "schema", "object"):
            text = require_identifier(getattr(self, f"{name}_name"), f"{name}_name")
            digest = require_digest(getattr(self, f"{name}_name_digest"), f"{name}_name_digest")
            if digest != canonical_identifier_digest(text):
                raise MssqlR1V3ContractError(f"{name}_name_digest does not bind its canonical name")

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return (
            self.target_object_uuid,
            self.server_instance_identity_sha256,
            self.database_guid,
            self.database_family_guid,
            self.recovery_fork_guid,
            self.recovery_domain_uuid,
            self.recovery_domain_epoch,
            self.database_name,
            self.database_name_digest,
            self.schema_name,
            self.schema_name_digest,
            self.object_name,
            self.object_name_digest,
            self.object_id,
            self.physical_generation_uuid,
            self.catalog_contract_digest,
            self.target_contract_revision,
        )

    @property
    def physical_object_coordinate_digest(self) -> bytes:
        return canonical_digest(
            _PHYSICAL_COORDINATE_DOMAIN,
            (self.server_instance_identity_sha256, self.database_guid, self.object_id),
        )

    @property
    def registered_physical_authority_digest(self) -> bytes:
        return canonical_digest(_REGISTERED_AUTHORITY_DOMAIN, self.canonical_values)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_REGISTERED_AUTHORITY_DOMAIN, self.canonical_values)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlTargetPhysicalIdentityV1:
        values = decode_canonical_bytes(payload, _REGISTERED_AUTHORITY_DOMAIN, field_count=17)
        return cls(*values)  # type: ignore[arg-type]


def writer_head_predecessor_digest(
    target: MssqlTargetPhysicalIdentityV1,
    generation: int,
    head_revision: int,
    receipt_id: UUID,
    receipt_digest: bytes,
) -> bytes:
    """Bind the exact predecessor receipt to its target generation and head."""

    return canonical_digest(
        _PREDECESSOR_HEAD_DOMAIN,
        (
            target.registered_physical_authority_digest,
            require_positive(generation, "generation"),
            require_positive(head_revision, "head_revision"),
            require_uuid(receipt_id, "receipt_id"),
            require_digest(receipt_digest, "receipt_digest"),
        ),
    )


def validate_generation_authority_coordinates(
    purpose: str,
    expected_generation: int | None,
    candidate_generation: int,
    expected_head_revision: int | None,
    candidate_head_revision: int,
) -> None:
    """Validate the closed initial, rebaseline, and empty-refresh coordinate shapes."""

    candidate = (
        require_positive(candidate_generation, "candidate_writer_generation"),
        require_positive(candidate_head_revision, "candidate_head_revision"),
    )
    if (expected_generation is None) != (expected_head_revision is None):
        raise MssqlR1V3ContractError("generation authority predecessor identity must be all-or-none")
    if expected_generation is None:
        if purpose == "rebaseline" or candidate != (1, 1):
            raise MssqlR1V3ContractError("initial authority must target generation 1 revision 1")
        return
    expected = (
        require_positive(expected_generation, "expected_writer_generation"),
        require_positive(expected_head_revision, "expected_head_revision"),
    )
    if purpose == "initial_cutover":
        raise MssqlR1V3ContractError("initial-cutover authority cannot name a predecessor")
    next_generation = candidate == (expected[0] + 1, 1)
    same_generation = candidate == (expected[0], expected[1] + 1)
    if purpose == "rebaseline" and not next_generation:
        raise MssqlR1V3ContractError("rebaseline authority must target an adjacent generation")
    if purpose == "empty_refresh" and not (next_generation or same_generation):
        raise MssqlR1V3ContractError("empty-refresh authority must target the exact adjacent effect")


def canonical_digest(domain: bytes, values: tuple[object, ...]) -> bytes:
    return hashlib.sha256(canonical_bytes(domain, values)).digest()


def require_digest(value: object, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise MssqlR1V3ContractError(f"{field} must be exactly 32 bytes")
    return value


def require_uuid(value: object, field: str) -> UUID:
    if not isinstance(value, UUID):
        raise MssqlR1V3ContractError(f"{field} must be a UUID")
    return value


def require_positive(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_SQL_BIGINT:
        raise MssqlR1V3ContractError(f"{field} must fit a positive SQL bigint")
    return value


def require_count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SQL_BIGINT:
        raise MssqlR1V3ContractError(f"{field} must fit a non-negative SQL bigint")
    return value


def require_sql_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_SQL_INT:
        raise MssqlR1V3ContractError(f"{field} must fit a positive SQL int")
    return value


def require_canonical_text(value: object, field: str, *, maximum_bytes: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or unicodedata.normalize("NFC", value) != value
        or "\0" in value
        or len(value.encode("utf-8")) > maximum_bytes
    ):
        raise MssqlR1V3ContractError(f"{field} must be bounded NFC canonical text")
    return value


def require_identifier(value: object, field: str) -> str:
    return require_canonical_text(value, field, maximum_bytes=256)


def canonical_identifier_digest(value: str) -> bytes:
    """Hash exact NFC UTF-8 identifier bytes as declared by the approved registration contract."""

    return hashlib.sha256(require_identifier(value, "identifier").encode("utf-8")).digest()


def canonical_utc_text(value: object, field: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != _UTC_TIMEZONE.utcoffset(value):
        raise MssqlR1V3ContractError(f"{field} must be an aware UTC instant")
    return value.astimezone(_UTC_TIMEZONE).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_canonical_utc_text(value: object, field: str) -> datetime:
    text = require_canonical_text(value, field, maximum_bytes=64)
    try:
        parsed = datetime.fromisoformat(text.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise MssqlR1V3ContractError(f"{field} must be canonical UTC text") from exc
    if canonical_utc_text(parsed, field) != text:
        raise MssqlR1V3ContractError(f"{field} must be canonical UTC text")
    return parsed


__all__ = [
    "AUTHORITY_SET_CODEC_VERSION",
    "EFFECT_CONTRACT_VERSION",
    "MAX_SQL_BIGINT",
    "MssqlR1EffectContractV3",
    "MssqlR1EffectIdentityV3",
    "MssqlR1V3ContractError",
    "MssqlTargetPhysicalIdentityV1",
    "QUALITY_CODEC_VERSION",
    "REQUEST_CODEC_VERSION",
    "canonical_bytes",
    "canonical_utf8_fields",
    "decode_canonical_bytes",
    "decode_canonical_utf8_fields",
    "encode_artifact_rows",
    "validate_detached_command_binding",
    "validate_signed_command_bytes",
    "expect_bool",
    "expect_bytes",
    "expect_enum",
    "expect_int",
    "expect_text",
    "expect_tuple",
    "expect_uuid",
    "canonical_digest",
    "canonical_identifier_digest",
    "canonical_utc_text",
    "parse_canonical_utc_text",
    "require_canonical_text",
    "require_count",
    "require_digest",
    "require_identifier",
    "require_positive",
    "require_sql_int",
    "require_uuid",
    "validate_generation_authority_coordinates",
    "writer_head_predecessor_digest",
]
