"""Closed original-subject identity and canonical codecs without I/O authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal, TypeAlias, cast
from uuid import UUID

from dpone.contracts.dbt_contract_validation import DbtPublishingError, require_digest
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)

_SCHEMA = "dpone.native-original-subject.v1"
_AUTHORITY_FIELDS = frozenset(
    {
        "environment",
        "release_id",
        "deployment_id",
        "release_sha256",
        "deployment_sha256",
        "binding_set_sha256",
        "connection_registry_sha256",
        "credential_runtime_sha256",
        "authority_subject_sha256",
    }
)
_COMMON_FIELDS = frozenset({"schema", "scope", "authority"})


class NativeOriginalSubjectError(ValueError):
    """An original subject has invalid fields, identity or canonical encoding."""


def _digest(value: object) -> str:
    if type(value) is not str:
        raise NativeOriginalSubjectError("subject digest must be a string")
    try:
        return require_digest(value, "subject digest", "DPONE_NATIVE_ORIGINAL_SUBJECT_INVALID")
    except DbtPublishingError as exc:
        raise NativeOriginalSubjectError("subject digest must be canonical sha256") from exc


def _authority_payload(authority: DbtWorkspaceRuntimeAuthority) -> dict[str, NativeJsonValue]:
    if type(authority) is not DbtWorkspaceRuntimeAuthority:
        raise NativeOriginalSubjectError("subject requires the existing workspace runtime authority")
    return {
        "environment": authority.environment,
        "release_id": authority.release_id,
        "deployment_id": authority.deployment_id,
        "release_sha256": authority.release_sha256,
        "deployment_sha256": authority.deployment_sha256,
        "binding_set_sha256": authority.binding_set_sha256,
        "connection_registry_sha256": authority.connection_registry_sha256,
        "credential_runtime_sha256": authority.credential_runtime_sha256,
        "authority_subject_sha256": authority.authority_subject_sha256,
    }


def _decode_authority(value: object) -> DbtWorkspaceRuntimeAuthority:
    if type(value) is not dict:
        raise NativeOriginalSubjectError("workspace authority must be an object")
    payload = cast(dict[str, object], value)
    if set(payload) != _AUTHORITY_FIELDS or any(type(item) is not str for item in payload.values()):
        raise NativeOriginalSubjectError("workspace authority requires its exact string fields")
    try:
        # The existing constructor verifies its own descriptor fingerprint.
        return DbtWorkspaceRuntimeAuthority(**cast(dict[str, str], payload))
    except (TypeError, ValueError) as exc:
        raise NativeOriginalSubjectError("workspace authority identity is invalid") from exc


def _require_authority(authority: DbtWorkspaceRuntimeAuthority) -> None:
    _decode_authority(_authority_payload(authority))


@dataclass(frozen=True, slots=True)
class NativeGenerationOriginalSubject:
    """Original belonging to one generation under an exact workspace authority."""

    authority: DbtWorkspaceRuntimeAuthority
    generation_id: UUID
    schema: ClassVar[str] = _SCHEMA
    scope: ClassVar[Literal["GENERATION"]] = "GENERATION"

    def __post_init__(self) -> None:
        _require_authority(self.authority)
        if type(self.generation_id) is not UUID:
            raise NativeOriginalSubjectError("generation subject requires a UUID")


@dataclass(frozen=True, slots=True)
class NativeDeliveryOriginalSubject:
    """Original belonging to one operation/attempt, never an interchangeable run."""

    authority: DbtWorkspaceRuntimeAuthority
    operation_id: str
    attempt_id: str
    schema: ClassVar[str] = _SCHEMA
    scope: ClassVar[Literal["DELIVERY"]] = "DELIVERY"

    def __post_init__(self) -> None:
        _require_authority(self.authority)
        _digest(self.operation_id)
        _digest(self.attempt_id)


@dataclass(frozen=True, slots=True)
class NativePlatformOriginalSubject:
    """Policy-bound platform original; its hash never authenticates itself.

    The caller must authenticate the full canonical selected v4 policy before
    constructing this subject. Its policy hash does not replace shared physical
    resource ownership across deployment snapshots.
    """

    authority: DbtWorkspaceRuntimeAuthority
    platform_policy_sha256: str
    schema: ClassVar[str] = _SCHEMA
    scope: ClassVar[Literal["PLATFORM"]] = "PLATFORM"

    def __post_init__(self) -> None:
        _require_authority(self.authority)
        _digest(self.platform_policy_sha256)


NativeOriginalSubject: TypeAlias = (
    NativeGenerationOriginalSubject | NativeDeliveryOriginalSubject | NativePlatformOriginalSubject
)


def encode_native_original_subject(value: NativeOriginalSubject) -> bytes:
    """Return exact closed canonical bytes for one validated subject variant.

    No storage lookup, credential resolution or authority grant is performed.
    Unknown variants/subclasses are rejected rather than coerced or serialized.
    """
    if type(value) not in (
        NativeGenerationOriginalSubject,
        NativeDeliveryOriginalSubject,
        NativePlatformOriginalSubject,
    ):
        raise NativeOriginalSubjectError("unsupported original subject type")
    value.__post_init__()
    payload: dict[str, NativeJsonValue] = {
        "schema": _SCHEMA,
        "scope": value.scope,
        "authority": _authority_payload(value.authority),
    }
    if isinstance(value, NativeGenerationOriginalSubject):
        payload["generation_id"] = str(value.generation_id)
    elif isinstance(value, NativeDeliveryOriginalSubject):
        payload.update(operation_id=value.operation_id, attempt_id=value.attempt_id)
    else:
        payload["platform_policy_sha256"] = value.platform_policy_sha256
    try:
        # Revalidate frozen values too; never trust a shape bypassed by a caller.
        _subject_from_payload(payload)
        return encode_native_delivery_json(payload)
    except (TypeError, ValueError) as exc:
        raise NativeOriginalSubjectError("original subject cannot be encoded") from exc


def _subject_from_payload(payload: dict[str, NativeJsonValue]) -> NativeOriginalSubject:
    if payload.get("schema") != _SCHEMA:
        raise NativeOriginalSubjectError("unsupported original subject schema")
    scope = payload.get("scope")
    if scope == "GENERATION":
        expected = _COMMON_FIELDS | {"generation_id"}
    elif scope == "DELIVERY":
        expected = _COMMON_FIELDS | {"operation_id", "attempt_id"}
    elif scope == "PLATFORM":
        expected = _COMMON_FIELDS | {"platform_policy_sha256"}
    else:
        raise NativeOriginalSubjectError("unsupported original subject scope")
    if set(payload) != expected:
        raise NativeOriginalSubjectError("original subject requires exactly its variant fields")
    authority = _decode_authority(payload["authority"])
    if scope == "GENERATION":
        raw = payload["generation_id"]
        if type(raw) is not str:
            raise NativeOriginalSubjectError("generation UUID must be a canonical string")
        generation = UUID(raw)
        if str(generation) != raw:
            raise NativeOriginalSubjectError("generation UUID must use canonical lowercase hyphenated form")
        return NativeGenerationOriginalSubject(authority, generation)
    if scope == "DELIVERY":
        return NativeDeliveryOriginalSubject(
            authority, _digest(payload["operation_id"]), _digest(payload["attempt_id"])
        )
    return NativePlatformOriginalSubject(authority, _digest(payload["platform_policy_sha256"]))


def decode_native_original_subject(payload: bytes) -> NativeOriginalSubject:
    """Reject noncanonical or invalid subjects before creating a domain value.

    The nested workspace authority preserves its existing fingerprint contract.
    Complete canonical subject equality binds every coordinate. Decoding neither
    authenticates a stored original nor permits source/target execution.
    """
    try:
        decoded = decode_native_delivery_json(payload)
        if encode_native_delivery_json(decoded) != payload:
            raise NativeOriginalSubjectError("original subject bytes must be canonical")
        return _subject_from_payload(decoded)
    except NativeOriginalSubjectError:
        raise
    except (TypeError, ValueError) as exc:
        raise NativeOriginalSubjectError("invalid original subject document") from exc
