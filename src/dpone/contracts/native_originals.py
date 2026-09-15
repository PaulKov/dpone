"""Closed original-subject identity and canonical codecs without I/O authority."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import ClassVar, Literal, TypeAlias, cast
from uuid import UUID

from dpone.contracts.dbt_contract_validation import DbtPublishingError, require_digest
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.s3_artifact_store_policy import S3ArtifactStorePolicy
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef

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


NativeOriginalKind: TypeAlias = Literal[
    "generation_storage_root_v1", "generation_stored_file_v1", "generation_seal_resolution_v1"
]
_BINDING_SCHEMA = "dpone.native-original-binding.v1"
_BINDING_FIELDS = frozenset(
    {"schema", "subject", "kind", "storage_authority", "object_ref", "payload_sha256", "locator"}
)
_OBJECT_FIELDS = frozenset({"key", "version", "size_bytes", "sha256", "encryption_scope", "retention_until"})
_KINDS = frozenset({"generation_storage_root_v1", "generation_stored_file_v1", "generation_seal_resolution_v1"})


class NativeOriginalBindingError(ValueError):
    """An original binding has malformed or noncanonical identity coordinates."""


def _object_payload(value: ArtifactObjectRef) -> dict[str, NativeJsonValue]:
    if type(value) is not ArtifactObjectRef:
        raise NativeOriginalBindingError("binding requires the existing object reference type")
    payload: dict[str, NativeJsonValue] = {
        "key": value.key,
        "version": value.version,
        "size_bytes": value.size_bytes,
        "sha256": value.sha256,
        "encryption_scope": value.encryption_scope,
        "retention_until": value.retention_until,
    }
    # Check bounded exact primitive types before parsing or inspecting provider text.
    encode_native_delivery_json(payload)
    for name in _OBJECT_FIELDS - {"size_bytes"}:
        if type(payload[name]) is not str or not payload[name]:
            raise NativeOriginalBindingError(f"object reference {name} must be a nonempty string")
    if type(value.size_bytes) is not int or value.size_bytes < 0:
        raise NativeOriginalBindingError("object size must be a nonnegative integer")
    if ".." in value.key.split("/"):
        raise NativeOriginalBindingError("object key must not contain parent traversal segments")
    _digest(value.sha256)
    retained_until = datetime.fromisoformat(value.retention_until.replace("Z", "+00:00"))
    if retained_until.tzinfo is None or retained_until.utcoffset() is None:
        raise NativeOriginalBindingError("object retention timestamp must include a timezone")
    return payload


@dataclass(frozen=True, slots=True)
class NativeOriginalBinding:
    """Exact original/authority/provider-version tuple, without storage admission.

    Provider keys and opaque versions are preserved, never treated as filesystem
    locators. Payload and stored-object hashes describe separate coordinates; the
    authenticating consumer must verify actual bytes under its representation.
    """

    subject: NativeOriginalSubject
    kind: NativeOriginalKind
    storage_authority: OriginalRef
    object_ref: ArtifactObjectRef
    payload_sha256: str
    locator: str
    schema: ClassVar[str] = _BINDING_SCHEMA

    def __post_init__(self) -> None:
        encode_native_original_binding(self)


def _binding_payload(value: NativeOriginalBinding) -> dict[str, NativeJsonValue]:
    if type(value) is not NativeOriginalBinding:
        raise NativeOriginalBindingError("unsupported original binding type")
    if type(value.kind) is not str or value.kind not in _KINDS:
        raise NativeOriginalBindingError("unsupported original kind")
    if type(value.storage_authority) is not OriginalRef:
        raise NativeOriginalBindingError("storage authority must be an OriginalRef")
    value.storage_authority.__post_init__()
    OriginalRef(value.locator, value.payload_sha256)
    payload: dict[str, NativeJsonValue] = {
        "schema": _BINDING_SCHEMA,
        "subject": decode_native_delivery_json(encode_native_original_subject(value.subject)),
        "kind": value.kind,
        "storage_authority": asdict(value.storage_authority),
        "object_ref": _object_payload(value.object_ref),
        "payload_sha256": value.payload_sha256,
        "locator": value.locator,
    }
    return payload


def encode_native_original_binding(value: NativeOriginalBinding) -> bytes:
    """Encode a complete revalidated binding, performing no provider operation."""
    try:
        return encode_native_delivery_json(_binding_payload(value))
    except (DbtPublishingError, TypeError, ValueError) as exc:
        raise NativeOriginalBindingError("original binding cannot be encoded") from exc


def decode_native_original_binding(payload: bytes) -> NativeOriginalBinding:
    """Decode bounded canonical bytes; provider provenance remains unverified."""
    try:
        raw = decode_native_delivery_json(payload)
        if encode_native_delivery_json(raw) != payload:
            raise NativeOriginalBindingError("original binding bytes must be canonical")
        if set(raw) != _BINDING_FIELDS or raw["schema"] != _BINDING_SCHEMA:
            raise NativeOriginalBindingError("binding requires exactly its versioned fields")
        authority = raw["storage_authority"]
        obj = raw["object_ref"]
        if type(authority) is not dict or set(authority) != {"locator", "sha256"}:
            raise NativeOriginalBindingError("storage authority reference fields are invalid")
        if type(obj) is not dict or set(obj) != _OBJECT_FIELDS:
            raise NativeOriginalBindingError("object reference requires exactly its six fields")
        subject = decode_native_original_subject(encode_native_delivery_json(raw["subject"]))
        return NativeOriginalBinding(
            subject=subject,
            kind=cast(NativeOriginalKind, raw["kind"]),
            storage_authority=OriginalRef(cast(str, authority["locator"]), cast(str, authority["sha256"])),
            object_ref=ArtifactObjectRef(**cast(dict, obj)),
            payload_sha256=cast(str, raw["payload_sha256"]),
            locator=cast(str, raw["locator"]),
        )
    except (DbtPublishingError, TypeError, ValueError) as exc:
        raise NativeOriginalBindingError("invalid original binding document") from exc


_STORAGE_SCHEMA = "dpone.native-original-storage-authority.v1"
_STORAGE_INTS = frozenset({"retention_days", "max_artifact_bytes"})
_STORAGE_FLAGS = frozenset({"conditional_create_authorized", "require_object_lock"})


class NativeOriginalStorageAuthorityError(ValueError):
    """A native storage authority has unsupported or malformed policy metadata."""


@dataclass(frozen=True, slots=True)
class NativeOriginalStorageAuthority:
    """Closed initial S3 policy description, not authenticated execution authority.

    Native admission requires create-only COMPLIANCE policy metadata. The caller
    still authenticates its original and verifies provider capabilities, current
    policy and credentials. MINIO_LOCAL_UNVERIFIED never supplies qualification.
    """

    provider: str
    provider_profile: str
    endpoint_authority_id: str
    bucket_or_container_authority_id: str
    kms_key_authority_id: str
    capability_evidence_sha256: str
    writer_scope: str
    artifact_prefix: str
    encryption_policy_sha256: str
    retention_policy_id: str
    retention_policy_sha256: str
    retention_days: int
    retention_issued_at: str
    retention_until: str
    max_artifact_bytes: int
    conditional_create_authorized: bool
    require_object_lock: bool
    object_lock_mode: str
    schema: ClassVar[str] = _STORAGE_SCHEMA

    def __post_init__(self) -> None:
        encode_native_original_storage_authority(self)


def _storage_payload(value: NativeOriginalStorageAuthority) -> dict[str, NativeJsonValue]:
    if type(value) is not NativeOriginalStorageAuthority:
        raise NativeOriginalStorageAuthorityError("unsupported native storage authority type")
    payload: dict[str, NativeJsonValue] = {field.name: getattr(value, field.name) for field in fields(value)}
    encode_native_delivery_json(payload)
    for name, item in payload.items():
        expected = int if name in _STORAGE_INTS else bool if name in _STORAGE_FLAGS else str
        if type(item) is not expected:
            raise NativeOriginalStorageAuthorityError(f"native storage field {name} has an invalid type")
    if (
        value.provider != "s3"
        or value.conditional_create_authorized is not True
        or value.require_object_lock is not True
        or value.object_lock_mode != "COMPLIANCE"
    ):
        raise NativeOriginalStorageAuthorityError("native storage requires S3 create-only COMPLIANCE policy")
    # Reuse the existing policy verbatim, without an infrastructure import or
    # inheritance from its deliberately permissive legacy defaults.
    S3ArtifactStorePolicy(**cast(dict, {name: item for name, item in payload.items() if name != "provider"}))
    return {"schema": _STORAGE_SCHEMA, **payload}


def encode_native_original_storage_authority(value: NativeOriginalStorageAuthority) -> bytes:
    """Return exact canonical policy bytes after structural revalidation."""
    try:
        return encode_native_delivery_json(_storage_payload(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeOriginalStorageAuthorityError("native storage authority cannot be encoded") from exc


def decode_native_original_storage_authority(payload: bytes) -> NativeOriginalStorageAuthority:
    """Decode the initial closed policy; do not authenticate or enroll it."""
    try:
        raw = decode_native_delivery_json(payload)
        expected = {field.name for field in fields(NativeOriginalStorageAuthority)} | {"schema"}
        if set(raw) != expected or raw["schema"] != _STORAGE_SCHEMA:
            raise NativeOriginalStorageAuthorityError("native storage authority requires exactly its versioned fields")
        if encode_native_delivery_json(raw) != payload:
            raise NativeOriginalStorageAuthorityError("native storage authority bytes must be canonical")
        return NativeOriginalStorageAuthority(
            **cast(dict, {name: item for name, item in raw.items() if name != "schema"})
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeOriginalStorageAuthorityError("invalid native storage authority document") from exc
