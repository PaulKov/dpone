"""Immutable authority and retirement receipts for native parent journal v4."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


def _hash(value: str, field: str) -> None:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"mssql_native.parent_invalid:{field}")


def canonical_digest(value: object) -> str:
    """Digest a closed JSON value with stable bytes."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class NativeParentAuthority:
    """Settled publication or authoritative abort; unknown outcomes are excluded."""

    kind: str
    digest: str
    fence: int

    def __post_init__(self) -> None:
        if type(self.kind) is not str or self.kind not in {"published", "aborted"}:
            raise ValueError("mssql_native.parent_unsettled")
        _hash(self.digest, "authority_digest")
        if type(self.fence) is not int or self.fence < 1:
            raise ValueError("mssql_native.parent_invalid:fence")


@dataclass(frozen=True)
class NativeChunkRetirementReceipt:
    """Exact source-free proof that one immutable chunk attempt was retired."""

    ordinal: int
    attempt_id: str
    parent_authority_digest: str
    object_incarnation_sha256: str
    verification_receipt_sha256: str
    implementation_sha256: str
    drop_operation_sha256: str
    absence_sha256: str
    terminal_sha256: str
    directory_sha256: str
    capacity_sha256: str

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or self.ordinal < 0 or type(self.attempt_id) is not str or not self.attempt_id:
            raise ValueError("mssql_native.invalid_chunk_retirement")
        for field in (
            "parent_authority_digest",
            "object_incarnation_sha256",
            "verification_receipt_sha256",
            "implementation_sha256",
            "drop_operation_sha256",
            "absence_sha256",
            "terminal_sha256",
            "directory_sha256",
            "capacity_sha256",
        ):
            _hash(getattr(self, field), field)

    def to_dict(self) -> dict[str, Any]:
        """Return the closed durable representation."""
        return asdict(self)


@dataclass(frozen=True)
class NativeParentRetirementReceipt:
    """Ordered retirement authority for every parent chunk."""

    authority_digest: str
    chunks: tuple[NativeChunkRetirementReceipt, ...]

    def __post_init__(self) -> None:
        _hash(self.authority_digest, "authority_digest")
        if (
            type(self.chunks) is not tuple
            or not self.chunks
            or any(type(chunk) is not NativeChunkRetirementReceipt for chunk in self.chunks)
            or tuple(chunk.ordinal for chunk in self.chunks) != tuple(range(len(self.chunks)))
            or any(chunk.parent_authority_digest != self.authority_digest for chunk in self.chunks)
        ):
            raise ValueError("mssql_native.invalid_parent_retirement")

    @property
    def digest(self) -> str:
        """Bind authority and every ordered nested proof."""
        return canonical_digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Return the closed durable representation."""
        return {
            "authority_digest": self.authority_digest,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
        }

    @classmethod
    def from_dict(cls, value: object) -> NativeParentRetirementReceipt:
        """Decode only the exact v4 retirement receipt shape."""
        if not isinstance(value, dict) or set(value) != {"authority_digest", "chunks"}:
            raise ValueError("mssql_native.invalid_parent_retirement")
        chunks = value["chunks"]
        if not isinstance(chunks, list):
            raise ValueError("mssql_native.invalid_parent_retirement")
        try:
            decoded = tuple(NativeChunkRetirementReceipt(**chunk) for chunk in chunks)
            return cls(value["authority_digest"], decoded)
        except (TypeError, KeyError) as error:
            raise ValueError("mssql_native.invalid_parent_retirement") from error


@dataclass(frozen=True)
class NativeCheckpointReceipt:
    """Exact acknowledged checkpoint CAS bound to retired parent authority."""

    parent_retirement_digest: str
    target_id: str
    window_fingerprint: str
    fence: int
    checkpoint_cas_revision: int
    checkpoint_sha256: str

    def __post_init__(self) -> None:
        _hash(self.parent_retirement_digest, "parent_retirement_digest")
        _hash(self.checkpoint_sha256, "checkpoint_sha256")
        if any(type(value) is not str or not value for value in (self.target_id, self.window_fingerprint)):
            raise ValueError("mssql_native.invalid_checkpoint_identity")
        if type(self.fence) is not int or self.fence < 1:
            raise ValueError("mssql_native.invalid_checkpoint_fence")
        if type(self.checkpoint_cas_revision) is not int or self.checkpoint_cas_revision < 0:
            raise ValueError("mssql_native.invalid_checkpoint_cas")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> NativeCheckpointReceipt:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("mssql_native.invalid_checkpoint_receipt")
        try:
            return cls(**value)
        except TypeError as error:
            raise ValueError("mssql_native.invalid_checkpoint_receipt") from error


_V4_FIELDS = {
    "phase",
    "prepared",
    "receipt",
    "authority",
    "chunk_retirements",
    "retirement_receipt",
    "checkpoint_receipt",
    "checkpoint_receipt_digest",
}
V4_PARENT_PHASES = {
    "preparing",
    "prepared",
    "publishing",
    "published",
    "abort_required",
    "aborted",
    "retirement_required",
    "retiring",
    "retired",
    "checkpoint_required",
    "succeeded",
}
_UNSETTLED_PHASES = {"preparing", "prepared", "publishing", "abort_required"}
_PRE_RETIREMENT_PHASES = _UNSETTLED_PHASES | {"published", "aborted", "retirement_required"}


def settled_parent_authority(
    data: dict[str, Any], kind: str, receipt: dict[str, Any], fence: int
) -> NativeParentAuthority:
    """Bind exact preparation, parent plan, window and ordered verified chunks."""
    chunks = [
        {
            "ordinal": int(ordinal),
            "attempt_id": chunk["receipt"]["attempt_id"],
            "stage_id": chunk["receipt"]["stage_id"],
            "file_sha256": chunk["receipt"]["file_sha256"],
            "typed_digest": chunk["receipt"]["typed_digest"],
        }
        for ordinal, chunk in sorted(data["chunks"].items(), key=lambda item: int(item[0]))
    ]
    digest = canonical_digest(
        {
            "identity": data["identity"],
            "prepared": data["publication"]["prepared"],
            "complete": data["complete"],
            "chunks": chunks,
            "kind": kind,
            "receipt": receipt,
            "fence": fence,
        }
    )
    return NativeParentAuthority(kind, digest, fence)


def validate_v4_publication(value: dict[str, Any], publication: object) -> None:
    """Reject reinterpretation, premature proofs and changed nested bindings."""
    if not isinstance(publication, dict) or set(publication) != _V4_FIELDS:
        raise ValueError("invalid v4 publication")
    phase = publication["phase"]
    if phase not in V4_PARENT_PHASES or not isinstance(publication["prepared"], dict) or not publication["prepared"]:
        raise ValueError("invalid v4 publication")
    authority = _validated_authority(value, publication, phase)
    chunks = _validated_chunks(value, publication, authority)
    retirement = publication["retirement_receipt"]
    if phase in {"retired", "checkpoint_required", "succeeded"}:
        decoded = NativeParentRetirementReceipt.from_dict(retirement)
        if decoded.to_dict() != retirement or retirement["chunks"] != chunks or len(chunks) != len(value["chunks"]):
            raise ValueError("invalid retirement receipt")
    elif retirement is not None:
        raise ValueError("premature retirement receipt")
    checkpoint = publication["checkpoint_receipt"]
    checkpoint_digest = publication["checkpoint_receipt_digest"]
    if phase == "succeeded":
        decoded_checkpoint = NativeCheckpointReceipt.from_dict(checkpoint)
        if (
            not isinstance(retirement, dict)
            or not _checkpoint_matches(value, decoded_checkpoint, retirement)
            or checkpoint_digest != canonical_digest(decoded_checkpoint.to_dict())
        ):
            raise ValueError("changed checkpoint receipt")
    elif checkpoint is not None or checkpoint_digest is not None:
        raise ValueError("premature checkpoint receipt")
    if value["phase"] != "stage_complete":
        raise ValueError("invalid v4 parent phase")


def _validated_authority(
    value: dict[str, Any], publication: dict[str, Any], phase: str
) -> NativeParentAuthority | None:
    authority = publication["authority"]
    if phase in V4_PARENT_PHASES - _UNSETTLED_PHASES:
        decoded = NativeParentAuthority(**authority)
        if publication["receipt"] is None or decoded != settled_parent_authority(
            value, decoded.kind, publication["receipt"], decoded.fence
        ):
            raise ValueError("changed parent authority")
        if (phase == "published" and decoded.kind != "published") or (phase == "aborted" and decoded.kind != "aborted"):
            raise ValueError("invalid parent outcome")
        return decoded
    if authority is not None or publication["receipt"] is not None:
        raise ValueError("premature parent authority")
    return None


def _validated_chunks(
    value: dict[str, Any], publication: dict[str, Any], authority: NativeParentAuthority | None
) -> list[dict[str, Any]]:
    chunks = publication["chunk_retirements"]
    if not isinstance(chunks, list) or publication["phase"] in _PRE_RETIREMENT_PHASES and chunks:
        raise ValueError("premature chunk retirements")
    if publication["phase"] in {"retired", "checkpoint_required", "succeeded"} and len(chunks) != len(value["chunks"]):
        raise ValueError("incomplete chunk retirements")
    for ordinal, chunk in enumerate(chunks):
        decoded = NativeChunkRetirementReceipt(**chunk)
        expected = value["chunks"].get(str(ordinal))
        if (
            decoded.ordinal != ordinal
            or authority is None
            or decoded.parent_authority_digest != authority.digest
            or not isinstance(expected, dict)
            or not isinstance(expected.get("receipt"), dict)
            or decoded.attempt_id != expected["receipt"].get("attempt_id")
            or decoded.verification_receipt_sha256 != canonical_digest(expected["receipt"])
        ):
            raise ValueError("unordered chunk retirements")
    return chunks


def checkpoint_matches(
    value: dict[str, Any], receipt: NativeCheckpointReceipt, retirement: NativeParentRetirementReceipt
) -> bool:
    """Check exact journal bindings; caller separately proves current fence."""
    return _checkpoint_matches(value, receipt, retirement.to_dict())


def _checkpoint_matches(value: dict[str, Any], receipt: NativeCheckpointReceipt, retirement: dict[str, Any]) -> bool:
    return (
        receipt.parent_retirement_digest == canonical_digest(retirement)
        and receipt.target_id == value["identity"]["target_id"]
        and receipt.window_fingerprint == value["identity"]["window_fingerprint"]
    )
