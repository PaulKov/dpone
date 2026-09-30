"""Closed, immutable selector and durable identity for native verification v2.

The default selector remains the released Python readback path. An explicit
target-local selection receives a new identity and journal keyspace; changing
any bound capability requires a new invocation. Process deadlines and secrets
are deliberately absent from this durable identity.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, TypedDict

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    WRITER_OUTCOMES,
    is_nonnegative_int,
)
from dpone.contracts.mssql_native_writer_event import (
    ARTIFACT_FIELDS as ARTIFACT_FIELDS,
)
from dpone.contracts.mssql_native_writer_event import (
    EVENT_FIELDS as EVENT_FIELDS,
)
from dpone.contracts.mssql_native_writer_event import (
    NEXT_EVENTS as NEXT_EVENTS,
)
from dpone.contracts.mssql_native_writer_event import (
    OBSERVATION_FIELDS as OBSERVATION_FIELDS,
)
from dpone.contracts.mssql_native_writer_event import (
    STAGE_FIELDS as STAGE_FIELDS,
)
from dpone.contracts.mssql_native_writer_event import (
    WRITER_FIELDS as WRITER_FIELDS,
)
from dpone.contracts.mssql_native_writer_event import (
    canonical_sha256 as canonical_sha256,
)
from dpone.contracts.mssql_native_writer_event import (
    is_sha256_digest as is_sha256_digest,
)
from dpone.contracts.mssql_native_writer_event import (
    validate_native_writer_event as validate_native_writer_event,
)
from dpone.contracts.strict_json import canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
WRITER_PROOF_CAPABILITIES = (BCP_STAGE_PROOF, SQLCLIENT_SESSION_PROOF)
NATIVE_WRITER_OUTCOMES = WRITER_OUTCOMES


class NativeVerificationBackend(StrEnum):
    """The only authored native verifier choices; omission selects v1."""

    PYTHON_READBACK = "python_readback"
    TARGET_LOCAL = "target_local"

    @classmethod
    def parse(cls, value: object | None) -> NativeVerificationBackend:
        """Resolve omission without accepting case folding, coercion, or aliases."""
        if value is None:
            return cls.PYTHON_READBACK
        if type(value) is not str:
            raise ValueError("mssql_native.invalid_verification_backend")
        try:
            return cls(value)
        except ValueError as error:
            raise ValueError("mssql_native.invalid_verification_backend") from error

    @property
    def identity_version(self) -> int:
        """Keep omitted/default manifests on the byte-compatible v1 path."""
        return 2 if self is self.TARGET_LOCAL else 1


class NativeImportBackend(StrEnum):
    """Closed native writer selection; omission preserves BCP."""

    BCP = "bcp"
    MSSQL_SQLCLIENT = "mssql_sqlclient"

    @classmethod
    def parse(cls, value: object | None) -> NativeImportBackend:
        if value is None:
            return cls.BCP
        if type(value) is not str:
            raise ValueError("mssql_native.invalid_import_backend")
        try:
            return cls(value)
        except ValueError as error:
            raise ValueError("mssql_native.invalid_import_backend") from error


@dataclass(frozen=True)
class NativeVerificationIdentityV2:
    """All versioned inputs that may change target-local interpretation.

    Digests are canonical lowercase SHA-256. P1 supplies domain-separated
    digests for the absent optional companion, so omission never weakens the
    identity comparison. The six plan bindings retain their v1 field names.
    """

    plan: NativeChunkPlan
    import_backend: str
    verification_backend: NativeVerificationBackend
    companion_protocol_sha256: str
    companion_package_sha256: str
    capability_layout_sha256: str
    digest_algorithm_id: str
    timeout_policy_sha256: str
    writer_proof_capability: str = BCP_STAGE_PROOF

    def __post_init__(self) -> None:
        if not isinstance(self.plan, NativeChunkPlan) or any(
            type(value) is not str or not value for value in asdict(self.plan).values()
        ):
            raise ValueError("mssql_native.invalid_v2_identity")
        if type(self.import_backend) is not str or self.import_backend not in {"bcp", "mssql_sqlclient"}:
            raise ValueError("mssql_native.invalid_v2_identity")
        if (self.import_backend, self.writer_proof_capability) not in {
            ("bcp", BCP_STAGE_PROOF),
            ("mssql_sqlclient", SQLCLIENT_SESSION_PROOF),
        }:
            raise ValueError("mssql_native.invalid_v2_identity")
        if self.verification_backend is not NativeVerificationBackend.TARGET_LOCAL:
            raise ValueError("mssql_native.invalid_v2_identity")
        for name in (
            "companion_protocol_sha256",
            "companion_package_sha256",
            "capability_layout_sha256",
            "timeout_policy_sha256",
        ):
            value = getattr(self, name)
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ValueError("mssql_native.invalid_v2_identity")
        if type(self.digest_algorithm_id) is not str or not self.digest_algorithm_id:
            raise ValueError("mssql_native.invalid_v2_identity")

    def document(self) -> dict[str, str]:
        """Flatten the six unchanged plan fields with the v2 bindings."""
        return {
            **asdict(self.plan),
            "import_backend": self.import_backend,
            "verification_backend": self.verification_backend.value,
            "writer_proof_capability": self.writer_proof_capability,
            "companion_protocol_sha256": self.companion_protocol_sha256,
            "companion_package_sha256": self.companion_package_sha256,
            "capability_layout_sha256": self.capability_layout_sha256,
            "digest_algorithm_id": self.digest_algorithm_id,
            "timeout_policy_sha256": self.timeout_policy_sha256,
        }

    @classmethod
    def from_document(cls, value: object) -> NativeVerificationIdentityV2:
        """Reconstruct one strict durable identity without guessing missing fields."""
        plan_fields = tuple(NativeChunkPlan.__dataclass_fields__)
        fields = {
            *plan_fields,
            "import_backend",
            "verification_backend",
            "writer_proof_capability",
            "companion_protocol_sha256",
            "companion_package_sha256",
            "capability_layout_sha256",
            "digest_algorithm_id",
            "timeout_policy_sha256",
        }
        if not isinstance(value, dict) or set(value) != fields or any(type(item) is not str for item in value.values()):
            raise ValueError("mssql_native.invalid_v2_identity")
        plan = NativeChunkPlan(*(value[field] for field in plan_fields))
        try:
            verifier = NativeVerificationBackend(value["verification_backend"])
        except ValueError as error:
            raise ValueError("mssql_native.invalid_v2_identity") from error
        return cls(
            plan=plan,
            import_backend=value["import_backend"],
            verification_backend=verifier,
            writer_proof_capability=value["writer_proof_capability"],
            companion_protocol_sha256=value["companion_protocol_sha256"],
            companion_package_sha256=value["companion_package_sha256"],
            capability_layout_sha256=value["capability_layout_sha256"],
            digest_algorithm_id=value["digest_algorithm_id"],
            timeout_policy_sha256=value["timeout_policy_sha256"],
        )

    @property
    def invocation_key(self) -> str:
        """RFC 8785 equivalent for this fixed-key, string-only JSON document."""
        return hashlib.sha256(canonical_json_bytes(self.document())).hexdigest()


def validated_native_receipt(value: object) -> NativeChunkReceipt:
    """Decode the unchanged receipt shape before it becomes v2 authority."""
    try:
        if not isinstance(value, dict) or set(value) != set(NativeChunkReceipt.__dataclass_fields__):
            raise ValueError
        receipt = NativeChunkReceipt(**value)
        if any(not is_nonnegative_int(getattr(receipt, name)) for name in ("ordinal", "rows", "encoded_bytes")):
            raise ValueError
        if any(not is_sha256_digest(getattr(receipt, name)) for name in ("file_sha256", "typed_digest")):
            raise ValueError
        if not receipt.attempt_id or not receipt.stage_id or not isinstance(receipt.consumed_part_evidence, dict):
            raise ValueError
        return receipt
    except (TypeError, ValueError) as error:
        raise WindowContractError("mssql_native.invalid_receipt") from error


def opaque_native_stage_id(identity: NativeVerificationIdentityV2, qualified_stage_id: str) -> str:
    """Domain-separate a physical receipt stage for coordinate-free events."""
    if type(qualified_stage_id) is not str or not qualified_stage_id:
        raise WindowContractError("mssql_native.invalid_stage_identity")
    return canonical_sha256(["dpone.mssql-native-stage.v2", identity.invocation_key, qualified_stage_id])


def matches_native_receipt(
    identity: NativeVerificationIdentityV2, receipt: NativeChunkReceipt, event: dict[str, Any]
) -> bool:
    """Require exact sealed artifact and opaque stage binding for one receipt."""
    artifact, stage = event["artifact_binding"], event["stage_binding"]
    return (
        receipt.attempt_id == event["attempt_id"]
        and receipt.ordinal == event["ordinal"]
        and opaque_native_stage_id(identity, receipt.stage_id) == stage["stage_id"]
        and all(
            getattr(receipt, name) == artifact[name]
            for name in ("rows", "encoded_bytes", "file_sha256", "typed_digest")
        )
    )


def ordered_native_receipts(chunks: dict[str, Any]) -> tuple[NativeChunkReceipt, ...]:
    """Require contiguous, independently verified stages before EOF authority."""
    if set(chunks) != {str(index) for index in range(len(chunks))}:
        raise WindowContractError("mssql_native.noncontiguous_receipts")
    if any(chunk["phase"] != "verified" for chunk in chunks.values()):
        raise WindowContractError("mssql_native.unverified_attempts")
    receipts = tuple(validated_native_receipt(chunks[str(index)]["receipt"]) for index in range(len(chunks)))
    if len({receipt.stage_id for receipt in receipts}) != len(receipts):
        raise WindowContractError("mssql_native.shared_stage_identity")
    return receipts


class NativeCompletionDigests(TypedDict):
    """Typed EOF digest fields shared by journal completion and reload."""

    rows: int
    receipt_digest: str
    metadata_digest: str


def native_completion_digests(
    receipts: tuple[NativeChunkReceipt, ...], metadata: dict[str, Any]
) -> NativeCompletionDigests:
    """Bind the unchanged receipt ordering and metadata to EOF authority."""

    def legacy_digest(value: object) -> str:
        # Receipt/publication authority keeps v1 escaping; only v2 identity/events use RFC 8785 bytes.
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    return {
        "rows": sum(receipt.rows for receipt in receipts),
        "receipt_digest": legacy_digest([asdict(receipt) for receipt in receipts]),
        "metadata_digest": legacy_digest(metadata),
    }
