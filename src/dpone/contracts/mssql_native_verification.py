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
from datetime import datetime
from enum import StrEnum
from typing import Any, TypedDict

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    is_nonnegative_int,
    valid_native_writer_observation,
    validate_bcp_writer_event,
)
from dpone.contracts.strict_json import canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


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

    @property
    def invocation_key(self) -> str:
        """RFC 8785 equivalent for this fixed-key, string-only JSON document."""
        return hashlib.sha256(canonical_json_bytes(self.document())).hexdigest()


_UTC = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z\Z")
EVENT_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "invocation_key",
        "ordinal",
        "attempt_id",
        "sequence",
        "previous_sha256",
        "event",
        "stage_binding",
        "artifact_binding",
        "writer_binding",
        "observation",
        "created_at",
    }
)
ARTIFACT_FIELDS = frozenset({"ordinal", "rows", "encoded_bytes", "file_sha256", "typed_digest"})
STAGE_FIELDS = frozenset({"stage_id", "owner_binding_sha256", "object_id", "schema_sha256"})
WRITER_FIELDS = frozenset(
    {
        "import_backend",
        "writer_proof_capability",
        "protocol_sha256",
        "package_sha256",
        "capability_sha256",
        "grant_token_sha256",
        "timeout_policy_sha256",
    }
)
OBSERVATION_FIELDS = frozenset(
    {
        "writer_outcome",
        "input_rows_consumed",
        "row_count",
        "count_overflow",
        "limbs",
        "quiescence",
        "diagnostic_code",
    }
)
NEXT_EVENTS: dict[str, frozenset[str]] = {
    "INTENT": frozenset({"STAGE_OWNED"}),
    "STAGE_OWNED": frozenset({"GRANTED"}),
    "GRANTED": frozenset({"WRITING", "UNKNOWN"}),
    "WRITING": frozenset({"WRITER_TERMINAL", "UNKNOWN"}),
    "WRITER_TERMINAL": frozenset({"QUIESCENT", "UNKNOWN"}),
    "QUIESCENT": frozenset({"VERIFIED", "UNKNOWN"}),
    "VERIFIED": frozenset({"FAILED_RETIRABLE"}),
    "UNKNOWN": frozenset({"QUIESCENT", "PARTIAL_PROVED", "INCIDENT_RETAINED"}),
    "PARTIAL_PROVED": frozenset({"FAILED_RETIRABLE"}),
    "FAILED_RETIRABLE": frozenset({"RETIRED"}),
    "INCIDENT_RETAINED": frozenset(),
    "RETIRED": frozenset(),
}


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def is_sha256_digest(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def validate_native_writer_event(
    identity: NativeVerificationIdentityV2,
    event: object,
    previous: dict[str, Any] | None,
    sequence: int,
    attempt_id: str,
) -> None:
    if not isinstance(event, dict) or set(event) != EVENT_FIELDS:
        raise ValueError("event shape")
    if (event["schema_version"], event["kind"], event["invocation_key"]) != (
        2,
        "dpone.mssql-native-writer-state.v2",
        identity.invocation_key,
    ) or type(event["schema_version"]) is not int:
        raise ValueError("event identity")
    if (
        not is_nonnegative_int(event["ordinal"])
        or event["attempt_id"] != attempt_id
        or type(event["sequence"]) is not int
        or event["sequence"] != sequence
    ):
        raise ValueError("event sequence")
    if not isinstance(event["created_at"], str) or _UTC.fullmatch(event["created_at"]) is None:
        raise ValueError("event timestamp")
    try:
        datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("event timestamp") from error
    if previous is None:
        if event["event"] != "INTENT" or event["previous_sha256"] is not None:
            raise ValueError("event beginning")
    elif (
        event["event"] not in NEXT_EVENTS[previous["event"]]
        or event["previous_sha256"] != canonical_sha256(previous)
        or event["ordinal"] != previous["ordinal"]
        or event["artifact_binding"] != previous["artifact_binding"]
    ):
        raise ValueError("event transition/link")
    if event["event"] not in NEXT_EVENTS:
        raise ValueError("event type")
    artifact = event["artifact_binding"]
    if not isinstance(artifact, dict) or set(artifact) != ARTIFACT_FIELDS or artifact["ordinal"] != event["ordinal"]:
        raise ValueError("artifact shape")
    if not all(is_nonnegative_int(artifact[name]) for name in ("ordinal", "rows", "encoded_bytes")) or not all(
        is_sha256_digest(artifact[name]) for name in ("file_sha256", "typed_digest")
    ):
        raise ValueError("artifact values")
    stage = event["stage_binding"]
    if event["event"] == "INTENT":
        if stage is not None:
            raise ValueError("premature stage")
    elif (
        not isinstance(stage, dict)
        or set(stage) != STAGE_FIELDS
        or not (
            is_sha256_digest(stage["stage_id"])
            and is_sha256_digest(stage["owner_binding_sha256"])
            and is_sha256_digest(stage["schema_sha256"])
            and type(stage["object_id"]) is int
            and stage["object_id"] > 0
        )
        or (previous is not None and previous["stage_binding"] is not None and stage != previous["stage_binding"])
    ):
        raise ValueError("stage binding")
    writer = event["writer_binding"]
    if event["event"] in {"INTENT", "STAGE_OWNED"}:
        if writer is not None:
            raise ValueError("premature writer")
    elif (
        not isinstance(writer, dict)
        or set(writer) != WRITER_FIELDS
        or writer["import_backend"] != identity.import_backend
        or writer["writer_proof_capability"] != identity.writer_proof_capability
        or not all(
            is_sha256_digest(writer[name]) for name in WRITER_FIELDS - {"import_backend", "writer_proof_capability"}
        )
        or (
            writer["protocol_sha256"] != identity.companion_protocol_sha256
            or writer["package_sha256"] != identity.companion_package_sha256
            or writer["capability_sha256"] != identity.capability_layout_sha256
            or writer["timeout_policy_sha256"] != identity.timeout_policy_sha256
        )
        or (previous is not None and previous["writer_binding"] is not None and writer != previous["writer_binding"])
    ):
        raise ValueError("writer binding")
    observation = event["observation"]
    if event["event"] in {"INTENT", "STAGE_OWNED", "GRANTED", "WRITING"}:
        if observation is not None:
            raise ValueError("premature observation")
    elif (
        not isinstance(observation, dict)
        or set(observation) != OBSERVATION_FIELDS
        or not valid_native_writer_observation(observation)
    ):
        raise ValueError("observation")
    if event["event"] in {"QUIESCENT", "PARTIAL_PROVED"} and observation["quiescence"] != "proved":
        raise ValueError("quiescence proof")
    if identity.writer_proof_capability == BCP_STAGE_PROOF:
        validate_bcp_writer_event(event, previous, artifact["rows"])
    if event["event"] == "VERIFIED" and (
        observation["quiescence"] != "proved"
        or observation["row_count"] != artifact["rows"]
        or observation["count_overflow"] is not False
        or observation["limbs"] is None
    ):
        raise ValueError("verification proof")


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
    if not chunks or set(chunks) != {str(index) for index in range(len(chunks))}:
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
