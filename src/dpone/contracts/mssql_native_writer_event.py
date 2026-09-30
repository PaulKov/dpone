"""Closed transition validation for native target-writer journal events."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any

from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    is_nonnegative_int,
    valid_native_writer_observation,
    validate_bcp_writer_event,
)
from dpone.contracts.strict_json import canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
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
    """Return canonical JSON SHA-256 for a journal authority value."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def is_sha256_digest(value: object) -> bool:
    """Accept only lowercase canonical SHA-256 text."""

    return type(value) is str and _SHA256.fullmatch(value) is not None


def validate_native_writer_event(
    identity: Any,
    event: object,
    previous: dict[str, Any] | None,
    sequence: int,
    attempt_id: str,
) -> None:
    """Validate one immutable event and its exact predecessor binding."""

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


__all__ = ["canonical_sha256", "is_sha256_digest", "validate_native_writer_event"]
