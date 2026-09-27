"""Canonical JSON Schema producer for one MSSQL native writer event."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_native_verification import (
    ARTIFACT_FIELDS,
    EVENT_FIELDS,
    NEXT_EVENTS,
    OBSERVATION_FIELDS,
    STAGE_FIELDS,
    WRITER_FIELDS,
)
from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    WRITER_OUTCOMES,
)

_SHA = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_NONNEGATIVE = {"type": "integer", "minimum": 0}
_YEAR = r"(?:[0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]|[0-9][1-9][0-9]{2}|[1-9][0-9]{3})"
_LEAP_YEAR = r"(?!(?:0000))(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:[02468][048]|[13579][26])00)"
_RFC3339_UTC = (
    rf"^(?:(?:{_YEAR}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12][0-9]|3[01])"
    rf"|(?:0[469]|11)-(?:0[1-9]|[12][0-9]|30)|02-(?:0[1-9]|1[0-9]|2[0-8])))"
    rf"|(?:{_LEAP_YEAR}-02-29))T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]+)?Z$"
)


def _closed(properties: dict[str, Any], required: frozenset[str] | set[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(required),
        "properties": properties,
    }


def _nullable(value: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [{"type": "null"}, value]}


def _bindings() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    artifact = _closed(
        {
            "ordinal": _NONNEGATIVE,
            "rows": _NONNEGATIVE,
            "encoded_bytes": _NONNEGATIVE,
            "file_sha256": _SHA,
            "typed_digest": _SHA,
        },
        ARTIFACT_FIELDS,
    )
    stage = _closed(
        {
            "stage_id": _SHA,
            "owner_binding_sha256": _SHA,
            "object_id": {"type": "integer", "minimum": 1},
            "schema_sha256": _SHA,
        },
        STAGE_FIELDS,
    )
    writer = _closed(
        {
            "import_backend": {"enum": ["bcp", "mssql_sqlclient"]},
            "writer_proof_capability": {"enum": [BCP_STAGE_PROOF, SQLCLIENT_SESSION_PROOF]},
            "protocol_sha256": _SHA,
            "package_sha256": _SHA,
            "capability_sha256": _SHA,
            "grant_token_sha256": _SHA,
            "timeout_policy_sha256": _SHA,
        },
        WRITER_FIELDS,
    )
    writer["allOf"] = [
        {
            "if": {"properties": {"import_backend": {"const": "bcp"}}},
            "then": {"properties": {"writer_proof_capability": {"const": BCP_STAGE_PROOF}}},
            "else": {"properties": {"writer_proof_capability": {"const": SQLCLIENT_SESSION_PROOF}}},
        }
    ]
    observation = _closed(
        {
            "writer_outcome": {"enum": sorted(WRITER_OUTCOMES)},
            "input_rows_consumed": _nullable(_NONNEGATIVE),
            "row_count": _nullable(_NONNEGATIVE),
            "count_overflow": {"type": ["boolean", "null"]},
            "limbs": _nullable(
                {
                    "type": "array",
                    "minItems": 8,
                    "maxItems": 8,
                    "items": {"type": "string", "pattern": "^(0|[1-9][0-9]*)$"},
                }
            ),
            "quiescence": {"enum": ["unverified", "proved", "failed"]},
            "diagnostic_code": {
                "type": "string",
                "pattern": "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*$",
            },
        },
        OBSERVATION_FIELDS,
    )
    return artifact, stage, writer, observation


def mssql_native_writer_state_v2_schema() -> dict[str, Any]:
    """Return the deterministic closed Draft 7 schema for shareable v2 events."""
    artifact, stage, writer, observation = _bindings()
    early = ["INTENT", "STAGE_OWNED"]
    preobservation = [*early, "GRANTED", "WRITING"]
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://dpone.dev/schemas/dpone.mssql-native-writer-state.v2.schema.json",
        "title": "dpone MSSQL native writer state v2 event",
        **_closed(
            {
                "schema_version": {"const": 2},
                "kind": {"const": "dpone.mssql-native-writer-state.v2"},
                "invocation_key": _SHA,
                "ordinal": _NONNEGATIVE,
                "attempt_id": _SHA,
                "sequence": _NONNEGATIVE,
                "previous_sha256": _nullable(_SHA),
                "event": {"enum": sorted(NEXT_EVENTS)},
                "stage_binding": _nullable(stage),
                "artifact_binding": artifact,
                "writer_binding": _nullable(writer),
                "observation": _nullable(observation),
                "created_at": {
                    "type": "string",
                    "format": "date-time",
                    "pattern": _RFC3339_UTC,
                },
            },
            EVENT_FIELDS,
        ),
        "allOf": [
            {
                "if": {"properties": {"event": {"const": "INTENT"}}},
                "then": {
                    "properties": {
                        "previous_sha256": {"type": "null"},
                        "sequence": {"const": 0},
                        "stage_binding": {"type": "null"},
                    }
                },
                "else": {
                    "properties": {
                        "previous_sha256": _SHA,
                        "sequence": {"type": "integer", "minimum": 1},
                        "stage_binding": stage,
                    }
                },
            },
            {
                "if": {"properties": {"event": {"enum": early}}},
                "then": {"properties": {"writer_binding": {"type": "null"}}},
                "else": {"properties": {"writer_binding": writer}},
            },
            {
                "if": {"properties": {"event": {"enum": preobservation}}},
                "then": {"properties": {"observation": {"type": "null"}}},
                "else": {"properties": {"observation": observation}},
            },
        ],
    }


__all__ = ["mssql_native_writer_state_v2_schema"]
