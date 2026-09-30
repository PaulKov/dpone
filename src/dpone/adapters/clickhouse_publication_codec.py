"""Closed, strict JSON codec for immutable v2 publication journal records."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from dpone.contracts.clickhouse_authority import AuthorityError
from dpone.contracts.clickhouse_publication import (
    PublicationIntent,
    PublicationObservation,
    PublicationRecord,
    PublicationState,
    PublicationTable,
    choose_publication,
)


def _object(value: Any, keys: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != set(keys.split()):
        raise ValueError("Unexpected record fields")
    return value


def _text(value: Any) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError("Expected canonical text")
    return value


def _boolean(value: Any) -> bool:
    if type(value) is not bool:
        raise ValueError("Expected boolean")
    return value


def _table(value: Any) -> PublicationTable | None:
    if value is None:
        return None
    row = _object(value, "uuid design_digest content_digest rows partitions engine")
    if type(row["partitions"]) is not list:
        raise ValueError("Expected partition inventory")
    return PublicationTable(
        _text(row["uuid"]),
        _text(row["design_digest"]),
        _text(row["content_digest"]),
        row["rows"],
        tuple(_text(p) for p in row["partitions"]),
        _text(row["engine"]),
    )


def _observation(value: Any) -> PublicationObservation:
    row = _object(value, "subject database_engine target candidate catalog_complete side_effects_safe")
    if type(row["subject"]) is not list or len(row["subject"]) != 4:
        raise ValueError("Expected complete physical subject")
    subject = row["subject"]
    result = PublicationObservation(
        (_text(subject[0]), _text(subject[1]), _text(subject[2]), _text(subject[3])),
        _text(row["database_engine"]),
        _table(row["target"]),
        _table(row["candidate"]),
        _boolean(row["catalog_complete"]),
        _boolean(row["side_effects_safe"]),
    )
    result.require_supported()
    return result


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("Nonfinite JSON value")


def decode_record(payload: str) -> PublicationRecord:
    """Reject malformed records, unsupported observations and divergent plans."""
    try:
        row = _object(
            json.loads(payload, object_pairs_hook=_unique_pairs, parse_constant=_reject_constant),
            "intent state claim_granted schema_version",
        )
        if row["schema_version"] != "dpone.clickhouse.guarded-publication.v2":
            raise ValueError("Unsupported journal schema")
        raw = _object(row["intent"], "operation_id before method reason partition_id")
        intent = choose_publication(_text(raw["operation_id"]), _observation(raw["before"]))
        if (raw["method"], raw["reason"], raw["partition_id"]) != (intent.method, intent.reason, intent.partition_id):
            raise ValueError("Divergent publication plan")
        state = PublicationState(_text(row["state"]))
        claimed = _boolean(row["claim_granted"])
        if (
            state == PublicationState.PREPARED
            and claimed
            or state in {PublicationState.CLAIMED, PublicationState.COMMITTED}
            and not claimed
        ):
            raise ValueError("Invalid claim history")
        return PublicationRecord(intent, state, claimed)
    except (TypeError, ValueError, KeyError, RecursionError) as error:
        raise AuthorityError("Invalid durable publication record") from error


def encode_record(record: PublicationRecord) -> str:
    """Validate before persisting deterministic bytes; enum spelling stays v2."""
    try:
        if not isinstance(record, PublicationRecord) or not isinstance(record.intent, PublicationIntent):
            raise ValueError("Expected publication record")
        if not isinstance(record.state, PublicationState):
            raise ValueError("Expected publication state")
        payload = json.dumps(asdict(record), sort_keys=True, separators=(",", ":"), allow_nan=False)
        if decode_record(payload) != record:
            raise ValueError("Noncanonical publication record")
        return payload
    except (TypeError, ValueError, RecursionError) as error:
        raise AuthorityError("Invalid durable publication record") from error
