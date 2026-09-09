"""Assertions for postgres→Kafka wide live certification (JSON message payloads)."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any


def parse_envelope(raw_value: bytes | None) -> dict[str, Any]:
    assert raw_value is not None, "Kafka message value must not be None"
    payload = json.loads(raw_value.decode("utf-8"))
    assert isinstance(payload, dict), f"expected JSON object, got {type(payload).__name__}"
    return payload


def message_data(envelope: Mapping[str, Any]) -> dict[str, Any]:
    data = envelope.get("data")
    if isinstance(data, dict):
        return dict(data)
    return dict(envelope)


def consume_envelopes(
    kafka,
    topic: str,
    *,
    expected: int,
    group_id: str,
) -> list[dict[str, Any]]:
    consumer = kafka.create_consumer(
        group_id=group_id,
        options={"auto.offset.reset": "earliest", "enable.partition.eof": True},
    )
    consumer.subscribe([topic])
    envelopes: list[dict[str, Any]] = []
    empty_polls = 0
    while len(envelopes) < expected and empty_polls < 40:
        message = consumer.poll(0.25)
        if message is None or message.error():
            empty_polls += 1
            continue
        empty_polls = 0
        envelopes.append(parse_envelope(message.value()))
    consumer.close()
    assert len(envelopes) == expected, f"expected {expected} Kafka messages, got {len(envelopes)}"
    return envelopes


def latest_data_by_id(envelopes: Sequence[Mapping[str, Any]]) -> dict[int, dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    for envelope in envelopes:
        data = message_data(envelope)
        latest[int(data["id"])] = data
    return latest


def assert_ids_present(envelopes: Sequence[Mapping[str, Any]], expected_ids: Iterable[int]) -> None:
    ids = {int(message_data(envelope)["id"]) for envelope in envelopes}
    assert ids == set(expected_ids), f"expected ids {set(expected_ids)}, got {ids}"


def assert_ops(envelopes: Sequence[Mapping[str, Any]], *, expected: Iterable[str]) -> None:
    ops = {str(envelope.get("op", "upsert")).lower() for envelope in envelopes}
    assert ops == {item.lower() for item in expected}, f"expected ops {set(expected)}, got {ops}"


def assert_typed_spot_checks(
    envelopes: Sequence[Mapping[str, Any]],
    *,
    row_id: int = 1,
    expected_name: str = "row-one",
) -> None:
    by_id = latest_data_by_id(envelopes)
    assert row_id in by_id, f"missing message for id={row_id}"
    row = by_id[row_id]
    assert str(row["id"]) == str(row_id)
    assert row["c_name"] == expected_name
    assert row["c_varchar"] == "alpha,comma"
    assert str(row["c_bool"]).lower() in {"1", "true", "t"}
    assert Decimal(str(row["c_decimal"])) == Decimal("12.3456")
    assert int(str(row["c_bigint"])) == 7000000000
    assert row["c_bytea"], "bytea column should round-trip as non-empty CSV/JSON wire value"


__all__ = [
    "assert_ids_present",
    "assert_ops",
    "assert_typed_spot_checks",
    "consume_envelopes",
    "latest_data_by_id",
    "message_data",
    "parse_envelope",
]
