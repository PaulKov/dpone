"""Kafka key and envelope helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone.runtime.kafka.config import KafkaEnvelopeMode, KafkaKeyMode, KafkaSinkOptions


class KafkaKeyBuilder:
    """Builds Kafka message keys from dpone row data."""

    def __init__(self, options: KafkaSinkOptions):
        self.options = options

    def build(self, row: Mapping[str, Any], unique_key: str | list[str] | None) -> bytes | None:
        mode = self.options.key.mode
        if mode == KafkaKeyMode.NULL:
            return None
        if mode == KafkaKeyMode.HASH_ROW:
            payload = json.dumps(dict(row), sort_keys=True, default=str, separators=(",", ":"))
            return hashlib.sha256(payload.encode("utf-8")).hexdigest().encode("utf-8")
        fields = self.options.key.fields or _unique_key_fields(unique_key)
        if not fields:
            return None
        values = [row.get(field) for field in fields]
        if any(value is None for value in values):
            return None
        return "|".join(str(value) for value in values).encode("utf-8")


class KafkaEnvelopeBuilder:
    """Builds optional dpone envelopes for Kafka values."""

    def __init__(self, options: KafkaSinkOptions):
        self.options = options

    def build_value(
        self,
        row: Mapping[str, Any],
        *,
        op: str,
        metadata: Mapping[str, Any],
        source: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        clean_row = {key: value for key, value in row.items() if not str(key).startswith("__dpone_")}
        if self.options.always_envelope or self.options.envelope == KafkaEnvelopeMode.DPONE:
            return {
                "op": op,
                "data": clean_row,
                "metadata": dict(metadata),
                "source": dict(source),
                "schema_version": 1,
            }
        return clean_row


def _unique_key_fields(unique_key: str | list[str] | None) -> tuple[str, ...]:
    if unique_key is None:
        return ()
    if isinstance(unique_key, str):
        return (unique_key,)
    return tuple(str(item) for item in unique_key)
