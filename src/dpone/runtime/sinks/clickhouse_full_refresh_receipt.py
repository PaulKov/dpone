"""Closed receipt codec for recoverable ClickHouse full-refresh publication."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks import clickhouse_full_refresh_contract as publication_contract


@dataclass(frozen=True, slots=True)
class FullRefreshPublicationReceipt:
    """Verified committed mapping retained until exact cleanup succeeds."""

    marker: publication_contract.FullRefreshPublicationMarker
    marker_table: str
    recovered_after_error: bool = False

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> FullRefreshPublicationReceipt:
        expected = {"marker", "marker_table", "recovered_after_error"}
        if set(raw) != expected or not isinstance(raw.get("marker"), Mapping):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_RECEIPT_INVALID", "receipt fields do not match v1"
            )
        marker_json = json.dumps(
            dict(raw["marker"]),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        marker = publication_contract.FullRefreshPublicationMarker.from_json(marker_json)
        marker_table = raw.get("marker_table")
        recovered = raw.get("recovered_after_error")
        if not isinstance(marker_table, str) or not marker_table or not isinstance(recovered, bool):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_RECEIPT_INVALID", "receipt identity is invalid"
            )
        return cls(marker=marker, marker_table=marker_table, recovered_after_error=recovered)

    def to_dict(self) -> dict[str, Any]:
        return {
            "marker": self.marker.to_dict(),
            "marker_table": self.marker_table,
            "recovered_after_error": self.recovered_after_error,
        }


__all__ = ["FullRefreshPublicationReceipt"]
