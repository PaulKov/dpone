"""Canonical temporal partition metadata for Airflow asset integration."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_DIMENSION_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_GRANULARITY_KEY_FORMATS = {
    "hour": "%Y-%m-%dT%H",
    "day": "%Y-%m-%d",
    "month": "%Y-%m",
}
_MAX_TEXT_LENGTH = 128


class AssetPartitionContractError(ValueError):
    """A stable build-time partition contract failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class AssetPartitionSpec:
    """One canonical temporal asset partition dimension."""

    dimension: str
    granularity: str
    timezone: str
    key_format: str
    source: str = "dag_schedule"
    type: str = "temporal"

    @property
    def identity(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.dimension,
            self.type,
            self.granularity,
            self.timezone,
            self.key_format,
            self.source,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "dimensions": {
                self.dimension: {
                    "type": self.type,
                    "granularity": self.granularity,
                    "timezone": self.timezone,
                    "key_format": self.key_format,
                    "source": self.source,
                }
            }
        }


def parse_asset_partition(raw: object) -> AssetPartitionSpec | None:
    """Normalize one v1 partition mapping or return ``None`` when absent."""

    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise AssetPartitionContractError("asset_partition_invalid", "asset partition must be a mapping")
    dimensions = raw.get("dimensions")
    if not isinstance(dimensions, Mapping) or len(dimensions) != 1:
        raise AssetPartitionContractError(
            "asset_partition_dimension_count_invalid",
            "asset partition v1 requires exactly one dimensions entry",
        )
    dimension, raw_dimension = next(iter(dimensions.items()))
    dimension_name = str(dimension).strip()
    if not _DIMENSION_NAME.fullmatch(dimension_name):
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            "asset partition dimension must be a bounded identifier",
        )
    if not isinstance(raw_dimension, Mapping):
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            f"asset partition dimension {dimension_name!r} must be a mapping",
        )
    partition_type = _bounded_text(raw_dimension.get("type") or "temporal", field="type")
    if partition_type != "temporal":
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            "asset partition v1 supports only type=temporal",
        )
    granularity = _bounded_text(raw_dimension.get("granularity"), field="granularity")
    if granularity not in _GRANULARITY_KEY_FORMATS:
        choices = ", ".join(sorted(_GRANULARITY_KEY_FORMATS))
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            f"asset partition granularity must be one of: {choices}",
        )
    timezone = _bounded_text(raw_dimension.get("timezone") or "UTC", field="timezone")
    try:
        ZoneInfo(timezone)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            f"unknown asset partition timezone {timezone!r}",
        ) from exc
    source = _bounded_text(raw_dimension.get("source") or "dag_schedule", field="source")
    if source != "dag_schedule":
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            "asset partition v1 supports only source=dag_schedule",
        )
    key_format = _bounded_text(
        raw_dimension.get("key_format") or _GRANULARITY_KEY_FORMATS[granularity],
        field="key_format",
    )
    if "{{" in key_format or "{%" in key_format:
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            "asset partition key_format cannot contain template expressions",
        )
    unknown = sorted(set(raw_dimension) - {"type", "granularity", "timezone", "key_format", "source"})
    if unknown:
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            f"unknown asset partition fields: {', '.join(unknown)}",
        )
    return AssetPartitionSpec(
        dimension=dimension_name,
        granularity=granularity,
        timezone=timezone,
        key_format=key_format,
        source=source,
        type=partition_type,
    )


def partition_from_asset_item(raw: object) -> AssetPartitionSpec | None:
    if not isinstance(raw, Mapping):
        return None
    return parse_asset_partition(raw.get("partition"))


def normalize_asset_items(raw: object) -> object:
    """Normalize partition mappings while preserving legacy asset item shapes."""

    if not isinstance(raw, list):
        return raw
    normalized: list[object] = []
    for item in raw:
        partition = partition_from_asset_item(item)
        if partition is None or not isinstance(item, Mapping):
            normalized.append(item)
            continue
        payload = dict(item)
        payload["partition"] = partition.to_jsonable()
        normalized.append(payload)
    return normalized


def _bounded_text(raw: object, *, field: str) -> str:
    text = str(raw or "").strip()
    if not text or len(text) > _MAX_TEXT_LENGTH or "\x00" in text:
        raise AssetPartitionContractError(
            "asset_partition_invalid",
            f"asset partition {field} must be a non-empty string up to {_MAX_TEXT_LENGTH} characters",
        )
    return text


__all__ = [
    "AssetPartitionContractError",
    "AssetPartitionSpec",
    "parse_asset_partition",
    "normalize_asset_items",
    "partition_from_asset_item",
]
