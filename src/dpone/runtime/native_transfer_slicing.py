from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy


@dataclass(frozen=True, slots=True)
class TransferPartition:
    index: int
    lower_bound: Any
    upper_bound: Any
    include_upper: bool = False
    estimated_rows: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "lower_bound": _json_safe_bound(self.lower_bound),
            "upper_bound": _json_safe_bound(self.upper_bound),
            "include_upper": self.include_upper,
            "estimated_rows": self.estimated_rows,
        }


@dataclass(frozen=True, slots=True)
class TransferSlice:
    partition_index: int
    slice_index: int
    lower_bound: Any
    upper_bound: Any
    include_upper: bool = False
    estimated_rows: int | None = None
    is_null_partition: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_index": self.partition_index,
            "slice_index": self.slice_index,
            "lower_bound": _json_safe_bound(self.lower_bound),
            "upper_bound": _json_safe_bound(self.upper_bound),
            "include_upper": self.include_upper,
            "estimated_rows": self.estimated_rows,
            "is_null_partition": self.is_null_partition,
        }


class AdaptiveSliceSizer:
    def __init__(self, resource: NativeTransferResourcePolicy) -> None:
        self._resource = resource

    def initial_rows(self, *, estimated_bytes_per_row: int | float | None) -> int:
        row_bytes = max(1, int(estimated_bytes_per_row or 1))
        rows = max(1, int(self._resource.target_file_bytes / row_bytes))
        return self._clamp(rows)

    def next_rows(self, *, previous_rows: int, observed_bytes: int) -> int:
        if previous_rows <= 0 or observed_bytes <= 0:
            return self._clamp(previous_rows)
        ratio = self._resource.target_file_bytes / observed_bytes
        return self._clamp(int(previous_rows * ratio))

    def _clamp(self, value: int) -> int:
        return max(self._resource.min_slice_rows, min(self._resource.max_slice_rows, int(value)))


class RangeSlicePlanner:
    def __init__(self, resource: NativeTransferResourcePolicy) -> None:
        self._resource = resource

    def plan(
        self, partition: TransferPartition, *, estimated_bytes_per_row: int | float | None
    ) -> tuple[TransferSlice, ...]:
        sizer = AdaptiveSliceSizer(self._resource)
        rows_per_slice = sizer.initial_rows(estimated_bytes_per_row=estimated_bytes_per_row)
        span = max(0, int(partition.upper_bound) - int(partition.lower_bound))
        if span == 0:
            return (
                TransferSlice(
                    partition_index=partition.index,
                    slice_index=0,
                    lower_bound=partition.lower_bound,
                    upper_bound=partition.upper_bound,
                    include_upper=partition.include_upper,
                    estimated_rows=partition.estimated_rows,
                ),
            )
        slice_count = max(1, math.ceil(span / rows_per_slice))
        slices: list[TransferSlice] = []
        lower = int(partition.lower_bound)
        for index in range(slice_count):
            upper = (
                int(partition.upper_bound)
                if index == slice_count - 1
                else min(int(partition.upper_bound), lower + rows_per_slice)
            )
            slices.append(
                TransferSlice(
                    partition_index=partition.index,
                    slice_index=index,
                    lower_bound=lower,
                    upper_bound=upper,
                    include_upper=partition.include_upper and index == slice_count - 1,
                    estimated_rows=max(0, upper - lower),
                )
            )
            lower = upper
        return tuple(slices)

    def split(self, item: TransferSlice) -> tuple[TransferSlice, ...]:
        if item.is_null_partition:
            return ()
        try:
            lower = int(item.lower_bound)
            upper = int(item.upper_bound)
        except (TypeError, ValueError):
            return ()
        if upper - lower <= 1:
            return ()
        midpoint = lower + max(1, int((upper - lower) / 2))
        return (
            TransferSlice(item.partition_index, 0, lower, midpoint, False, midpoint - lower),
            TransferSlice(item.partition_index, 1, midpoint, upper, item.include_upper, upper - midpoint),
        )


def _json_safe_bound(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray):
        return f"0x{bytes(value).hex().upper()}"
    return value


__all__ = ["AdaptiveSliceSizer", "RangeSlicePlanner", "TransferPartition", "TransferSlice"]
