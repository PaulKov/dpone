"""Small validation helpers for canonical range partition planning."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from dpone.runtime.partitioning_bounds import compare_partition_bounds


class RangeLike(Protocol):
    @property
    def upper_bound(self) -> Any: ...

    @property
    def lower_bound(self) -> Any: ...

    @property
    def include_upper(self) -> bool: ...

    @property
    def include_lower(self) -> bool: ...

    @property
    def boundary(self) -> Any: ...


def unpack_bounds(value: tuple[Any, ...]) -> tuple[Any, Any, int | None, int | None]:
    lower, upper, row_count, *rest = value
    null_count = rest[0] if rest else None
    return (
        lower,
        upper,
        int(row_count) if row_count is not None else None,
        int(null_count) if null_count is not None else None,
    )


def validate_explicit_coverage(partitions: Sequence[RangeLike], *, gap_policy: str) -> None:
    """Reject overlap and unintended gaps without reordering user ranges."""

    for previous, current in zip(partitions, partitions[1:], strict=False):
        comparison = compare_partition_bounds(previous.upper_bound, current.lower_bound, previous.boundary)
        overlapping_boundary = comparison == 0 and previous.include_upper and current.include_lower
        missing_boundary = comparison == 0 and not previous.include_upper and not current.include_lower
        if comparison > 0 or overlapping_boundary:
            raise ValueError("Explicit partition ranges overlap under source comparison semantics.")
        if (comparison < 0 or missing_boundary) and gap_policy != "allow_explicit":
            raise ValueError("Explicit partition ranges contain a gap; set gap_policy=allow_explicit to accept it.")


__all__ = ["unpack_bounds", "validate_explicit_coverage"]
