"""Selection helpers for integration matrix manual filters."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from dpone.integration_matrix_counts import _matches_filter, _split_filter


def matrix_case_selected(case: Any, *, env: Mapping[str, str] | None = None) -> bool:
    """Return whether a matrix case matches manual CI filters."""
    source = env or os.environ
    filters = {
        "source": _split_filter(source.get("DPONE_MATRIX_SOURCE")),
        "sink": _split_filter(source.get("DPONE_MATRIX_SINK")),
        "strategy": _split_filter(source.get("DPONE_MATRIX_STRATEGY")),
        "case_id": _split_filter(source.get("DPONE_MATRIX_CASE_ID")),
    }
    return (
        _matches_filter(case.source, filters["source"])
        and _matches_filter(case.sink, filters["sink"])
        and _matches_filter(case.strategy, filters["strategy"])
        and _matches_filter(case.case_id, filters["case_id"])
    )


__all__ = ["matrix_case_selected"]
