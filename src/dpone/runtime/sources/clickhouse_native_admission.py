"""Closed local-engine admission for the native source read policy."""

from collections.abc import Mapping, Sequence
from typing import Any

_RAW_ENGINES = frozenset({"MergeTree", "ReplicatedMergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree"})


def admit_native_relation(tables: Sequence[Mapping[str, Any]], source_read_mode: str | None) -> None:
    """Require one Atomic relation under either strict legacy or explicit raw semantics.

    Raw mode admits only the four local engines whose returned row multiplicity
    the caller preserves with query-level ``final=0``. Admission conveys no
    cross-replica freshness or persistent source-snapshot guarantee.
    """
    if source_read_mode not in (None, "raw_single_query"):
        raise ValueError("mssql_native.source_read_invalid")
    engines = {"MergeTree"} if source_read_mode is None else _RAW_ENGINES
    if len(tables) == 1 and tables[0]["engine"] in engines and tables[0]["database_engine"] == "Atomic":
        return
    diagnostic = "plain_mergetree_atomic_required" if source_read_mode is None else "source_engine_unsupported"
    raise ValueError(f"mssql_native.{diagnostic}")
