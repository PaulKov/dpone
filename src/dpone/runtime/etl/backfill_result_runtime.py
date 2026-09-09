"""Terminal result and operator-evidence helpers for backfill orchestration."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def requires_post_publication_normalization(ledger: Any) -> bool:
    """Resume any impossible legacy cancellation recorded after publication."""

    publication = ledger.publication
    return publication is not None and publication.phase == "published"


def campaign_completed(
    ledger: Any,
    chunks: tuple[Any, ...],
    errors: list[str],
) -> bool:
    """Return whether all immutable chunks committed without run errors."""

    return not errors and ledger.status != "cancel_requested" and len(ledger.committed_indexes()) == len(chunks)


def aggregate_backfill_result(
    *,
    ledger: Any,
    store: Any,
    chunks: tuple[Any, ...],
    retry_policy: str,
    selected: int,
    skipped: int,
    errors: list[str],
    duration_seconds: float,
    selection: Any = None,
    result_builder: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    """Build one terminal projection after all campaign hooks have settled."""

    return result_builder(
        ledger=ledger,
        store=store,
        chunks=chunks,
        retry_policy=retry_policy,
        selected=selected,
        skipped=skipped,
        errors=errors,
        duration_seconds=duration_seconds,
        selection=selection,
    )


def export_verification_execution(
    result: dict[str, Any],
    *,
    ledger: Any,
    store: Any,
    load_config: Any,
    verification_writer: Callable[..., Any],
) -> None:
    """Emit the route-refresh verification bridge for a complete campaign."""

    if result["status"] != "success" or len(ledger.committed_indexes()) != len(ledger.chunks):
        return
    options = load_config.options or {}
    path = verification_writer(
        ledger,
        source=str(options.get("source_type") or ""),
        sink=str(options.get("sink_type") or ""),
        output_path=store.root_dir / f"{ledger.run_key}.execution.json",
    )
    result["backfill"]["verification_execution_path"] = str(path)


__all__ = [
    "aggregate_backfill_result",
    "campaign_completed",
    "export_verification_execution",
    "requires_post_publication_normalization",
]
