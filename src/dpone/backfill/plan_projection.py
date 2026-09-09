"""Read-only payload projection for self-service backfill planning."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dpone.backfill.models import (
    chunk_spec_from_options,
    inner_mode_from_options,
    parallel_workers_from_options,
)
from dpone.backfill.planner import backfill_run_key, plan_chunks
from dpone.backfill.service_advisor import build_backfill_advisor_payload
from dpone.backfill.service_helpers import (
    advisor_profile,
    backfill_status_projection,
    state_backend,
)

if TYPE_CHECKING:
    from dpone.backfill.state import BackfillLedger
    from dpone.config.load_config import LoadConfig


class BackfillPlanStateReader(Protocol):
    """Minimal state-store surface required to project one campaign plan."""

    def load(self, run_key: str) -> BackfillLedger | None: ...

    def path_for(self, run_key: str) -> Path: ...

    def state_capabilities(self) -> dict[str, Any]: ...


def build_backfill_plan_payload(
    load_config: LoadConfig,
    backfill_options: Mapping[str, Any],
    *,
    manifest: str,
    executed: bool,
    advisor: bool,
    state_store_provider: Callable[[], BackfillPlanStateReader],
    advisor_evidence_paths: tuple[str | Path, ...] = (),
) -> dict[str, Any]:
    """Project deterministic chunks and persisted status without moving data."""

    spec = chunk_spec_from_options(backfill_options)
    if spec is None:
        raise ValueError(
            "No backfill.chunk window configured: define sink.strategy.backfill.chunk in the manifest "
            "or pass --column/--from/--to/--step overrides"
        )
    inner_mode = inner_mode_from_options(backfill_options)
    workers = parallel_workers_from_options(backfill_options)
    dataset = f"{load_config.target_schema}.{load_config.target_table}"
    run_key = str(backfill_options.get("backfill_id") or "") or backfill_run_key(
        dataset=dataset,
        spec=spec,
        inner_mode=inner_mode,
    )
    chunks = plan_chunks(spec, run_key=run_key)
    store = state_store_provider()
    ledger = store.load(run_key)
    persisted_chunks = {record.index: record for record in (ledger.chunks if ledger else [])}
    chunk_rows = []
    for chunk in chunks:
        record = persisted_chunks.get(chunk.index)
        row = chunk.to_jsonable()
        row.update(record.to_jsonable() if record else {"status": "pending"})
        chunk_rows.append(row)
    counts: dict[str, int] = {}
    for row in chunk_rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    payload = {
        "kind": "dpone.backfill_plan",
        "manifest": manifest,
        "dataset": dataset,
        "run_key": run_key,
        "inner_mode": inner_mode,
        "parallel_workers": workers,
        "chunk_config": {
            "column": spec.column,
            "from": spec.start,
            "to": spec.end,
            "step": spec.step,
            "kind": spec.kind,
        },
        "state_backend": state_backend(backfill_options),
        "state_capabilities": store.state_capabilities(),
        "state_path": str(store.path_for(run_key)),
        "chunks_total": len(chunks),
        "counts": counts,
        **backfill_status_projection(ledger),
        "chunks": chunk_rows,
        "executed": executed,
    }
    if advisor:
        payload["advisor"] = build_backfill_advisor_payload(
            chunks_total=len(chunks),
            chunk_rows=chunk_rows,
            profile=advisor_profile(backfill_options),
            current_step=spec.step,
            current_parallel_workers=workers,
            lease_ttl_minutes=int(backfill_options.get("lease_ttl_minutes") or 60),
            evidence_paths=advisor_evidence_paths,
        )
    return payload


__all__ = ["BackfillPlanStateReader", "build_backfill_plan_payload"]
