"""Post-backfill verification bridge to the route-refresh toolchain.

A completed backfill campaign is exported as a route-refresh-execution
document, so the existing evidence tooling verifies backfills without any
bespoke machinery:

.. code-block:: bash

    dpone ops route-refresh-capture-snapshots \
        --execution .dpone/backfill/<run_key>.execution.json ...
    dpone ops route-refresh-verify ...

The document is written automatically by the backfill orchestrator once every
chunk is committed (see ``verification_execution_path`` in the run result).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.backfill.state import CHUNK_STATUS_SUCCESS, BackfillLedger


def verification_execution_payload(
    ledger: BackfillLedger,
    *,
    source: str,
    sink: str,
) -> dict[str, Any]:
    """Route-refresh-execution-compatible document for a committed campaign."""

    committed = [record for record in ledger.chunks if record.status == CHUNK_STATUS_SUCCESS]
    succeeded = len(committed) == len(ledger.chunks) and bool(ledger.chunks)
    return {
        "kind": "dpone.backfill_verification_execution",
        "schema_version": "1",
        "producer": "dpone backfill",
        "route": {"source": source, "sink": sink, "strategy": "backfill"},
        "dataset": ledger.dataset,
        "status": "succeeded" if succeeded else "failed",
        "passed": succeeded,
        "executed": True,
        "run_key": ledger.run_key,
        "inner_mode": ledger.inner_mode,
        "chunks": [
            {
                "ordinal": record.index,
                "start": record.start,
                "end": record.end,
                "partition": "",
                "source_boundary": f"{record.start}..{record.end}",
                "sink_boundary": f"{record.start}..{record.end}",
                "idempotency_key": record.idempotency_key,
            }
            for record in committed
        ],
        "blockers": [],
    }


def write_verification_execution(
    ledger: BackfillLedger,
    *,
    source: str,
    sink: str,
    output_path: str | Path,
) -> Path:
    """Write the verification execution document and return its path."""

    payload = verification_execution_payload(ledger, source=source, sink=sink)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


__all__ = ["verification_execution_payload", "write_verification_execution"]
