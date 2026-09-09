"""Validation and evidence helpers for MSSQL backfill publication."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Any

from dpone.runtime.support.mssql_object_name import MSSQLObjectName, quote_mssql_identifier

if TYPE_CHECKING:
    from dpone.backfill.state import BackfillLedger, BackfillPublicationRecord
    from dpone.runtime.sinks.mssql_backfill_publication_generation import MssqlPublicationGeneration

MssqlPublicationTarget = MSSQLObjectName


def count_rows(connector: Any, target: MssqlPublicationTarget) -> int:
    rows = connector.get_records(f"SELECT COUNT_BIG(*) AS row_count FROM {target.quoted()}", as_dict=True)
    if len(rows or ()) != 1:
        raise RuntimeError("mssql_backfill_publication.row_count_unavailable")
    return int(rows[0].get("row_count") or 0)


def has_duplicate_key(connector: Any, target: MssqlPublicationTarget, unique_key: Any) -> bool:
    keys = [unique_key] if isinstance(unique_key, str) else list(unique_key or ())
    if not keys:
        raise RuntimeError("mssql_backfill_publication.unique_key_required")
    columns = ", ".join(quote_mssql_identifier(str(key)) for key in keys)
    rows = connector.get_records(
        f"SELECT TOP (1) 1 AS duplicate_key FROM {target.quoted()} GROUP BY {columns} HAVING COUNT_BIG(*) > 1",
        as_dict=True,
    )
    return bool(rows)


def publication_receipt_id(
    ledger: BackfillLedger,
    live: MssqlPublicationTarget,
    rows: int,
) -> str:
    material = json.dumps(
        {
            "version": 1,
            "run_key": ledger.run_key,
            "plan_hash": ledger.plan_hash,
            "config_hash": ledger.config_hash,
            "target": live.dataset,
            "rows": rows,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "mssql-backfill-publication-v1:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def publication_evidence(record: BackfillPublicationRecord) -> dict[str, Any]:
    return {
        "mode": record.mode,
        "phase": record.phase,
        "validation": "passed" if record.phase == "published" else "pending",
        "expected_rows": record.expected_rows,
        "actual_rows": record.actual_rows,
        "duplicate_keys": record.duplicate_keys,
        "receipt_id": record.receipt_id,
        "backup_retained": record.phase == "published",
    }


def require_published_generation(
    publication: BackfillPublicationRecord,
    generation: MssqlPublicationGeneration,
) -> None:
    if (
        publication.shadow_object_id != generation.object_id
        or publication.shadow_create_token != generation.create_token
        or publication.receipt_id != generation.publication_receipt_id
    ):
        raise RuntimeError("mssql_backfill_publication.published_generation_changed")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "count_rows",
    "has_duplicate_key",
    "publication_evidence",
    "publication_receipt_id",
    "require_published_generation",
    "utc_now",
]
