"""Append-only SQL journal I/O for backfill campaign state."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from dpone.backfill.sql_state_journal_writers import BackfillJournalWriter, JournalRow

_LedgerT = TypeVar("_LedgerT")
_RecordT = TypeVar("_RecordT")
JOURNAL_ORDER_ERROR = "DPONE_BACKFILL_JOURNAL_ORDER_INVALID"


class BackfillJournalOrderError(ValueError):
    """Raised when connector rows do not prove one causal latest revision."""


def campaign_journal_row(ledger: Any) -> JournalRow:
    """Serialize one campaign snapshot independently from SQL dialect I/O."""

    return (
        ledger.run_key,
        ledger.dataset,
        ledger.inner_mode,
        ledger.status,
        ledger.plan_hash or "",
        ledger.config_hash or "",
        _json(ledger.chunk_config),
        _json(ledger.campaign_jsonable()),
    )


def chunk_journal_row(ledger: Any, chunk: Any) -> JournalRow:
    """Serialize one chunk transition independently from SQL dialect I/O."""

    return (
        ledger.run_key,
        chunk.index,
        chunk.status,
        chunk.start,
        chunk.end,
        chunk.idempotency_key,
        chunk.run_id or "",
        chunk.load_id or "",
        chunk.error or "",
        _json(chunk.to_jsonable()),
    )


def record_campaign(
    connector: Any,
    insert_sql: str,
    ledger: Any,
    *,
    writer: BackfillJournalWriter,
) -> None:
    """Append one campaign snapshot."""

    writer.append_row(connector, insert_sql, campaign_journal_row(ledger))


def record_chunk(
    connector: Any,
    insert_sql: str,
    ledger: Any,
    chunk: Any,
    *,
    writer: BackfillJournalWriter,
) -> None:
    """Append one chunk transition."""

    writer.append_row(connector, insert_sql, chunk_journal_row(ledger, chunk))


def record_snapshot(
    connector: Any,
    campaign_insert_sql: str,
    chunk_insert_sql: str,
    ledger: Any,
    chunks: Iterable[Any],
    *,
    writer: BackfillJournalWriter,
) -> None:
    """Append one campaign snapshot and the selected chunk transitions."""

    record_campaign(connector, campaign_insert_sql, ledger, writer=writer)
    for chunk in chunks:
        record_chunk(connector, chunk_insert_sql, ledger, chunk, writer=writer)


def load_campaign(
    connector: Any,
    select_sql: str,
    deserialize: Callable[[dict[str, Any]], _LedgerT],
) -> _LedgerT | None:
    """Read the latest campaign snapshot when the connector supports records."""

    get_records = getattr(connector, "get_records", None)
    if not callable(get_records):
        return None
    details = latest_campaign_details(get_records(select_sql, as_dict=True))
    if details is None:
        return None
    payload = details if isinstance(details, dict) else json.loads(str(details))
    return deserialize(payload)


def load_chunks(
    connector: Any,
    select_sql: str,
    deserialize: Callable[[dict[str, Any]], _RecordT],
    *,
    params: tuple[Any, ...] | None = None,
) -> tuple[_RecordT, ...]:
    """Read the latest transition for every chunk."""

    get_records = getattr(connector, "get_records", None)
    if not callable(get_records):
        return ()
    rows = (
        get_records(select_sql, params=params, as_dict=True)
        if params is not None
        else get_records(
            select_sql,
            as_dict=True,
        )
    )
    return latest_chunk_records(rows, deserialize)


def replace_chunk(ledger: Any, record: Any) -> None:
    """Replace one logical chunk transition without changing campaign shape."""

    for position, existing in enumerate(ledger.chunks):
        if existing.index == record.index:
            ledger.chunks[position] = record
            return
    ledger.chunks.append(record)


def latest_chunk_records(
    rows: Any,
    deserialize: Callable[[dict[str, Any]], _RecordT],
) -> tuple[_RecordT, ...]:
    """Decode the newest transition for each ordered chunk index."""

    latest: dict[int, _RecordT] = {}
    prior_revision: dict[int, int] = {}
    for row in rows or []:
        index, revision, details = _chunk_row(row)
        if details is None:
            raise BackfillJournalOrderError(f"{JOURNAL_ORDER_ERROR}: chunk details_json is required")
        _validate_descending_revision(
            revision,
            previous=prior_revision.get(index),
            key=f"chunk:{index}",
        )
        prior_revision[index] = revision
        if index in latest:
            continue
        payload = details if isinstance(details, dict) else json.loads(str(details))
        if not isinstance(payload, dict) or "index" not in payload:
            continue
        record = deserialize(payload)
        if getattr(record, "index", None) != index:
            raise ValueError("backfill SQL chunk index does not match details_json")
        latest[index] = record
    return tuple(latest[index] for index in sorted(latest))


def latest_campaign_details(rows: Any) -> Any | None:
    """Validate descending causal revisions and return the latest campaign."""

    latest: Any | None = None
    previous: int | None = None
    for row in rows or []:
        revision, details = _campaign_row(row)
        if details is None:
            raise BackfillJournalOrderError(f"{JOURNAL_ORDER_ERROR}: campaign details_json is required")
        _validate_descending_revision(revision, previous=previous, key="campaign")
        previous = revision
        if latest is None:
            latest = details
    return latest


def _campaign_row(row: Any) -> tuple[int, Any | None]:
    if isinstance(row, dict):
        return _exact_revision(row.get("journal_id")), row.get("details_json")
    if isinstance(row, (list, tuple)) and len(row) >= 2:
        return _exact_revision(row[0]), row[1]
    return _exact_revision(getattr(row, "journal_id", None)), getattr(row, "details_json", None)


def _chunk_row(row: Any) -> tuple[int, int, Any | None]:
    if isinstance(row, dict):
        index = row.get("chunk_index")
        return (
            _exact_chunk_index(index),
            _exact_revision(row.get("journal_id")),
            row.get("details_json"),
        )
    if isinstance(row, (list, tuple)) and len(row) >= 3:
        return _exact_chunk_index(row[0]), _exact_revision(row[1]), row[2]
    index = getattr(row, "chunk_index", None)
    return (
        _exact_chunk_index(index),
        _exact_revision(getattr(row, "journal_id", None)),
        getattr(row, "details_json", None),
    )


def _exact_revision(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise BackfillJournalOrderError(f"{JOURNAL_ORDER_ERROR}: journal_id must be a positive integer")
    return value


def _exact_chunk_index(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise BackfillJournalOrderError(f"{JOURNAL_ORDER_ERROR}: chunk_index must be a positive integer")
    return value


def _validate_descending_revision(revision: int, *, previous: int | None, key: str) -> None:
    if previous is not None and revision >= previous:
        raise BackfillJournalOrderError(
            f"{JOURNAL_ORDER_ERROR}: {key} revisions must be unique and strictly descending"
        )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = [
    "BackfillJournalOrderError",
    "JOURNAL_ORDER_ERROR",
    "campaign_journal_row",
    "chunk_journal_row",
    "latest_campaign_details",
    "load_campaign",
    "load_chunks",
    "record_campaign",
    "record_chunk",
    "record_snapshot",
    "replace_chunk",
]
