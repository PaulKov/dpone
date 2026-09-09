"""Bounded-memory child-quality validation for spilled nested packages."""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dpone.runtime.normalization.child_quality import (
    ChildQualityResult,
    ChildQualityService,
    _ChildQualityObservation,
)
from dpone.runtime.normalization.external_record_sort import ExternalRecordSpool
from dpone.runtime.normalization.options import NestedChildQualityOptions

_ROW_ID = "__dpone__row_id"
_PARENT_ROW_ID = "__dpone__parent_row_id"
_ROOT_ROW_ID = "__dpone__root_row_id"
_UTC = timezone(timedelta(0))


@dataclass(slots=True)
class _TableCounters:
    row_count: int = 0
    duplicate_child_keys: int | None = None
    duplicate_row_ids: int | None = None
    missing_row_ids: int | None = None
    has_unique_key: bool = False
    hierarchy_applicable: bool = False


class _QualitySpools:
    """Sequential facts used by external sort/merge quality checks."""

    def __init__(self, directory: Path, *, chunk_bytes: int) -> None:
        def spool(prefix: str) -> ExternalRecordSpool:
            return ExternalRecordSpool(directory, prefix=prefix, chunk_bytes=chunk_bytes)

        self.row_ids = spool(".dpone-quality-row-id-")
        self.business_keys = spool(".dpone-quality-business-key-")
        self.root_ids = spool(".dpone-quality-root-")
        self.owners_exact = spool(".dpone-quality-owner-exact-")
        self.owners_any = spool(".dpone-quality-owner-any-")
        self.root_needs = spool(".dpone-quality-root-need-")
        self.owner_exact_needs = spool(".dpone-quality-owner-exact-need-")
        self.owner_any_needs = spool(".dpone-quality-owner-any-need-")
        self.failures = spool(".dpone-quality-orphan-")

    def cleanup(self) -> None:
        for spool in vars(self).values():
            spool.cleanup()


class SpilledChildQualityService:
    """Validate spill files through sequential spools and external merge joins."""

    def __init__(
        self,
        *,
        policy_service: ChildQualityService | None = None,
        sort_chunk_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self._policy_service = policy_service or ChildQualityService()
        self._sort_chunk_bytes = sort_chunk_bytes

    def evaluate_package(
        self,
        *,
        root_table: str,
        table_rows: Mapping[str, Iterable[Mapping[str, object]]],
        table_keys: Mapping[str, Sequence[str]],
        options: NestedChildQualityOptions,
        work_dir: Path,
        table_parents: Mapping[str, str] | None = None,
        ignored_hierarchy_tables: Sequence[str] = (),
    ) -> tuple[ChildQualityResult, ...]:
        """Scan once, then evaluate exact relations with bounded memory."""

        work_dir.mkdir(parents=True, exist_ok=True)
        spools = _QualitySpools(work_dir, chunk_bytes=self._sort_chunk_bytes)
        try:
            root_rows, observations = self._scan(
                spools=spools,
                root_table=root_table,
                table_rows=table_rows,
                table_keys=table_keys,
                table_parents=table_parents or {},
                ignored_hierarchy_tables=set(ignored_hierarchy_tables),
                options=options,
            )
            return self._policy_service._evaluate_observations(
                root_table=root_table,
                root_rows=root_rows,
                observations=observations,
                options=options,
            )
        finally:
            spools.cleanup()

    def _scan(
        self,
        *,
        spools: _QualitySpools,
        root_table: str,
        table_rows: Mapping[str, Iterable[Mapping[str, object]]],
        table_keys: Mapping[str, Sequence[str]],
        table_parents: Mapping[str, str],
        ignored_hierarchy_tables: set[str],
        options: NestedChildQualityOptions,
    ) -> tuple[int, tuple[_ChildQualityObservation, ...]]:
        counters: dict[str, _TableCounters] = {}
        track_orphans = options.orphan_child_rows != "skip"
        for table_name, rows in table_rows.items():
            unique_key = tuple(table_keys.get(table_name, ()))
            is_child = table_name != root_table
            counter = _TableCounters(
                duplicate_child_keys=(0 if unique_key and options.duplicate_child_key != "skip" else None),
                duplicate_row_ids=0 if is_child and not unique_key else None,
                missing_row_ids=0 if is_child else None,
                has_unique_key=bool(unique_key),
                hierarchy_applicable=is_child and table_name not in ignored_hierarchy_tables,
            )
            for row in rows:
                self._scan_row(
                    spools=spools,
                    root_table=root_table,
                    table_name=table_name,
                    row=row,
                    unique_key=unique_key,
                    counter=counter,
                    expected_parent_table=table_parents.get(table_name),
                    track_orphans=track_orphans,
                )
            counters[table_name] = counter

        for table_name, count in _duplicate_counts(spools.row_ids.sorted_records()).items():
            counters[table_name].duplicate_row_ids = count
        for table_name, count in _duplicate_counts(spools.business_keys.sorted_records()).items():
            counters[table_name].duplicate_child_keys = count
        if track_orphans:
            _emit_missing(
                needs=spools.root_needs.sorted_records(),
                available=spools.root_ids.sorted_records(),
                failures=spools.failures,
            )
            _emit_missing(
                needs=spools.owner_exact_needs.sorted_records(),
                available=spools.owners_exact.sorted_records(),
                failures=spools.failures,
            )
            _emit_missing(
                needs=spools.owner_any_needs.sorted_records(),
                available=spools.owners_any.sorted_records(),
                failures=spools.failures,
            )
            orphan_counts = _unique_failure_counts(spools.failures.sorted_records())
        else:
            orphan_counts = Counter()

        root_rows = counters.get(root_table, _TableCounters()).row_count
        observations = tuple(
            _ChildQualityObservation(
                child_table=table_name,
                child_rows=counter.row_count,
                duplicate_child_keys=counter.duplicate_child_keys,
                orphan_child_rows=(
                    orphan_counts[table_name] if counter.hierarchy_applicable and track_orphans else None
                ),
                duplicate_row_ids=counter.duplicate_row_ids,
                missing_row_ids=counter.missing_row_ids,
                has_unique_key=counter.has_unique_key,
                hierarchy_applicable=counter.hierarchy_applicable,
            )
            for table_name, counter in counters.items()
            if table_name != root_table
        )
        return root_rows, observations

    def _scan_row(
        self,
        *,
        spools: _QualitySpools,
        root_table: str,
        table_name: str,
        row: Mapping[str, object],
        unique_key: Sequence[str],
        counter: _TableCounters,
        expected_parent_table: str | None,
        track_orphans: bool,
    ) -> None:
        counter.row_count += 1
        ordinal = str(counter.row_count)
        row_id = _text(row.get(_ROW_ID))
        root_id = _text(row.get(_ROOT_ROW_ID))
        if row_id is None:
            if counter.missing_row_ids is not None:
                counter.missing_row_ids += 1
        else:
            if counter.duplicate_row_ids is not None:
                spools.row_ids.append((table_name, row_id))
            if track_orphans and root_id is not None:
                spools.owners_exact.append((_lookup_key((table_name, row_id, root_id)),))
                spools.owners_any.append((_lookup_key((row_id, root_id)),))
            if track_orphans and table_name == root_table:
                spools.root_ids.append((_lookup_key((row_id,)),))
        if counter.hierarchy_applicable and track_orphans:
            parent_id = _text(row.get(_PARENT_ROW_ID))
            if parent_id is None or root_id is None:
                spools.failures.append((table_name, ordinal))
            else:
                spools.root_needs.append((_lookup_key((root_id,)), table_name, ordinal))
                if expected_parent_table is None:
                    spools.owner_any_needs.append((_lookup_key((parent_id, root_id)), table_name, ordinal))
                else:
                    spools.owner_exact_needs.append(
                        (_lookup_key((expected_parent_table, parent_id, root_id)), table_name, ordinal)
                    )
        if counter.duplicate_child_keys is not None:
            encoded_key = _encode_business_key(tuple(row.get(column) for column in unique_key))
            spools.business_keys.append((table_name, encoded_key))


def _duplicate_counts(records: Iterator[tuple[str, ...]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    previous: tuple[str, ...] | None = None
    for record in records:
        if record == previous:
            counts[record[0]] += 1
        previous = record
    return counts


def _emit_missing(
    *,
    needs: Iterator[tuple[str, ...]],
    available: Iterator[tuple[str, ...]],
    failures: ExternalRecordSpool,
) -> None:
    try:
        current = next(available, None)
        for lookup, table_name, ordinal in needs:
            while current is not None and current[0] < lookup:
                current = next(available, None)
            if current is None or current[0] != lookup:
                failures.append((table_name, ordinal))
    finally:
        for records in (needs, available):
            close = getattr(records, "close", None)
            if callable(close):
                close()


def _unique_failure_counts(records: Iterator[tuple[str, ...]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    previous: tuple[str, ...] | None = None
    for record in records:
        if record != previous:
            counts[record[0]] += 1
        previous = record
    return counts


def _lookup_key(values: tuple[str, ...]) -> str:
    payload = json.dumps(values, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return payload.hex()


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _encode_business_key(values: tuple[object, ...]) -> str:
    hash(values)
    return json.dumps(
        [_encode_scalar(value) for value in values],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _encode_scalar(value: object) -> tuple[str, object]:
    if value is None:
        return ("null", "")
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, bytes):
        return ("bytes", value.hex())
    if isinstance(value, bool | int):
        return ("number", (int(value), 1))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite spilled child unique-key values are not supported")
        numerator, denominator = value.as_integer_ratio()
        return ("number", (numerator, denominator))
    if isinstance(value, datetime):
        normalized = value.astimezone(_UTC) if value.tzinfo is not None and value.utcoffset() is not None else value
        return ("timestamp", normalized.isoformat())
    if isinstance(value, date):
        return ("date", value.isoformat())
    raise TypeError(
        "unsupported spilled child unique-key value "
        f"{type(value).__name__}; expected null, string, bytes, date, datetime, bool, int, or finite float"
    )


__all__ = ["SpilledChildQualityService"]
