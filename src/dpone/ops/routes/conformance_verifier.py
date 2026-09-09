"""Exact source/sink verification for Route Conformance Lab."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence

from dpone.ops.routes.conformance_models import (
    RouteConformanceChunk,
    RouteConformanceSnapshot,
    RouteConformanceVerificationResult,
)


class RouteConformanceVerifier:
    """Compare source and sink snapshots with typed hash semantics."""

    def verify(
        self,
        *,
        source_snapshot: RouteConformanceSnapshot,
        sink_snapshot: RouteConformanceSnapshot,
        chunk_size: int | None = None,
    ) -> RouteConformanceVerificationResult:
        size = chunk_size or max(1, source_snapshot.row_count)
        contract_mismatches = _physical_contract_mismatches(source_snapshot, sink_snapshot)
        mismatch_samples = _mismatch_samples(source_snapshot, sink_snapshot)
        chunks = _chunks(source_snapshot.rows, sink_snapshot.rows, size)
        source_hash = _hash_rows(source_snapshot.rows)
        sink_hash = _hash_rows(sink_snapshot.rows)
        blockers = _blockers(
            source_snapshot=source_snapshot,
            sink_snapshot=sink_snapshot,
            source_hash=source_hash,
            sink_hash=sink_hash,
            contract_mismatches=contract_mismatches,
        )
        return RouteConformanceVerificationResult(
            passed=not blockers,
            source_rows=source_snapshot.row_count,
            sink_rows=sink_snapshot.row_count,
            chunk_count=len(chunks),
            source_hash=source_hash,
            sink_hash=sink_hash,
            chunks=chunks,
            physical_contract_mismatches=contract_mismatches,
            mismatch_samples=mismatch_samples,
            blockers=blockers,
            warnings=tuple(),
        )


def _physical_contract_mismatches(
    source_snapshot: RouteConformanceSnapshot,
    sink_snapshot: RouteConformanceSnapshot,
) -> tuple[Mapping[str, object], ...]:
    sink_contracts = {column.name: column.physical_contract for column in sink_snapshot.columns}
    mismatches: list[Mapping[str, object]] = []
    for column in source_snapshot.columns:
        sink_contract = sink_contracts.get(column.name)
        if sink_contract != column.physical_contract:
            mismatches.append(
                {
                    "column": column.name,
                    "source_contract": column.physical_contract,
                    "sink_contract": sink_contract or "<missing>",
                }
            )
    return tuple(mismatches)


def _mismatch_samples(
    source_snapshot: RouteConformanceSnapshot,
    sink_snapshot: RouteConformanceSnapshot,
    *,
    limit: int = 10,
) -> tuple[Mapping[str, object], ...]:
    samples: list[Mapping[str, object]] = []
    columns = [column.name for column in source_snapshot.columns]
    for index, source_row in enumerate(source_snapshot.rows):
        if index >= sink_snapshot.row_count:
            samples.append({"row_index": index, "column": "<row>", "source": dict(source_row), "sink": "<missing>"})
            break
        sink_row = sink_snapshot.rows[index]
        for column in columns:
            source_value = source_row.get(column)
            sink_value = sink_row.get(column)
            if source_value != sink_value:
                samples.append(
                    {
                        "row_index": index,
                        "column": column,
                        "source": source_value,
                        "sink": sink_value,
                    }
                )
                if len(samples) >= limit:
                    return tuple(samples)
    if sink_snapshot.row_count > source_snapshot.row_count:
        samples.append(
            {
                "row_index": source_snapshot.row_count,
                "column": "<row>",
                "source": "<missing>",
                "sink": dict(sink_snapshot.rows[source_snapshot.row_count]),
            }
        )
    return tuple(samples)


def _chunks(
    source_rows: Sequence[Mapping[str, object]],
    sink_rows: Sequence[Mapping[str, object]],
    chunk_size: int,
) -> tuple[RouteConformanceChunk, ...]:
    count = max(len(source_rows), len(sink_rows))
    chunks: list[RouteConformanceChunk] = []
    for start in range(0, count, chunk_size):
        end = min(start + chunk_size, count)
        source_hash = _hash_rows(source_rows[start:end])
        sink_hash = _hash_rows(sink_rows[start:end])
        chunks.append(
            RouteConformanceChunk(
                index=len(chunks),
                start=start,
                end=end,
                source_hash=source_hash,
                sink_hash=sink_hash,
                passed=source_hash == sink_hash,
            )
        )
    return tuple(chunks)


def _blockers(
    *,
    source_snapshot: RouteConformanceSnapshot,
    sink_snapshot: RouteConformanceSnapshot,
    source_hash: str,
    sink_hash: str,
    contract_mismatches: Sequence[Mapping[str, object]],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if source_snapshot.row_count != sink_snapshot.row_count:
        blockers.append("row_count.mismatch")
    if source_hash != sink_hash:
        blockers.append("typed_hash.mismatch")
    blockers.extend(f"physical_contract.{item['column']}.mismatch" for item in contract_mismatches)
    return tuple(dict.fromkeys(blockers))


def _hash_rows(rows: Sequence[Mapping[str, object]]) -> str:
    data = json.dumps([dict(row) for row in rows], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


__all__ = ["RouteConformanceVerifier"]
