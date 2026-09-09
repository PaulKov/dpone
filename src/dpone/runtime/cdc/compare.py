"""Generic CDC compare and repair-plan service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol

from dpone.runtime.cdc.compare_models import (
    CdcCompareDiff,
    CdcCompareRepairReport,
    CdcCompareRow,
    CdcRepairAction,
    CdcRepairPlan,
    canonical_json,
    sha256,
)
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


class CdcCompareReader(Protocol):
    """Read current-state rows for a CDC compare side."""

    def read_rows(self) -> tuple[CdcCompareRow, ...]: ...


class CdcRowHasher:
    """Stable canonical hash helper for current-state row payloads."""

    def payload_hash(self, payload: Mapping[str, object]) -> str:
        return sha256(canonical_json(dict(payload)))


class InMemoryCdcCompareReader:
    """Credential-free compare reader for tests, fixtures, and local CLI mode."""

    def __init__(self, rows: Sequence[CdcCompareRow]) -> None:
        self._rows = tuple(rows)

    def read_rows(self) -> tuple[CdcCompareRow, ...]:
        return self._rows


class CdcCompareRepairService:
    """Compare source and target current state and write a repair plan."""

    def compare(
        self,
        *,
        output_dir: str | Path,
        stream: CdcRuntimeStream,
        source_reader: CdcCompareReader,
        target_reader: CdcCompareReader,
        max_diffs: int = 1000,
    ) -> CdcCompareRepairReport:
        directory = Path(output_dir)
        json_path = directory / "cdc_compare_repair.json"
        markdown_path = directory / "cdc_compare_repair.md"
        repair_plan_path = directory / "cdc_repair_plan.json"

        source_rows = source_reader.read_rows()
        target_rows = target_reader.read_rows()
        source_by_key = {row.key_json: row for row in source_rows}
        target_by_key = {row.key_json: row for row in target_rows}
        all_diffs, matched_rows = _compare_rows(source_by_key=source_by_key, target_by_key=target_by_key)
        diffs = tuple(all_diffs[: max(0, max_diffs)])
        warnings = ("cdc_compare.max_diffs_reached",) if len(all_diffs) > len(diffs) else tuple()
        blockers = ("cdc_compare.differences_found",) if all_diffs else tuple()
        repair_plan = CdcRepairPlan(
            stream=stream,
            actions=tuple(_repair_action(stream=stream, diff=diff, index=index) for index, diff in enumerate(diffs, 1)),
        ).write(repair_plan_path)
        report = CdcCompareRepairReport(
            stream=stream,
            rows_source=len(source_rows),
            rows_target=len(target_rows),
            matched_rows=matched_rows,
            diff_count=len(diffs),
            passed=not all_diffs,
            blockers=blockers,
            warnings=warnings,
            metrics={
                "total_diff_count": len(all_diffs),
                "max_diffs": max_diffs,
                "repair_plan_path": str(repair_plan_path),
            },
            diffs=diffs,
            repair_plan=repair_plan,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        report.write()
        return report


def _compare_rows(
    *,
    source_by_key: Mapping[str, CdcCompareRow],
    target_by_key: Mapping[str, CdcCompareRow],
) -> tuple[list[CdcCompareDiff], int]:
    diffs: list[CdcCompareDiff] = []
    matched_rows = 0
    for key_json, source in source_by_key.items():
        target = target_by_key.get(key_json)
        if target is None:
            diffs.append(
                CdcCompareDiff(
                    kind="missing_in_target",
                    key=source.key,
                    source_payload=source.payload,
                    target_payload=None,
                    source_hash=source.payload_hash,
                    target_hash=None,
                    reason="source key is absent from target",
                )
            )
            continue
        if source.deleted != target.deleted:
            diffs.append(
                CdcCompareDiff(
                    kind="delete_mismatch",
                    key=source.key,
                    source_payload=source.payload,
                    target_payload=target.payload,
                    source_hash=source.payload_hash,
                    target_hash=target.payload_hash,
                    reason="source and target delete flags differ",
                )
            )
            continue
        if source.payload_hash != target.payload_hash:
            diffs.append(
                CdcCompareDiff(
                    kind="value_mismatch",
                    key=source.key,
                    source_payload=source.payload,
                    target_payload=target.payload,
                    source_hash=source.payload_hash,
                    target_hash=target.payload_hash,
                    reason="source and target hashes differ",
                )
            )
            continue
        matched_rows += 1
    for key_json, target in target_by_key.items():
        if key_json not in source_by_key:
            diffs.append(
                CdcCompareDiff(
                    kind="extra_in_target",
                    key=target.key,
                    source_payload=None,
                    target_payload=target.payload,
                    source_hash=None,
                    target_hash=target.payload_hash,
                    reason="target key is absent from source",
                )
            )
    return diffs, matched_rows


def _repair_action(*, stream: CdcRuntimeStream, diff: CdcCompareDiff, index: int) -> CdcRepairAction:
    operation = _repair_operation(diff)
    payload = diff.source_payload if operation in {"insert", "update"} else diff.key
    return CdcRepairAction(
        action_id=f"repair:{stream.stream_id}:{index}",
        kind=diff.kind,
        operation=operation,
        key=diff.key,
        payload=payload or diff.key,
        target_payload=diff.target_payload,
        reason=diff.reason,
    )


def _repair_operation(diff: CdcCompareDiff) -> str:
    if diff.kind == "missing_in_target":
        return "insert"
    if diff.kind == "extra_in_target":
        return "delete"
    if diff.source_payload is None:
        return "delete"
    return "delete" if diff.kind == "delete_mismatch" and diff.source_payload == diff.key else "update"


__all__ = [
    "CdcCompareReader",
    "CdcCompareRepairService",
    "CdcRowHasher",
    "InMemoryCdcCompareReader",
]
