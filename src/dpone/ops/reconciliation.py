"""Reconciliation 2.0 reports with repair action planning."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class ReconciliationRepairAction:
    action: str
    key: dict[str, object]
    reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    passed: bool
    generated_at: str
    source_count: int
    target_count: int
    missing_count: int
    extra_count: int
    mismatch_count: int
    delete_count: int
    source_checksum: str
    target_checksum: str
    repair_actions: tuple[ReconciliationRepairAction, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "generated_at": self.generated_at,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "missing_count": self.missing_count,
            "extra_count": self.extra_count,
            "mismatch_count": self.mismatch_count,
            "delete_count": self.delete_count,
            "source_checksum": self.source_checksum,
            "target_checksum": self.target_checksum,
            "repair_actions": [item.to_dict() for item in self.repair_actions],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone reconciliation report",
            "",
            f"- Passed: `{self.passed}`",
            f"- Source rows: `{self.source_count}`",
            f"- Target rows: `{self.target_count}`",
            f"- Missing in target: `{self.missing_count}`",
            f"- Extra in target: `{self.extra_count}`",
            f"- Mismatches: `{self.mismatch_count}`",
            f"- Physical deletes: `{self.delete_count}`",
            f"- Source checksum: `{self.source_checksum}`",
            f"- Target checksum: `{self.target_checksum}`",
            "",
            "| action | key | reason |",
            "|---|---|---|",
        ]
        if self.repair_actions:
            for action in self.repair_actions:
                lines.append(f"| `{action.action}` | `{json.dumps(action.key, sort_keys=True)}` | {action.reason} |")
        else:
            lines.append("| `none` | `{}` | source and target are reconciled |")
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. For `insert_target_row`, re-run the same load package or replay from source state.",
                "2. For `update_target_row`, prefer staging-first `incremental_merge` repair.",
                "3. For `delete_target_row`, verify physical-delete policy before applying hard deletes.",
                "4. Never advance source state after a failed reconciliation gate.",
                "",
            ]
        )
        return "\n".join(lines)


class ReconciliationService:
    """Computes bounded source-target reconciliation and repair actions."""

    def reconcile(
        self,
        *,
        output_dir: str | Path,
        source_rows: Sequence[Mapping[str, Any]],
        target_rows: Sequence[Mapping[str, Any]],
        key_columns: Sequence[str],
        compare_columns: Sequence[str] | None = None,
        delete_column: str | None = None,
    ) -> ReconciliationReport:
        keys = tuple(key_columns)
        if not keys:
            raise ValueError("At least one key column is required for reconciliation.")
        source_index = _index(source_rows, keys)
        target_index = _index(target_rows, keys)
        deleted_keys = {
            key
            for key, row in source_index.items()
            if delete_column and row.get(delete_column) not in {None, "", False}
        }
        effective_source_keys = set(source_index) - deleted_keys
        target_keys = set(target_index)
        columns = _compare_columns(source_index, target_index, keys, compare_columns, delete_column)
        missing = tuple(sorted(effective_source_keys - target_keys, key=_sort_key))
        extra = tuple(sorted((target_keys - effective_source_keys) | (deleted_keys & target_keys), key=_sort_key))
        mismatches = tuple(
            key
            for key in sorted(effective_source_keys & target_keys, key=_sort_key)
            if any(source_index[key].get(column) != target_index[key].get(column) for column in columns)
        )
        actions = (
            *(
                ReconciliationRepairAction("insert_target_row", _key_dict(keys, key), "source row is missing in target")
                for key in missing
            ),
            *(
                ReconciliationRepairAction(
                    "delete_target_row",
                    _key_dict(keys, key),
                    "target row is extra or source row is physically deleted",
                )
                for key in extra
            ),
            *(
                ReconciliationRepairAction(
                    "update_target_row", _key_dict(keys, key), "target values differ from source"
                )
                for key in mismatches
            ),
        )
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        json_path = directory / "reconciliation_report.json"
        markdown_path = directory / "reconciliation_report.md"
        source_checksum = _checksum(source_index, effective_source_keys, columns)
        target_checksum = _checksum(target_index, target_keys - deleted_keys, columns)
        report = ReconciliationReport(
            passed=not actions and source_checksum == target_checksum,
            generated_at=utc_now_iso(),
            source_count=len(source_rows),
            target_count=len(target_rows),
            missing_count=len(missing),
            extra_count=len(extra),
            mismatch_count=len(mismatches),
            delete_count=len(deleted_keys),
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            repair_actions=tuple(actions),
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _index(rows: Sequence[Mapping[str, Any]], key_columns: tuple[str, ...]) -> dict[tuple[object, ...], dict[str, Any]]:
    return {tuple(row.get(column) for column in key_columns): dict(row) for row in rows}


def _compare_columns(
    source_index: Mapping[tuple[object, ...], Mapping[str, Any]],
    target_index: Mapping[tuple[object, ...], Mapping[str, Any]],
    key_columns: tuple[str, ...],
    compare_columns: Sequence[str] | None,
    delete_column: str | None,
) -> tuple[str, ...]:
    if compare_columns is not None:
        return tuple(compare_columns)
    ignored = {*key_columns, *(["__dpone__deleted_at", delete_column] if delete_column else [])}
    return tuple(
        sorted(
            {
                column
                for row in (*source_index.values(), *target_index.values())
                for column in row
                if column not in ignored
            }
        )
    )


def _checksum(
    rows: Mapping[tuple[object, ...], Mapping[str, Any]],
    keys: set[tuple[object, ...]],
    columns: tuple[str, ...],
) -> str:
    digest = hashlib.sha256()
    for key in sorted(keys, key=_sort_key):
        payload = {"key": key, "values": {column: rows[key].get(column) for column in columns}}
        digest.update(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"))
    return digest.hexdigest()


def _key_dict(key_columns: tuple[str, ...], key: tuple[object, ...]) -> dict[str, object]:
    return dict(zip(key_columns, key, strict=False))


def _sort_key(key: tuple[object, ...]) -> tuple[str, ...]:
    return tuple(str(item) for item in key)
