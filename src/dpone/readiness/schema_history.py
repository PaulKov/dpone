"""Local schema history registry for schema evolution evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SchemaHistoryRecord:
    table: str
    version: int
    run_id: str
    schema: tuple[tuple[str, str], ...]
    diff: dict[str, list[str]]
    artifact_path: str

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["schema"] = [list(item) for item in self.schema]
        return data


class SchemaHistoryRegistry:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def record(self, *, table: str, schema: list[tuple[str, str]], run_id: str) -> SchemaHistoryRecord:
        table_dir = self.root / _safe_name(table)
        table_dir.mkdir(parents=True, exist_ok=True)
        previous = self.current(table)
        version = int(previous.get("version", 0)) + 1 if previous else 1
        normalized_schema = tuple((str(column), str(dtype)) for column, dtype in schema)
        diff = _diff(previous.get("schema", []) if previous else [], normalized_schema)
        artifact = table_dir / f"v{version:04d}.json"
        record = SchemaHistoryRecord(
            table=table,
            version=version,
            run_id=run_id,
            schema=normalized_schema,
            diff=diff,
            artifact_path=str(artifact),
        )
        artifact.write_text(
            json.dumps(record.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (table_dir / "current.json").write_text(artifact.read_text(encoding="utf-8"), encoding="utf-8")
        return record

    def current(self, table: str) -> dict[str, object]:
        path = self.root / _safe_name(table) / "current.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))


def _diff(previous: object, current: tuple[tuple[str, str], ...]) -> dict[str, list[str]]:
    previous_names = {str(item[0]) for item in previous if isinstance(item, list | tuple) and item}
    current_names = {column for column, _ in current}
    return {
        "added": sorted(current_names - previous_names),
        "removed": sorted(previous_names - current_names),
        "unchanged": sorted(current_names & previous_names),
    }


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value).strip("_") or "table"
