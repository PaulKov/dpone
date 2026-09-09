"""Object-storage staging evidence and native load hints."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class StagingEvidenceReport:
    passed: bool
    sink: str
    target_table: str
    object_count: int
    total_size_bytes: int
    native_load_hint: str
    blockers: tuple[str, ...]
    json_path: str
    markdown_path: str
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        return "\n".join(
            [
                "# dpone object-storage staging evidence",
                "",
                f"- Passed: `{self.passed}`",
                f"- Sink: `{self.sink}`",
                f"- Target: `{self.target_table}`",
                f"- Objects: `{self.object_count}`",
                f"- Total size bytes: `{self.total_size_bytes}`",
                "",
                "## Native load hint",
                "",
                "```sql",
                self.native_load_hint,
                "```",
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Runbook",
                "",
                "1. Verify object count and checksums before starting target-native load.",
                "2. Keep staging manifests immutable until target load and reconciliation pass.",
                "3. Use cleanup only after load evidence and source state commit are recorded.",
                "",
            ]
        )


class StagingEvidenceService:
    """Validates object-storage staging manifests and renders native load hints."""

    def build(
        self,
        *,
        output_dir: str | Path,
        manifest_path: str | Path,
        sink: str,
        target_table: str,
    ) -> StagingEvidenceReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        manifest = _read_json(Path(manifest_path))
        blockers = list(_manifest_blockers(manifest))
        object_count = int(manifest.get("object_count") or len(manifest.get("objects", [])) if manifest else 0)
        total_size = int(manifest.get("total_size_bytes") or 0) if manifest else 0
        hint = _native_load_hint(sink=sink, target_table=target_table, manifest=manifest)
        json_path = directory / "staging_evidence.json"
        markdown_path = directory / "staging_evidence.md"
        report = StagingEvidenceReport(
            passed=not blockers,
            sink=sink,
            target_table=target_table,
            object_count=object_count,
            total_size_bytes=total_size,
            native_load_hint=hint,
            blockers=tuple(blockers),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
            output_dir=str(directory),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _manifest_blockers(payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if not payload:
        return ("manifest.invalid_or_missing",)
    objects = payload.get("objects", [])
    if not isinstance(objects, list) or not objects:
        blockers.append("manifest.objects.empty")
    for index, item in enumerate(objects):
        if not isinstance(item, Mapping):
            blockers.append(f"manifest.objects[{index}].invalid")
            continue
        if len(str(item.get("sha256") or "")) != 64:
            blockers.append(f"manifest.objects[{index}].sha256.invalid")
        if int(item.get("size_bytes") or 0) <= 0:
            blockers.append(f"manifest.objects[{index}].size_bytes.invalid")
    return tuple(blockers)


def _native_load_hint(*, sink: str, target_table: str, manifest: Mapping[str, Any]) -> str:
    first_uri = _first_uri(manifest)
    fmt = str(manifest.get("file_format") or "tsv").upper()
    normalized = sink.lower()
    if normalized == "clickhouse":
        return f"INSERT INTO {target_table} SELECT * FROM s3('{first_uri}') FORMAT {fmt};"
    if normalized == "bigquery":
        return f"bq load --source_format={fmt} {target_table} {first_uri}"
    if normalized == "mssql":
        return f"BULK INSERT {target_table} FROM '{first_uri}' WITH (FORMAT = 'CSV', FIRSTROW = 2);"
    if normalized == "postgres":
        return f"COPY {target_table} FROM PROGRAM 'cloud object fetch {first_uri}' WITH (FORMAT csv, HEADER true);"
    return f"Use target-native load for {target_table} from {first_uri}"


def _first_uri(manifest: Mapping[str, Any]) -> str:
    objects = manifest.get("objects", [])
    if isinstance(objects, list) and objects and isinstance(objects[0], Mapping):
        return str(objects[0].get("uri") or "")
    return str(manifest.get("base_uri") or "")
