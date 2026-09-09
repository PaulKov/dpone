"""CDC apply certification value objects."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcHandoffProfile, CdcStreamKey

SCHEMA_VERSION = "dpone.cdc_apply_certification.v1"

Row = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CdcApplyEvent:
    """Serializable CDC event used by credential-free apply certification."""

    operation: str
    position: str
    sequence: int
    key: Mapping[str, Any]
    before: Mapping[str, Any] | None = None
    after: Mapping[str, Any] | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcApplyEvent:
        return cls(
            operation=str(payload["operation"]).strip().lower(),
            position=str(payload["position"]),
            sequence=int(payload.get("sequence", 0)),
            key=_mapping(payload.get("key")),
            before=_optional_mapping(payload.get("before")),
            after=_optional_mapping(payload.get("after")),
        )

    @property
    def event_id(self) -> str:
        return _sha256_json(
            {
                "operation": self.operation,
                "position": self.position,
                "sequence": self.sequence,
                "key": dict(self.key),
                "before": dict(self.before or {}),
                "after": dict(self.after or {}),
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "position": self.position,
            "sequence": self.sequence,
            "key": dict(self.key),
            "before": dict(self.before or {}),
            "after": dict(self.after or {}),
            "event_id": self.event_id,
        }


@dataclass(frozen=True, slots=True)
class CdcApplyFixture:
    """Credential-free source event and expected sink-state fixture."""

    unique_key: tuple[str, ...]
    snapshot_boundary: str
    window_start: str
    window_end: str
    retention_min: str
    initial_rows: tuple[Row, ...]
    events: tuple[CdcApplyEvent, ...]
    expected_rows: tuple[Row, ...]
    schema_changes: tuple[Mapping[str, Any], ...] = tuple()

    @classmethod
    def from_path(cls, path: str | Path) -> CdcApplyFixture:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC apply fixture must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcApplyFixture:
        return cls(
            unique_key=tuple(str(item) for item in _sequence(payload.get("unique_key"))),
            snapshot_boundary=str(payload.get("snapshot_boundary", "")),
            window_start=str(payload.get("window_start", "")),
            window_end=str(payload.get("window_end", "")),
            retention_min=str(payload.get("retention_min", "")),
            initial_rows=tuple(_mapping(item) for item in _sequence(payload.get("initial_rows"))),
            events=tuple(CdcApplyEvent.from_dict(_mapping(item)) for item in _sequence(payload.get("events"))),
            expected_rows=tuple(_mapping(item) for item in _sequence(payload.get("expected_rows"))),
            schema_changes=tuple(_mapping(item) for item in _sequence(payload.get("schema_changes"))),
        )


@dataclass(frozen=True, slots=True)
class CdcApplyResult:
    """Result produced by a CDC apply strategy."""

    passed: bool
    actual_rows: tuple[dict[str, Any], ...]
    expected_rows: tuple[dict[str, Any], ...]
    metrics: Mapping[str, object]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    typed_hash_passed: bool
    delete_semantics_passed: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "actual_rows": list(self.actual_rows),
            "expected_rows": list(self.expected_rows),
            "metrics": dict(self.metrics),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "typed_hash_passed": self.typed_hash_passed,
            "delete_semantics_passed": self.delete_semantics_passed,
        }


@dataclass(frozen=True, slots=True)
class CdcApplyCertificationReport:
    """Stable JSON/Markdown contract for CDC apply certification."""

    stream: CdcStreamKey
    profile: CdcHandoffProfile | None
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    evidence_artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str
    handoff_json_path: str
    handoff_markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "route": self.stream.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "evidence_status": "UNVERIFIED",
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "evidence_artifacts": dict(self.evidence_artifacts),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "handoff_json_path": self.handoff_json_path,
            "handoff_markdown_path": self.handoff_markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone CDC apply certification",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            "- Evidence status: `UNVERIFIED`",
            f"- Handoff JSON: `{self.handoff_json_path}`",
            "",
            "## Evidence artifacts",
            "",
            "| evidence | path |",
            "|---|---|",
        ]
        lines.extend(f"| `{name}` | `{path}` |" for name, path in self.evidence_artifacts.items())
        lines.extend(["", "## Metrics", ""])
        lines.extend(f"- `{name}`: `{value}`" for name, value in self.metrics.items())
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def canonical_rows(rows: tuple[Row, ...], unique_key: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
    values = tuple(dict(row) for row in rows)
    return tuple(sorted(values, key=lambda row: tuple(str(row.get(key, "")) for key in unique_key)))


def rows_hash(rows: tuple[Row, ...], unique_key: tuple[str, ...]) -> str:
    return _sha256_json(list(canonical_rows(rows, unique_key)))


def row_key(row: Row, unique_key: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(name, "")) for name in unique_key)


def _sha256_json(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC apply fixture items must be JSON objects")


def _optional_mapping(value: object) -> Mapping[str, Any] | None:
    if value is None:
        return None
    return _mapping(value)


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    raise ValueError("CDC apply fixture field must be a JSON array")
