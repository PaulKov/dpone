"""Public contracts for route refresh verification evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal
from uuid import UUID

from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_refresh_verification.v1"
SNAPSHOT_SCHEMA_VERSION = "dpone.route_refresh_snapshot.v1"

RouteRefreshVerificationStatus = Literal["verified", "failed", "blocked"]


@dataclass(frozen=True, slots=True)
class RouteRefreshSideSnapshot:
    """Read-only source or sink snapshot for one executed refresh chunk."""

    row_count: int
    min_boundary: str
    max_boundary: str
    typed_hash: str
    duplicate_keys: int = 0
    null_keys: int = 0
    sample_rows: int = 0
    summary: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshVerificationRequest:
    """One chunk verification request passed to source and sink readers."""

    route: RouteKey
    dataset: str
    ordinal: int
    start: str
    end: str
    partition: str
    source_boundary: str
    sink_boundary: str
    idempotency_key: str
    runner_id: str
    execution_path: str
    chunk_artifact_path: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    boundary_column: str
    type_hints: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class RouteRefreshChunkVerification:
    """Verification result for one executed refresh chunk."""

    ordinal: int
    idempotency_key: str
    status: str
    passed: bool
    source: RouteRefreshSideSnapshot
    sink: RouteRefreshSideSnapshot
    summary: str
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "idempotency_key": self.idempotency_key,
            "status": self.status,
            "passed": self.passed,
            "source": self.source.to_dict(),
            "sink": self.sink.to_dict(),
            "summary": self.summary,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class RouteRefreshVerificationArtifact:
    """Artifact referenced by a route refresh verification receipt."""

    name: str
    path: str
    required: bool
    exists: bool
    sha256: str
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshVerificationReport:
    """Stable JSON/Markdown verification receipt for release gates."""

    route: RouteKey
    profile: RouteProfile | None
    dataset: str
    runner_id: str
    route_refresh_execution_json: str
    execution_sha256: str
    status: RouteRefreshVerificationStatus
    passed: bool
    ready_for_state_promotion: bool
    summary: dict[str, int]
    chunks: tuple[RouteRefreshChunkVerification, ...]
    artifacts: tuple[RouteRefreshVerificationArtifact, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "dataset": self.dataset,
            "runner_id": self.runner_id,
            "route_refresh_execution_json": self.route_refresh_execution_json,
            "execution_sha256": self.execution_sha256,
            "status": self.status,
            "passed": self.passed,
            "ready_for_state_promotion": self.ready_for_state_promotion,
            "summary": self.summary,
            "chunks": [item.to_dict() for item in self.chunks],
            "artifacts": [item.to_dict() for item in self.artifacts],
            "artifact_index": {item.name: item.to_dict() for item in self.artifacts},
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route refresh verification",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Runner: `{self.runner_id}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Ready for state promotion: `{self.ready_for_state_promotion}`",
            "",
            "## Summary",
            "",
        ]
        lines.extend(f"- {key}: `{value}`" for key, value in self.summary.items())
        lines.extend(
            [
                "",
                "## Chunks",
                "",
                "| # | status | source rows | sink rows | duplicate keys | null keys | summary |",
                "|---:|---|---:|---:|---:|---:|---|",
            ]
        )
        for chunk in self.chunks:
            lines.append(
                f"| `{chunk.ordinal}` | {chunk.status} | `{chunk.source.row_count}` | `{chunk.sink.row_count}` | "
                f"`{chunk.sink.duplicate_keys}` | `{chunk.sink.null_keys}` | {chunk.summary} |"
            )
        lines.extend(
            ["", "## Artifacts", "", "| artifact | required | exists | sha256 | path |", "|---|---:|---:|---|---|"]
        )
        for artifact in self.artifacts:
            lines.append(
                f"| `{artifact.name}` | `{artifact.required}` | `{artifact.exists}` | "
                f"`{artifact.sha256}` | `{artifact.path}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.append("- Attach `route_refresh_verification.json` before route state promotion.")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def typed_hash(
    rows: Sequence[Mapping[str, object]],
    *,
    columns: Sequence[str],
    key_columns: Sequence[str] = (),
    type_hints: Mapping[str, str] | None = None,
) -> str:
    """Return a stable hash for rows using declared route column types."""

    hints = {str(key): str(value).lower() for key, value in (type_hints or {}).items()}
    ordered_rows = sorted(rows, key=lambda row: _sort_key(row, key_columns or columns, hints))
    digest = hashlib.sha256()
    for row in ordered_rows:
        payload = {column: _canonical_value(row.get(column), hints.get(column, "string")) for column in columns}
        digest.update(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _sort_key(row: Mapping[str, object], columns: Sequence[str], hints: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(json.dumps(_canonical_value(row.get(column), hints.get(column, "string"))) for column in columns)


def _canonical_value(value: object, type_hint: str) -> dict[str, object]:
    if value is None:
        return {"type": "null", "value": None}
    if _is_int(type_hint):
        return {"type": "int", "value": _int_text(value)}
    if _is_decimal(type_hint):
        decimal_value = _decimal(value)
        scale = _decimal_scale(type_hint)
        if scale is not None:
            decimal_value = decimal_value.quantize(Decimal(1).scaleb(-scale))
        return {"type": "decimal", "value": format(decimal_value.normalize(), "f")}
    if _is_float(type_hint):
        return {"type": "float", "value": _float_text(value)}
    if _is_bool(type_hint):
        return {"type": "bool", "value": str(value).strip().lower() in {"1", "true", "yes", "y"}}
    if _is_binary(type_hint):
        return {"type": "binary", "value": _binary_text(value)}
    if _is_time(type_hint):
        return {"type": "time", "value": _time_text(value)}
    if _is_uuid(type_hint):
        return {"type": "uuid", "value": str(UUID(str(value))).lower()}
    if _is_date(type_hint):
        return {"type": "date", "value": _date_text(value)}
    if _is_datetime(type_hint):
        return {"type": "datetime", "value": _datetime_text(value)}
    return {"type": "string", "value": str(value)}


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value for typed hash: {value}") from exc


def _int_text(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return str(int(value))
    return str(int(str(value)))


def _float_text(value: object) -> str:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return format(float(value), ".17g")
    return format(float(str(value)), ".17g")


def _binary_text(value: object) -> str:
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    text = str(value).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    return re.sub(r"[^0-9a-f]", "", text)


def _time_text(value: object) -> str:
    if isinstance(value, time):
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        if value.microsecond:
            return f"{seconds}.{value.microsecond:06d}".rstrip("0").rstrip(".")
        return str(seconds)
    text = str(value).strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return text.rstrip("0").rstrip(".") if "." in text else text
    match = re.match(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?", text)
    if match:
        hours, minutes, seconds, fraction = match.groups()
        total = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        if fraction:
            return f"{total}.{fraction}".rstrip("0").rstrip(".")
        return str(total)
    return text


def _date_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()[:10]


def _datetime_text(value: object) -> str:
    if isinstance(value, datetime):
        current = value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
        return _trim_datetime_fraction(current.isoformat(sep=" "))
    text = str(value).strip().replace("T", " ")
    text = re.sub(r"\s*\+00:?00$", "", text)
    text = re.sub(r"\s*Z$", "", text)
    return _trim_datetime_fraction(text)


def _trim_datetime_fraction(value: str) -> str:
    if "." not in value:
        return value
    head, fraction = value.split(".", 1)
    fraction = re.sub(r"[^0-9].*$", "", fraction)
    trimmed = fraction.rstrip("0")
    return f"{head}.{trimmed}" if trimmed else head


def _decimal_scale(type_hint: str) -> int | None:
    match = re.search(r"(?:decimal|numeric)\s*\(\s*\d+\s*,\s*(\d+)\s*\)", type_hint)
    return int(match.group(1)) if match else None


def _is_int(type_hint: str) -> bool:
    return type_hint in {"int", "integer", "bigint", "smallint", "tinyint", "int32", "int64"}


def _is_decimal(type_hint: str) -> bool:
    return type_hint.startswith(("decimal", "numeric"))


def _is_float(type_hint: str) -> bool:
    return type_hint in {"float", "float32", "float64", "double", "real"}


def _is_bool(type_hint: str) -> bool:
    return type_hint in {"bool", "boolean", "bit"}


def _is_binary(type_hint: str) -> bool:
    normalized = type_hint.strip().lower()
    return (
        normalized == "timestamp"
        or "rowversion" in normalized
        or "binary" in normalized
        or normalized.startswith("image")
    )


def _is_time(type_hint: str) -> bool:
    normalized = type_hint.strip().lower()
    return normalized.startswith("time") and "timestamp" not in normalized


def _is_uuid(type_hint: str) -> bool:
    normalized = type_hint.strip().lower()
    return normalized in {"uuid", "uniqueidentifier"}


def _is_date(type_hint: str) -> bool:
    return type_hint.strip().lower() == "date"


def _is_datetime(type_hint: str) -> bool:
    normalized = type_hint.strip().lower()
    return any(token in normalized for token in ("datetime", "timestamp", "timestamptz"))


__all__ = [
    "RouteRefreshChunkVerification",
    "RouteRefreshSideSnapshot",
    "RouteRefreshVerificationArtifact",
    "RouteRefreshVerificationReport",
    "RouteRefreshVerificationRequest",
    "RouteRefreshVerificationStatus",
    "SCHEMA_VERSION",
    "SNAPSHOT_SCHEMA_VERSION",
    "typed_hash",
]
