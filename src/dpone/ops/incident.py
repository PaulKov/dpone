"""Incident and release review bundles for dpone ops."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class IncidentPackItem:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IncidentPackReport:
    incident_id: str
    title: str
    severity: str
    passed: bool
    items: tuple[IncidentPackItem, ...]
    artifact_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "incident_id": self.incident_id,
            "title": self.title,
            "severity": self.severity,
            "passed": self.passed,
            "artifact_dir": self.artifact_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops incident pack",
            "",
            f"- Incident ID: `{self.incident_id}`",
            f"- Title: `{self.title}`",
            f"- Severity: `{self.severity}`",
            f"- Passed: `{self.passed}`",
            "",
            "| artifact | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |")
        if not self.passed:
            lines.extend(["", "Fix failed incident pack items before close/go-live."])
        return "\n".join(lines) + "\n"


class OpsIncidentPackService:
    """Builds one auditable bundle from existing ops artifacts."""

    def build(
        self,
        *,
        artifact_dir: str | Path,
        incident_id: str,
        title: str,
        artifacts: Mapping[str, str | Path],
        severity: str = "review",
    ) -> IncidentPackReport:
        directory = Path(artifact_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = tuple(self._item(name, Path(path)) for name, path in sorted(artifacts.items()))
        report = IncidentPackReport(
            incident_id=incident_id,
            title=title,
            severity=severity,
            passed=all(item.passed for item in items),
            items=items,
            artifact_dir=str(directory),
        )
        (directory / "ops_incident_pack.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "ops_incident_pack.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, name: str, path: Path) -> IncidentPackItem:
        if not path.exists():
            return IncidentPackItem(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._read_payload(path)
        return IncidentPackItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(name, payload),
            summary=self._summary(payload),
        )

    @staticmethod
    def _read_payload(path: Path) -> Mapping[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"passed": True, "summary": "non-json artifact"}
        return payload if isinstance(payload, Mapping) else {"passed": True, "summary": "json artifact"}

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool:
        return artifact_payload_passed(payload, name=name)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in ("violations", "blockers", "warnings", "findings", "results", "samples"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "summary" in payload:
            return str(payload["summary"])
        return "passed" if bool(payload.get("passed", True)) else "failed"
