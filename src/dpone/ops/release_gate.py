"""Top-level release readiness gate for dpone ops artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    artifact_payload_passed,
    production_artifact_payload_passed,
)
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ReleaseGateItem:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseGateReport:
    release: str
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    items: tuple[ReleaseGateItem, ...]
    artifact_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "artifact_dir": self.artifact_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops release gate",
            "",
            f"- Release: `{self.release}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            f"- Warnings: `{len(self.warnings)}`",
            "",
            "| artifact | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |")
        if self.blockers:
            lines.extend(["", "Fix release blockers before publishing:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class ReleaseGateService:
    """Aggregates release readiness artifacts into one gate decision."""

    def evaluate(
        self,
        *,
        artifact_dir: str | Path,
        release: str,
        artifacts: Mapping[str, str | Path],
    ) -> ReleaseGateReport:
        directory = Path(artifact_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = tuple(self._item(name, Path(path)) for name, path in sorted(artifacts.items()))
        blockers = tuple(item.name for item in items if not item.passed)
        report = ReleaseGateReport(
            release=release,
            passed=not blockers,
            blockers=blockers,
            warnings=tuple(),
            items=items,
            artifact_dir=str(directory),
        )
        (directory / "release_gate.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "release_gate.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, name: str, path: Path) -> ReleaseGateItem:
        if not path.exists():
            return ReleaseGateItem(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._read_payload(path)
        return ReleaseGateItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=production_artifact_payload_passed(payload, name=name),
            summary=self._summary(payload),
        )

    @staticmethod
    def _read_payload(path: Path) -> Mapping[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"passed": False, "summary": "invalid json artifact"}
        return payload if isinstance(payload, Mapping) else {"passed": False, "summary": "non-object json artifact"}

    @staticmethod
    def _passed(payload: Mapping[str, Any]) -> bool:
        return artifact_payload_passed(payload)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in ("blockers", "violations", "findings", "results", "items", "strategy_rows", "samples"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if artifact_payload_passed(payload) else "failed"
