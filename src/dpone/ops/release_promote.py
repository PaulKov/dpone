"""Environment promotion evidence for dpone releases."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    production_artifact_payload_passed,
)
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ReleasePromotionItem:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleasePromotionReport:
    release: str
    from_environment: str
    to_environment: str
    promoted_at: str
    passed: bool
    blockers: tuple[str, ...]
    items: tuple[ReleasePromotionItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "from_environment": self.from_environment,
            "to_environment": self.to_environment,
            "promoted_at": self.promoted_at,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone release promotion manifest",
            "",
            f"- Release: `{self.release}`",
            f"- From: `{self.from_environment}`",
            f"- To: `{self.to_environment}`",
            f"- Promoted at: `{self.promoted_at}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| artifact | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |")
        if self.blockers:
            lines.extend(["", "Release promotion blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Verify every artifact checksum before environment promotion.",
                "2. Promote only when `passed=true` and `blockers=[]`.",
                "3. Attach `promotion_manifest.json` to the release record.",
                "4. Keep rollback and runbook artifacts immutable for post-release audit.",
                "",
            ]
        )
        return "\n".join(lines)


class ReleasePromotionService:
    """Builds immutable evidence for release promotion between environments."""

    def promote(
        self,
        *,
        output_dir: str | Path,
        release: str,
        from_environment: str,
        to_environment: str,
        artifacts: Mapping[str, str | Path],
    ) -> ReleasePromotionReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = tuple(self._item(name=name, path=Path(path)) for name, path in sorted(artifacts.items()))
        blockers = tuple(item.name for item in items if not item.passed)
        report = ReleasePromotionReport(
            release=release,
            from_environment=from_environment,
            to_environment=to_environment,
            promoted_at=datetime.now(UTC).isoformat(),
            passed=not blockers,
            blockers=blockers,
            items=items,
            output_dir=str(directory),
        )
        (directory / "promotion_manifest.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "promotion_manifest.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, *, name: str, path: Path) -> ReleasePromotionItem:
        if not path.exists():
            return ReleasePromotionItem(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._payload(path)
        return ReleasePromotionItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(name, payload),
            summary=self._summary(payload),
        )

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if path.suffix.lower() != ".json":
            return {"passed": False, "summary": "unsupported non-json artifact"}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"passed": False, "summary": "invalid json artifact"}
        return payload if isinstance(payload, Mapping) else {"passed": False, "summary": "invalid json artifact"}

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool:
        return production_artifact_payload_passed(payload, name=name)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in ("blockers", "violations", "findings", "results", "items", "steps", "sections"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if bool(payload.get("passed", True)) else "failed"
