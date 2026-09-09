"""Operator runbook generation from dpone ops artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class RunbookSection:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RunbookPackReport:
    runbook_id: str
    title: str
    passed: bool
    blockers: tuple[str, ...]
    sections: tuple[RunbookSection, ...]
    output_dir: str
    operator_runbook_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "runbook_id": self.runbook_id,
            "title": self.title,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "operator_runbook_path": self.operator_runbook_path,
            "sections": [section.to_dict() for section in self.sections],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops runbook pack",
            "",
            f"- Runbook ID: `{self.runbook_id}`",
            f"- Title: `{self.title}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| section | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for section in self.sections:
            status = "pass" if section.passed else "fail"
            lines.append(f"| `{section.name}` | {status} | `{section.sha256}` | {section.summary} | `{section.path}` |")
        if self.blockers:
            lines.extend(["", "Resolve runbook blockers before go-live or incident close:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class RunbookPackService:
    """Builds operator-facing runbooks from existing ops artifacts."""

    def build(
        self,
        *,
        output_dir: str | Path,
        runbook_id: str,
        title: str,
        artifacts: Mapping[str, str | Path],
    ) -> RunbookPackReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        payloads = {name: self._read_payload(Path(path)) for name, path in artifacts.items()}
        sections = tuple(self._section(name, Path(path), payloads[name]) for name, path in sorted(artifacts.items()))
        blockers = tuple(section.name for section in sections if not section.passed)
        operator_runbook_path = directory / "OPERATOR_RUNBOOK.md"
        operator_runbook_path.write_text(
            self._operator_runbook(
                runbook_id=runbook_id,
                title=title,
                sections=sections,
                payloads=payloads,
            ),
            encoding="utf-8",
        )
        report = RunbookPackReport(
            runbook_id=runbook_id,
            title=title,
            passed=not blockers,
            blockers=blockers,
            sections=sections,
            output_dir=str(directory),
            operator_runbook_path=str(operator_runbook_path),
        )
        (directory / "runbook_pack.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "runbook_pack.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _section(self, name: str, path: Path, payload: Mapping[str, Any]) -> RunbookSection:
        if not path.exists():
            return RunbookSection(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        return RunbookSection(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(name, payload),
            summary=self._summary(payload),
        )

    @staticmethod
    def _read_payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool:
        return artifact_payload_passed(payload, name=name)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        for key in ("blockers", "violations", "findings", "results", "items", "sections", "samples"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if bool(payload.get("passed", True)) else "failed"

    def _operator_runbook(
        self,
        *,
        runbook_id: str,
        title: str,
        sections: tuple[RunbookSection, ...],
        payloads: Mapping[str, Mapping[str, Any]],
    ) -> str:
        lines = [
            f"# {title}",
            "",
            f"Runbook ID: `{runbook_id}`",
            "",
            "## Current status",
            "",
            "| section | status | summary |",
            "|---|---|---|",
        ]
        for section in sections:
            status = "pass" if section.passed else "fail"
            lines.append(f"| `{section.name}` | {status} | {section.summary} |")
        lines.extend(self._go_live_checklist(payloads))
        lines.extend(self._rollback_checklist(payloads))
        lines.extend(self._incident_handoff_checklist(sections))
        return "\n".join(lines) + "\n"

    def _go_live_checklist(self, payloads: Mapping[str, Mapping[str, Any]]) -> list[str]:
        release_gate = payloads.get("release_gate", {})
        blockers = release_gate.get("blockers", [])
        lines = ["", "## Go-live checklist", ""]
        lines.append("- Confirm `dpone ops release-gate` is green.")
        lines.append("- Confirm `dpone ops security-audit` has no failed findings.")
        lines.append("- Confirm `dpone ops slo-evaluate` meets release objectives.")
        if isinstance(blockers, list) and blockers:
            lines.append(f"- Resolve release blockers: `{', '.join(str(item) for item in blockers)}`.")
        lines.append("- Attach release gate, connector badges, and artifact index to the release review.")
        return lines

    def _rollback_checklist(self, payloads: Mapping[str, Mapping[str, Any]]) -> list[str]:
        rollback_payload = payloads.get("rollback", {}) or payloads.get("rollback_execute", {})
        post_actions = rollback_payload.get("post_actions", [])
        lines = ["", "## Rollback checklist", ""]
        lines.append("- Generate `dpone ops rollback-plan` before applying rollback.")
        lines.append("- Preview rollback with `dpone ops rollback-execute` without `--yes`.")
        lines.append("- Apply rollback only with explicit `--yes` after external review.")
        if isinstance(post_actions, list):
            lines.extend(f"- {action}" for action in post_actions)
        lines.append("- Re-run data contracts and reconciliation before replaying state.")
        return lines

    @staticmethod
    def _incident_handoff_checklist(sections: tuple[RunbookSection, ...]) -> list[str]:
        lines = ["", "## Incident handoff checklist", ""]
        lines.append("- Attach `runbook_pack.json` and `OPERATOR_RUNBOOK.md` to the incident or PR.")
        lines.append("- Keep original artifacts immutable; regenerate new artifacts after fixes.")
        failed = [section.name for section in sections if not section.passed]
        if failed:
            lines.append(f"- Failed sections requiring owner follow-up: `{', '.join(failed)}`.")
        return lines
