"""Pre-release checklist gate for minor and major releases."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.ids import utc_now_iso

REQUIRED_MINOR_MAJOR_CHECKS = (
    "cli_help_surface",
    "cli_output_contracts",
    "run_cli_manifest",
    "run_python_api_manifest",
    "nested_hierarchical_identity",
    "nested_parent_child_integrity",
    "source_sink_strategy_matrix",
    "source_sink_artifacts",
    "docker_live_routes",
    "contracts_guardrails",
    "documentation_yaml_examples",
    "documentation_links",
    "documentation_mkdocs",
    "ci_cd_quality",
    "package",
)

REQUIRED_PATCH_CHECKS = (
    "documentation_mkdocs",
    "ci_cd_quality",
    "package",
)


@dataclass(frozen=True, slots=True)
class PreReleaseCheckDecision:
    name: str
    required: bool
    passed: bool
    blocker: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PreReleaseChecklistReport:
    release: str
    release_type: str
    passed: bool
    generated_at: str
    blockers: tuple[str, ...]
    checks: tuple[PreReleaseCheckDecision, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "release_type": self.release_type,
            "passed": self.passed,
            "generated_at": self.generated_at,
            "blockers": list(self.blockers),
            "checks": [item.to_dict() for item in self.checks],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        lines = [
            "# dpone pre-release checklist",
            "",
            f"- Release: `{self.release}`",
            f"- Release type: `{self.release_type}`",
            f"- Passed: `{self.passed}`",
            f"- Generated at: `{self.generated_at}`",
            "",
            "| check | required | status |",
            "|---|---:|---|",
        ]
        for item in self.checks:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | `{item.required}` | {status} |")
        lines.extend(
            [
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Runbook",
                "",
                "1. Run the missing or failing gate and regenerate this checklist.",
                "2. Do not tag or publish a minor/major release while this report is red.",
                "3. Attach this checklist to `release-evidence-pack` as `pre_release_checklist` when publishing.",
                "",
            ]
        )
        return "\n".join(lines)


class PreReleaseChecklistService:
    """Builds the explicit pre-release checklist required before publishing."""

    def build(
        self,
        *,
        output_dir: str | Path,
        release: str,
        release_type: str,
        checks: Mapping[str, object],
    ) -> PreReleaseChecklistReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        required = _required_checks(release_type)
        names = tuple(dict.fromkeys((*required, *sorted(checks.keys()))))
        decisions = tuple(_decision(name, checks.get(name), required=name in required) for name in names)
        blockers = tuple(item.blocker for item in decisions if item.blocker)
        json_path = directory / "pre_release_checklist.json"
        markdown_path = directory / "pre_release_checklist.md"
        report = PreReleaseChecklistReport(
            release=release,
            release_type=release_type,
            passed=not blockers,
            generated_at=utc_now_iso(),
            blockers=blockers,
            checks=decisions,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _required_checks(release_type: str) -> tuple[str, ...]:
    if release_type in {"minor", "major"}:
        return REQUIRED_MINOR_MAJOR_CHECKS
    return REQUIRED_PATCH_CHECKS


def _decision(name: str, value: object, *, required: bool) -> PreReleaseCheckDecision:
    if value is None:
        passed = not required
        blocker = f"{name}.missing" if required else None
    else:
        passed = _bool_value(value)
        blocker = None if passed else f"{name}.not_passed"
    return PreReleaseCheckDecision(name=name, required=required, passed=passed, blocker=blocker)


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "passed", "pass"}
    return bool(value)


__all__ = [
    "PreReleaseChecklistReport",
    "PreReleaseChecklistService",
    "REQUIRED_MINOR_MAJOR_CHECKS",
    "REQUIRED_PATCH_CHECKS",
]
