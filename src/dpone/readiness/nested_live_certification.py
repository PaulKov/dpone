"""Live certification evidence harness for nested normalization."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

NESTED_LIVE_REQUIRED_CHECKS = (
    "root_child_load",
    "physical_child_deletes",
    "state_commit_after_success",
    "state_not_advanced_after_failure",
    "native_fast_path",
    "child_quality",
)


@dataclass(frozen=True, slots=True)
class NestedLiveEvidenceArtifact:
    json_path: Path
    markdown_path: Path


@dataclass(frozen=True, slots=True)
class NestedLiveCertificationCase:
    sink_type: str
    check: Callable[[], dict[str, Any]]
    available: Callable[[], bool] = lambda: True


class NestedLiveCertificationRunner:
    """Run per-sink live certification probes and write evidence artifacts."""

    def __init__(self, cases: Sequence[NestedLiveCertificationCase]) -> None:
        self.cases = tuple(cases)

    def run(self, *, output_dir: str | Path) -> NestedLiveEvidenceArtifact:
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        sinks: dict[str, dict[str, Any]] = {}
        for case in self.cases:
            sinks[case.sink_type] = self._run_case(case)
        payload = {
            "status": _overall_status(sinks),
            "sinks": sinks,
            "required_checks": list(NESTED_LIVE_REQUIRED_CHECKS),
        }
        return _write_artifact(output, "nested_live_certification", payload)

    def _run_case(self, case: NestedLiveCertificationCase) -> dict[str, Any]:
        if not case.available():
            return {"status": "skipped", "reason": "local service or credentials unavailable"}
        try:
            details = case.check()
        except Exception as exc:  # pragma: no cover - exercised by live failures
            return {"status": "failed", "error": str(exc)}
        if not isinstance(details, Mapping):
            return {
                "status": "unverified",
                "reason": "live data-path evidence must be an object",
            }
        checks = details.get("checks")
        if not isinstance(checks, Mapping):
            return {
                "status": "unverified",
                "reason": "required live data-path checks were not reported",
                "details": details,
            }
        observed = {check: checks.get(check, "missing") for check in NESTED_LIVE_REQUIRED_CHECKS}
        if any(status == "failed" for status in observed.values()):
            return {"status": "failed", "checks": observed, "details": details}
        if not all(status == "passed" for status in observed.values()):
            return {
                "status": "unverified",
                "reason": "one or more required live data-path checks are missing or not passed",
                "checks": observed,
                "details": details,
            }
        return {"status": "passed", "checks": observed, "details": details}


def _overall_status(sinks: Mapping[str, Mapping[str, Any]]) -> str:
    statuses = [item.get("status") for item in sinks.values()]
    if any(status == "failed" for status in statuses):
        return "failed"
    if statuses and all(status == "passed" for status in statuses):
        return "passed"
    return "unverified"


def _write_artifact(output_dir: Path, stem: str, payload: dict[str, Any]) -> NestedLiveEvidenceArtifact:
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(stem, payload), encoding="utf-8")
    return NestedLiveEvidenceArtifact(json_path=json_path, markdown_path=markdown_path)


def _markdown(title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title.replace('_', ' ').title()}", "", f"Status: **{payload.get('status')}**", ""]
    sinks = payload.get("sinks", {})
    if isinstance(sinks, dict):
        lines.extend(["| Sink | Status |", "|---|---|"])
        lines.extend(f"| `{sink}` | {data.get('status')} |" for sink, data in sinks.items() if isinstance(data, dict))
        lines.append("")
    lines.append("```json")
    lines.append(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    return "\n".join(lines) + "\n"
