"""Route certification pack generation for readiness-compatible evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from dpone.ops.route_readiness import RouteReadinessService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.evidence import RouteEvidenceReader
from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_certification_pack.v1"


@dataclass(frozen=True, slots=True)
class RouteEvidenceProbeResult:
    """A safe probe result that can be written as route evidence."""

    name: str
    passed: bool
    summary: str
    payload: Mapping[str, object] = field(default_factory=dict)
    blockers: tuple[str, ...] = tuple()
    source_path: str | Path | None = None


class RouteEvidenceProbe(Protocol):
    """Small extension point for generating one evidence artifact."""

    name: str

    def collect(self, *, output_dir: Path, route: RouteKey) -> RouteEvidenceProbeResult:
        """Collect evidence without executing heavy certification tests."""


@dataclass(frozen=True, slots=True)
class RouteCertificationPackReport:
    """Stable JSON/Markdown route certification pack contract."""

    route: RouteKey
    profile: RouteProfile | None
    passed: bool
    readiness_level: str
    readiness_score: float
    blockers: tuple[str, ...]
    artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str
    readiness_json_path: str
    readiness_markdown_path: str

    @property
    def evidence_status(self) -> str:
        """Expose the pack's validated evidence result independently of readiness level."""

        return "PASS" if self.passed else "FAIL"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "readiness_level": self.readiness_level,
            "readiness_score": self.readiness_score,
            "blockers": list(self.blockers),
            "artifacts": dict(self.artifacts),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "readiness_json_path": self.readiness_json_path,
            "readiness_markdown_path": self.readiness_markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone route certification pack",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Readiness level: `{self.readiness_level}`",
            f"- Readiness score: `{self.readiness_score}`",
            f"- Readiness JSON: `{self.readiness_json_path}`",
            "",
            "| evidence | path |",
            "|---|---|",
        ]
        for name, path in self.artifacts.items():
            lines.append(f"| `{name}` | `{path}` |")
        lines.extend(["", "## Blockers", ""])
        if self.blockers:
            lines.extend(f"- `{item}`" for item in self.blockers)
        else:
            lines.append("- none")
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Open `route_readiness.json` for the final go/no-go decision.",
                "2. Regenerate missing evidence artifacts before release review.",
                "3. Fix failed source artifacts in their specialized certification gate.",
                "4. Re-run this command after refreshing route evidence.",
                "",
            ]
        )
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class RouteCertificationPackService:
    """Generate route evidence files and evaluate them through route readiness."""

    def __init__(
        self,
        *,
        repo_root: str | Path | None = None,
        catalog: RouteProfileCatalog | None = None,
        probes: Sequence[RouteEvidenceProbe] = (),
    ) -> None:
        self._repo_root = Path(repo_root or Path.cwd())
        self._catalog = catalog or RouteProfileCatalog.default()
        self._reader = RouteEvidenceReader()
        self._probes = {probe.name: probe for probe in probes}

    def build(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
    ) -> RouteCertificationPackReport:
        directory = Path(output_dir)
        evidence_dir = directory / "evidence"
        evidence_dir.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        generated = self._generate_evidence(
            evidence_dir=evidence_dir, route=route, profile=profile, artifacts=artifacts
        )
        readiness = RouteReadinessService(catalog=self._catalog).evaluate(
            output_dir=directory / "readiness",
            source=source,
            sink=sink,
            strategy=strategy,
            artifacts=generated,
        )
        report = RouteCertificationPackReport(
            route=route,
            profile=profile,
            passed=readiness.passed,
            readiness_level=readiness.level,
            readiness_score=readiness.score,
            blockers=readiness.blockers,
            artifacts={name: str(path) for name, path in generated.items()},
            output_dir=str(directory),
            json_path=str(directory / "route_certification_pack.json"),
            markdown_path=str(directory / "route_certification_pack.md"),
            readiness_json_path=readiness.json_path,
            readiness_markdown_path=readiness.markdown_path,
        )
        report.write()
        return report

    def _generate_evidence(
        self,
        *,
        evidence_dir: Path,
        route: RouteKey,
        profile: RouteProfile | None,
        artifacts: Mapping[str, str | Path],
    ) -> dict[str, Path]:
        if profile is None:
            return {}
        required = tuple(profile.required_evidence)
        names = tuple(dict.fromkeys((*required, *sorted(artifacts))))
        return {
            name: self._write_evidence(
                evidence_dir=evidence_dir,
                route=route,
                profile=profile,
                name=name,
                path=artifacts.get(name),
                required=name in required,
            )
            for name in names
        }

    def _write_evidence(
        self,
        *,
        evidence_dir: Path,
        route: RouteKey,
        profile: RouteProfile,
        name: str,
        path: str | Path | None,
        required: bool,
    ) -> Path:
        target = evidence_dir / f"{name}.json"
        payload = self._payload(
            evidence_dir=evidence_dir,
            route=route,
            profile=profile,
            name=name,
            path=path,
            required=required,
        )
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return target

    def _payload(
        self,
        *,
        evidence_dir: Path,
        route: RouteKey,
        profile: RouteProfile,
        name: str,
        path: str | Path | None,
        required: bool,
    ) -> dict[str, object]:
        if path is not None:
            return self._payload_from_existing(name=name, path=path, required=required)
        if probe := self._probes.get(name):
            return self._payload_from_probe(probe.collect(output_dir=evidence_dir, route=route), route=route)
        if auto := self._auto_payload(route=route, profile=profile, name=name):
            return auto
        return {
            "passed": not required,
            "summary": "required route evidence missing" if required else "optional route evidence missing",
            "blockers": [f"{name}.missing"] if required else [],
            "route": route.to_dict(),
            "source_path": "",
        }

    def _payload_from_existing(self, *, name: str, path: str | Path, required: bool) -> dict[str, object]:
        item = self._reader.read(name=name, path=path, required=required)
        payload: dict[str, object] = {
            "passed": item.passed,
            "summary": item.summary,
            "blockers": list(item.blockers),
            "required": item.required,
            "kind": item.kind,
            "source_path": item.path,
            "source_sha256": item.sha256,
            "source_missing": item.missing,
        }
        if item.evidence_status is not None:
            payload["evidence_status"] = item.evidence_status
        return payload

    @staticmethod
    def _payload_from_probe(result: RouteEvidenceProbeResult, *, route: RouteKey) -> dict[str, object]:
        payload = {
            "passed": result.passed,
            "summary": result.summary,
            "blockers": list(result.blockers),
            "route": route.to_dict(),
            "source_path": str(result.source_path or ""),
        }
        payload.update(dict(result.payload))
        return payload

    def _auto_payload(self, *, route: RouteKey, profile: RouteProfile, name: str) -> dict[str, object] | None:
        if name == "matrix_case":
            return {
                "passed": True,
                "summary": "route exists in integration matrix",
                "route": route.to_dict(),
                "profile": profile.to_dict(),
                "blockers": [],
            }
        if name == "docs_runbook":
            return self._docs_payload(route=route, profile=profile)
        if name == "manifest_example":
            return self._manifest_example_payload(route=route, profile=profile)
        return None

    def _docs_payload(self, *, route: RouteKey, profile: RouteProfile) -> dict[str, object]:
        path = self._repo_root / profile.docs_link
        passed = path.is_file()
        return {
            "passed": passed,
            "summary": "source-sink runbook exists" if passed else "source-sink runbook missing",
            "blockers": [] if passed else ["docs_runbook.missing"],
            "route": route.to_dict(),
            "source_path": str(path),
        }

    def _manifest_example_payload(self, *, route: RouteKey, profile: RouteProfile) -> dict[str, object]:
        path = self._repo_root / profile.docs_link
        text = path.read_text(encoding="utf-8") if path.is_file() else ""
        passed = "Copy/paste manifest" in text and "```yaml" in text
        return {
            "passed": passed,
            "summary": "source-sink guide includes copy/paste manifest"
            if passed
            else "source-sink guide is missing copy/paste manifest",
            "blockers": [] if passed else ["manifest_example.missing"],
            "route": route.to_dict(),
            "source_path": str(path),
        }
