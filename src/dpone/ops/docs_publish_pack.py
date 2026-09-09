"""GitHub Pages and README publish pack generation for ops artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class DocsPublishSection:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DocsPublishPackReport:
    release: str
    passed: bool
    blockers: tuple[str, ...]
    sections: tuple[DocsPublishSection, ...]
    output_dir: str
    readme_snippet_path: str
    pages_index_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "readme_snippet_path": self.readme_snippet_path,
            "pages_index_path": self.pages_index_path,
            "sections": [section.to_dict() for section in self.sections],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone docs publish pack",
            "",
            f"- Release: `{self.release}`",
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
            lines.extend(["", "Fix publish pack blockers before updating README or GitHub Pages:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class DocsPublishPackService:
    """Builds docs-ready publication snippets from ops artifacts."""

    def build(
        self,
        *,
        output_dir: str | Path,
        release: str,
        artifacts: Mapping[str, str | Path],
    ) -> DocsPublishPackReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        payloads = {name: self._read_payload(Path(path)) for name, path in artifacts.items()}
        sections = tuple(self._section(name, Path(path), payloads[name]) for name, path in sorted(artifacts.items()))
        blockers = tuple(section.name for section in sections if not section.passed)
        readme_snippet_path = directory / "README_SNIPPET.md"
        pages_index_path = directory / "PAGES_INDEX.md"
        readme_snippet_path.write_text(self._readme_snippet(release=release, payloads=payloads), encoding="utf-8")
        pages_index_path.write_text(
            self._pages_index(release=release, sections=sections, payloads=payloads),
            encoding="utf-8",
        )
        report = DocsPublishPackReport(
            release=release,
            passed=not blockers,
            blockers=blockers,
            sections=sections,
            output_dir=str(directory),
            readme_snippet_path=str(readme_snippet_path),
            pages_index_path=str(pages_index_path),
        )
        (directory / "docs_publish_pack.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "docs_publish_pack.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _section(self, name: str, path: Path, payload: Mapping[str, Any]) -> DocsPublishSection:
        if not path.exists():
            return DocsPublishSection(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        return DocsPublishSection(
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
        for key in ("blockers", "violations", "findings", "results", "items", "entries", "strategy_rows"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "total_artifacts" in payload:
            return f"total_artifacts={payload['total_artifacts']}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if bool(payload.get("passed", True)) else "failed"

    def _readme_snippet(self, *, release: str, payloads: Mapping[str, Mapping[str, Any]]) -> str:
        lines = [
            "<!-- dpone-ops-readme-snippet:start -->",
            f"### dpone release readiness `{release}`",
            "",
        ]
        release_gate = payloads.get("release_gate", {})
        if release_gate:
            lines.append(f"- Release gate: `{'passing' if release_gate.get('passed') else 'blocked'}`")
        connector_badges = payloads.get("connector_badges", {})
        entries = connector_badges.get("entries", [])
        if isinstance(entries, list) and entries:
            lines.extend(["", "| connector | badge |", "|---|---|"])
            for entry in entries:
                if isinstance(entry, Mapping):
                    lines.append(f"| `{entry.get('connector')}` | `{entry.get('badge')}` |")
        lines.extend(["", "<!-- dpone-ops-readme-snippet:end -->", ""])
        return "\n".join(lines)

    def _pages_index(
        self,
        *,
        release: str,
        sections: tuple[DocsPublishSection, ...],
        payloads: Mapping[str, Mapping[str, Any]],
    ) -> str:
        lines = [
            "# dpone operational release index",
            "",
            f"Release: `{release}`",
            "",
            "## Sections",
            "",
            "| section | status | path |",
            "|---|---|---|",
        ]
        for section in sections:
            status = "pass" if section.passed else "fail"
            lines.append(f"| `{section.name}` | {status} | `{section.path}` |")
        artifact_index = payloads.get("artifact_index", {})
        if "total_artifacts" in artifact_index:
            lines.extend(["", f"Total indexed artifacts: `{artifact_index['total_artifacts']}`"])
        return "\n".join(lines) + "\n"
