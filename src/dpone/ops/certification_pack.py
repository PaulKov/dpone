"""Connector certification 2.0 evidence pack.

The certification pack is the operator-facing layer above individual matrix,
benchmark, lineage, observability, and quality artifacts. It does not execute
tests by itself; it verifies and summarizes immutable artifacts produced by
other services.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed, certification_trust
from dpone.ops.checksums import sha256_file
from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class CertificationPackItem:
    name: str
    path: str
    required: bool
    passed: bool
    sha256: str
    summary: str
    missing: bool = False
    evidence_status: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CertificationCoverage:
    case_count: int
    sources: tuple[str, ...]
    sinks: tuple[str, ...]
    strategies: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "case_count": self.case_count,
            "sources": list(self.sources),
            "sinks": list(self.sinks),
            "strategies": list(self.strategies),
        }


@dataclass(frozen=True, slots=True)
class ConnectorCertificationPackReport:
    pack_id: str
    passed: bool
    generated_at: str
    blockers: tuple[str, ...]
    coverage: CertificationCoverage
    items: tuple[CertificationPackItem, ...]
    output_dir: str
    json_path: str
    markdown_path: str
    evidence_status: str = "UNVERIFIED"

    def to_dict(self) -> dict[str, object]:
        return {
            "pack_id": self.pack_id,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "generated_at": self.generated_at,
            "blockers": list(self.blockers),
            "coverage": self.coverage.to_dict(),
            "items": [item.to_dict() for item in self.items],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        lines = [
            "# dpone connector certification pack",
            "",
            f"- Pack ID: `{self.pack_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Generated at: `{self.generated_at}`",
            f"- Cases: `{self.coverage.case_count}`",
            f"- Sources: `{', '.join(self.coverage.sources) or '-'}`",
            f"- Sinks: `{', '.join(self.coverage.sinks) or '-'}`",
            f"- Strategies: `{', '.join(self.coverage.strategies) or '-'}`",
            "",
            "| artifact | status | required | sha256 | summary | path |",
            "|---|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(
                f"| `{item.name}` | {status} | `{item.required}` | `{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        lines.extend(
            [
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Operator runbook",
                "",
                "1. Re-run the exact source -> sink matrix profile before accepting a changed certification pack.",
                "2. Treat missing required artifacts as a release blocker, not a warning.",
                "3. Compare `coverage` across releases to catch accidental connector/strategy gaps.",
                "4. Attach this pack to release, PR, or manual certification evidence.",
                "",
            ]
        )
        return "\n".join(lines)


class ConnectorCertificationPackService:
    """Builds a single auditable certification pack from evidence artifacts."""

    def build(
        self,
        *,
        output_dir: str | Path,
        pack_id: str,
        artifacts: Mapping[str, str | Path],
        required: Sequence[str] = ("certification_report",),
    ) -> ConnectorCertificationPackReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        required_names = {str(item) for item in required}
        names = tuple(sorted({*artifacts.keys(), *required_names}))
        items = tuple(self._item(name, artifacts.get(name), required=name in required_names) for name in names)
        coverage = self._coverage(artifacts.get("certification_report"))
        blockers = tuple(self._blocker(item) for item in items if not item.passed)
        json_path = directory / "connector_certification_pack.json"
        markdown_path = directory / "connector_certification_pack.md"
        report = ConnectorCertificationPackReport(
            pack_id=pack_id,
            passed=not blockers,
            generated_at=utc_now_iso(),
            blockers=blockers,
            coverage=coverage,
            items=items,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
            evidence_status=_pack_evidence_status(items, blockers),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _item(self, name: str, path: str | Path | None, *, required: bool) -> CertificationPackItem:
        if path is None:
            return CertificationPackItem(
                name=name,
                path="",
                required=required,
                passed=not required,
                sha256="0" * 64,
                summary="optional artifact not provided" if not required else "required artifact missing",
                missing=True,
            )
        artifact_path = Path(path)
        if not artifact_path.is_file():
            return CertificationPackItem(
                name=name,
                path=str(artifact_path),
                required=required,
                passed=False,
                sha256="0" * 64,
                summary="artifact missing",
                missing=True,
            )
        payload = _read_json(artifact_path)
        certification_required = name == "certification_report"
        trust = certification_trust(payload) if certification_required or "evidence_status" in payload else None
        return CertificationPackItem(
            name=name,
            path=str(artifact_path),
            required=required,
            passed=artifact_payload_passed(
                payload,
                require_certification_status=certification_required,
            ),
            sha256=sha256_file(artifact_path),
            summary=_payload_summary(payload),
            missing=False,
            evidence_status=trust.evidence_status if trust is not None else None,
        )

    @staticmethod
    def _blocker(item: CertificationPackItem) -> str:
        suffix = "missing" if item.missing else "not_passed"
        return f"{item.name}.{suffix}"

    @staticmethod
    def _coverage(path: str | Path | None) -> CertificationCoverage:
        if path is None:
            return CertificationCoverage(0, tuple(), tuple(), tuple())
        payload = _read_json(Path(path))
        results = payload.get("results", [])
        sources: set[str] = set()
        sinks: set[str] = set()
        strategies: set[str] = set()
        case_count = 0
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, Mapping):
                    continue
                source, sink, strategy = _case_parts(item)
                if source:
                    sources.add(source)
                if sink:
                    sinks.add(sink)
                if strategy:
                    strategies.add(strategy)
                case_count += 1
        return CertificationCoverage(
            case_count=case_count,
            sources=tuple(sorted(sources)),
            sinks=tuple(sorted(sinks)),
            strategies=tuple(sorted(strategies)),
        )


def _case_parts(item: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    source = str(item.get("source")) if item.get("source") else None
    sink = str(item.get("sink")) if item.get("sink") else None
    strategy = str(item.get("strategy")) if item.get("strategy") else None
    case_id = str(item.get("case_id") or "")
    if case_id and (source is None or sink is None or strategy is None):
        pair, _, parsed_strategy = case_id.partition("__")
        parsed_source, _, parsed_sink = pair.partition("_to_")
        source = source or parsed_source or None
        sink = sink or parsed_sink or None
        strategy = strategy or parsed_strategy or None
    return source, sink, strategy


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.is_file() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _payload_passed(payload: Mapping[str, Any]) -> bool:
    return bool(payload) and artifact_payload_passed(payload)


def _payload_summary(payload: Mapping[str, Any]) -> str:
    if not payload:
        return "invalid or empty json"
    if "status" in payload:
        return f"status={payload['status']}"
    if "metric_count" in payload:
        return f"metric_count={payload['metric_count']}"
    if "results" in payload and isinstance(payload["results"], list):
        return f"results={len(payload['results'])}"
    if "items" in payload and isinstance(payload["items"], list):
        return f"items={len(payload['items'])}"
    return "passed" if _payload_passed(payload) else "failed"


def _pack_evidence_status(
    items: tuple[CertificationPackItem, ...],
    blockers: tuple[str, ...],
) -> str:
    certification = next(
        (item for item in items if item.name == "certification_report"),
        None,
    )
    status = certification.evidence_status if certification is not None else None
    return "PASS" if not blockers and status == "PASS" else status or "UNVERIFIED"
