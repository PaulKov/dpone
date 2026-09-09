"""Top-level go/no-go release summary across certification evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import certification_trust
from dpone.ops.checksums import sha256_file
from dpone.ops.evidence_chain import EvidenceChainService

_ZERO_SHA256 = "0" * 64
_CHAIN_INDEX = "evidence_chain_index.json"


@dataclass(frozen=True, slots=True)
class ReleaseSummaryItem:
    """Single release evidence input normalized for the final summary."""

    name: str
    kind: str
    path: str
    sha256: str
    passed: bool
    missing: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseSummaryReport:
    """Final release go/no-go report for OSS certification promotion."""

    release_id: str
    passed: bool
    blockers: tuple[str, ...]
    items: tuple[ReleaseSummaryItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release_id": self.release_id,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "items": [item.to_dict() for item in self.items],
            "output_dir": self.output_dir,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone certification release summary",
            "",
            f"- Release ID: `{self.release_id}`",
            f"- Passed: `{self.passed}`",
            "",
            "| evidence | kind | status | sha256 | summary | path |",
            "|---|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(
                f"| `{item.name}` | `{item.kind}` | {status} | `{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        if self.blockers:
            lines.extend(["", "## Release blockers", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Release summary runbook",
                "",
                "1. Re-run replay, source -> sink matrix, and connector certification workflows.",
                "2. Download the failing workflow artifact named in the blocker.",
                "3. Verify the evidence chain before trusting regenerated artifacts.",
                "4. Do not publish a PyPI/GitHub release while this summary is red.",
                "",
            ]
        )
        return "\n".join(lines)


class ReleaseSummaryService:
    """Combines replay, matrix, and connector evidence into one final gate."""

    def __init__(self, *, evidence_chain: EvidenceChainService | None = None) -> None:
        self._evidence_chain = evidence_chain or EvidenceChainService()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        release_id: str,
        replay_chain_dir: str | Path,
        matrix_suite_path: str | Path,
        matrix_chain_dir: str | Path,
        connector_suite_path: str | Path,
        connector_chain_dir: str | Path,
    ) -> ReleaseSummaryReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        items = (
            self._chain_item(
                "replay_evidence_chain",
                replay_chain_dir,
                expected_release=release_id,
            ),
            self._suite_item(
                "matrix_certification_suite",
                matrix_suite_path,
                expected_release=release_id,
            ),
            self._chain_item(
                "matrix_evidence_chain",
                matrix_chain_dir,
                expected_release=release_id,
                required_artifact_path=matrix_suite_path,
            ),
            self._suite_item(
                "connector_certification_suite",
                connector_suite_path,
                expected_release=release_id,
            ),
            self._chain_item(
                "connector_evidence_chain",
                connector_chain_dir,
                expected_release=release_id,
                required_artifact_path=connector_suite_path,
            ),
        )
        blockers = tuple(self._blocker(item) for item in items if not item.passed)
        report = ReleaseSummaryReport(
            release_id=release_id,
            passed=not blockers,
            blockers=blockers,
            items=items,
            output_dir=str(directory),
        )
        (directory / "release_summary.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "release_summary.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _suite_item(
        self,
        name: str,
        path: str | Path,
        *,
        expected_release: str | None = None,
    ) -> ReleaseSummaryItem:
        suite_path = Path(path)
        if not suite_path.exists():
            return ReleaseSummaryItem(
                name=name,
                kind="certification_suite",
                path=str(suite_path),
                sha256=_ZERO_SHA256,
                passed=False,
                missing=True,
                summary="missing certification suite",
            )
        payload = self._json_payload(suite_path)
        identity_matches = expected_release is None or (
            payload.get("schema_version") == "dpone.certification_suite.v1"
            and payload.get("release_id") == expected_release
            and isinstance(payload.get("suite_id"), str)
            and bool(payload["suite_id"])
        )
        passed = certification_trust(payload).passed and identity_matches
        blockers = payload.get("blockers", [])
        case_count = payload.get("case_count", 0)
        evidence_count = payload.get("evidence_count", len(payload.get("items", [])))
        summary = f"cases={case_count}; evidence={evidence_count}; blockers={len(blockers) if isinstance(blockers, list) else 0}"
        return ReleaseSummaryItem(
            name=name,
            kind="certification_suite",
            path=str(suite_path),
            sha256=sha256_file(suite_path),
            passed=passed,
            missing=False,
            summary=summary,
        )

    def _chain_item(
        self,
        name: str,
        chain_dir: str | Path,
        *,
        expected_release: str | None = None,
        required_artifact_path: str | Path | None = None,
    ) -> ReleaseSummaryItem:
        directory = Path(chain_dir)
        index_path = directory / _CHAIN_INDEX
        if not directory.exists() or not index_path.exists():
            return ReleaseSummaryItem(
                name=name,
                kind="evidence_chain",
                path=str(directory),
                sha256=_ZERO_SHA256,
                passed=False,
                missing=False,
                summary="missing evidence chain index",
            )
        verification = self._evidence_chain.verify(
            chain_dir=directory,
            expected_release=expected_release,
            required_artifact_path=required_artifact_path,
        )
        broken = ",".join(verification.broken_entries) if verification.broken_entries else "none"
        return ReleaseSummaryItem(
            name=name,
            kind="evidence_chain",
            path=str(directory),
            sha256=sha256_file(index_path),
            passed=verification.verified,
            missing=False,
            summary=f"entries={verification.entry_count}; broken={broken}",
        )

    @staticmethod
    def _blocker(item: ReleaseSummaryItem) -> str:
        if item.missing:
            return f"{item.name}.missing"
        if item.kind == "evidence_chain":
            return f"{item.name}.not_verified"
        return f"{item.name}.not_passed"

    @staticmethod
    def _json_payload(path: Path) -> dict[str, Any]:
        try:
            with path.open(encoding="utf-8") as file:
                payload = json.load(file)
        except (json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}
