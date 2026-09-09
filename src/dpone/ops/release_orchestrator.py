"""End-to-end release operations orchestration for dpone ops artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.artifact_index import ArtifactIndexService
from dpone.ops.checksums import sha256_file
from dpone.ops.docs_publish_pack import DocsPublishPackService
from dpone.ops.evidence_chain import EvidenceChainService
from dpone.ops.release_gate import ReleaseGateService
from dpone.ops.runbook_pack import RunbookPackService


@dataclass(frozen=True, slots=True)
class ReleaseOrchestrationStep:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseOrchestrationReport:
    release: str
    passed: bool
    blockers: tuple[str, ...]
    steps: tuple[ReleaseOrchestrationStep, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "steps": [step.to_dict() for step in self.steps],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops release orchestration",
            "",
            f"- Release: `{self.release}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| step | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        for step in self.steps:
            status = "pass" if step.passed else "fail"
            lines.append(f"| `{step.name}` | {status} | `{step.sha256}` | {step.summary} | `{step.path}` |")
        if self.blockers:
            lines.extend(["", "Resolve release orchestration blockers before publishing:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class ReleaseOrchestratorService:
    """Runs the standard dpone release evidence pipeline as one operation."""

    def __init__(
        self,
        *,
        artifact_index: ArtifactIndexService | None = None,
        evidence_chain: EvidenceChainService | None = None,
        release_gate: ReleaseGateService | None = None,
        docs_publish_pack: DocsPublishPackService | None = None,
        runbook_pack: RunbookPackService | None = None,
    ) -> None:
        self._artifact_index = artifact_index or ArtifactIndexService()
        self._evidence_chain = evidence_chain or EvidenceChainService()
        self._release_gate = release_gate or ReleaseGateService()
        self._docs_publish_pack = docs_publish_pack or DocsPublishPackService()
        self._runbook_pack = runbook_pack or RunbookPackService()

    def run(
        self,
        *,
        output_dir: str | Path,
        release: str,
        roots: Sequence[str | Path],
        artifacts: Mapping[str, str | Path] | None = None,
    ) -> ReleaseOrchestrationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        external_artifacts = dict(artifacts or {})

        artifact_index_report = self._artifact_index.build(
            output_dir=directory / "artifact-index",
            roots=roots,
            release=release,
        )
        artifact_index_path = Path(artifact_index_report.output_dir) / "artifact_index.json"

        evidence_entry = self._evidence_chain.append(
            chain_dir=directory / "evidence-chain",
            release=release,
            artifact_index_path=artifact_index_path,
        )
        evidence_chain_path = Path(evidence_entry.entry_path)

        release_gate_artifacts = {
            **external_artifacts,
            "artifact_index": artifact_index_path,
            "evidence_chain": evidence_chain_path,
        }
        release_gate_report = self._release_gate.evaluate(
            artifact_dir=directory / "release-gate",
            release=release,
            artifacts=release_gate_artifacts,
        )
        release_gate_path = Path(release_gate_report.artifact_dir) / "release_gate.json"

        publish_artifacts = {
            **release_gate_artifacts,
            "release_gate": release_gate_path,
        }
        docs_report = self._docs_publish_pack.build(
            output_dir=directory / "docs-publish-pack",
            release=release,
            artifacts=publish_artifacts,
        )
        docs_pack_path = Path(docs_report.output_dir) / "docs_publish_pack.json"

        runbook_artifacts = {
            **publish_artifacts,
            "docs_publish_pack": docs_pack_path,
        }
        runbook_report = self._runbook_pack.build(
            output_dir=directory / "runbook-pack",
            runbook_id=f"{release}-release",
            title=f"dpone {release} release operations runbook",
            artifacts=runbook_artifacts,
        )
        runbook_path = Path(runbook_report.output_dir) / "runbook_pack.json"

        steps = (
            self._step(
                name="artifact_index",
                path=artifact_index_path,
                passed=artifact_index_report.passed,
                summary=f"total_artifacts={artifact_index_report.total_artifacts}",
            ),
            self._step(
                name="evidence_chain",
                path=evidence_chain_path,
                passed=evidence_entry.verified,
                summary=f"chain_hash={evidence_entry.chain_hash[:12]}",
            ),
            self._step(
                name="release_gate",
                path=release_gate_path,
                passed=release_gate_report.passed,
                summary=f"blockers={len(release_gate_report.blockers)}",
            ),
            self._step(
                name="docs_publish_pack",
                path=docs_pack_path,
                passed=docs_report.passed,
                summary=f"sections={len(docs_report.sections)}",
            ),
            self._step(
                name="runbook_pack",
                path=runbook_path,
                passed=runbook_report.passed,
                summary=f"sections={len(runbook_report.sections)}",
            ),
        )
        blockers = tuple(step.name for step in steps if not step.passed)
        report = ReleaseOrchestrationReport(
            release=release,
            passed=not blockers,
            blockers=blockers,
            steps=steps,
            output_dir=str(directory),
        )
        (directory / "release_orchestration.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "release_orchestration.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    @staticmethod
    def _step(*, name: str, path: Path, passed: bool, summary: str) -> ReleaseOrchestrationStep:
        return ReleaseOrchestrationStep(
            name=name,
            path=str(path),
            sha256=sha256_file(path) if path.exists() else "0" * 64,
            passed=passed,
            summary=summary,
        )
