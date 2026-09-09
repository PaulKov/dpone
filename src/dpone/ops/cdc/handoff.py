"""CDC snapshot handoff facade over catalog, evidence, and policy services."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.ops.cdc.catalog import CdcHandoffCatalog
from dpone.ops.cdc.evidence import CdcEvidenceReader
from dpone.ops.cdc.models import CdcHandoffReport, CdcStreamKey
from dpone.ops.cdc.policy import CdcHandoffPolicy


class SnapshotCdcHandoffService:
    """Build CDC handoff reports without executing live source or sink work."""

    def __init__(
        self,
        *,
        catalog: CdcHandoffCatalog | None = None,
        evidence_reader: CdcEvidenceReader | None = None,
        policy: CdcHandoffPolicy | None = None,
    ) -> None:
        self._catalog = catalog or CdcHandoffCatalog.default()
        self._evidence_reader = evidence_reader or CdcEvidenceReader()
        self._policy = policy or CdcHandoffPolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        source_dataset: str,
        target_dataset: str,
        artifacts: Mapping[str, str | Path],
    ) -> CdcHandoffReport:
        directory = Path(output_dir)
        stream = CdcStreamKey.of(
            source=source,
            sink=sink,
            strategy=strategy,
            source_dataset=source_dataset,
            target_dataset=target_dataset,
        )
        profile = self._catalog.get(stream.route)
        if profile is None:
            blocker = f"cdc.route_unsupported:{stream.route.colon_id}"
            report = CdcHandoffReport(
                stream=stream,
                profile=None,
                passed=False,
                level="unknown",
                score=0.0,
                blockers=(blocker,),
                warnings=tuple(),
                next_actions=("Add CDC handoff metadata for the matrix route before requesting release evidence.",),
                evidence=tuple(),
                output_dir=str(directory),
                json_path=str(directory / "cdc_handoff.json"),
                markdown_path=str(directory / "cdc_handoff.md"),
            )
            report.write()
            return report

        evidence = self._evidence_reader.read(profile=profile, artifacts=artifacts)
        decision = self._policy.evaluate(profile=profile, evidence=evidence)
        report = CdcHandoffReport(
            stream=stream,
            profile=profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "cdc_handoff.json"),
            markdown_path=str(directory / "cdc_handoff.md"),
        )
        report.write()
        return report
