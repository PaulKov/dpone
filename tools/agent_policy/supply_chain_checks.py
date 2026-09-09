"""Supply-chain control checks used by the agent governance gate."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

Status = Literal["PASS", "FAIL", "N/A"]


@dataclass(frozen=True)
class ControlCheck:
    """Single machine-readable governance control result."""

    name: str
    status: Status
    details: str
    artifact: str | None = None


def check_release_attestations(root: Path, *, release_workflow: str) -> ControlCheck:
    workflow = root / release_workflow
    if not workflow.is_file():
        return ControlCheck(
            name="release_artifact_attestations",
            status="FAIL",
            details=f"Missing {release_workflow}.",
            artifact=release_workflow,
        )
    text = workflow.read_text(encoding="utf-8")
    required_terms = (
        "actions/attest@",
        "subject-path:",
        "dist/*.whl",
        "dist/*.tar.gz",
        "gh attestation verify",
        '--source-ref "$GITHUB_REF"',
        '--source-digest "${RELEASE_COMMIT}"',
        "test_artifacts/supply-chain/github-attestations",
        ".github-attestation.json",
        "name: Authorize controller publication handoff",
        "- attest",
    )
    missing = [term for term in required_terms if term not in text]
    if missing:
        return ControlCheck(
            name="release_artifact_attestations",
            status="FAIL",
            details="Release workflow is missing required attestation controls: " + ", ".join(missing),
            artifact=release_workflow,
        )
    return ControlCheck(
        name="release_artifact_attestations",
        status="PASS",
        details=(
            "Release workflow attests wheel/sdist artifacts and verifies GitHub "
            "attestation receipts before handing immutable candidates to the external publisher."
        ),
        artifact=release_workflow,
    )


def check_scorecard(root: Path, *, scorecard_workflow: str) -> ControlCheck:
    workflow = root / scorecard_workflow
    if not workflow.is_file():
        return ControlCheck(
            name="ossf_scorecard",
            status="FAIL",
            details=f"Missing {scorecard_workflow}.",
            artifact=scorecard_workflow,
        )
    text = workflow.read_text(encoding="utf-8")
    required_terms = (
        "ossf/scorecard-action@",
        "results_file: scorecard-results.sarif",
        "results_format: sarif",
        "publish_results: true",
        "security-events: write",
        "upload-sarif",
    )
    missing = [term for term in required_terms if term not in text]
    if missing:
        return ControlCheck(
            name="ossf_scorecard",
            status="FAIL",
            details="Scorecard workflow is missing required evidence controls: " + ", ".join(missing),
            artifact=scorecard_workflow,
        )
    return ControlCheck(
        name="ossf_scorecard",
        status="PASS",
        details="OpenSSF Scorecard publishes SARIF and public results from a least-privilege workflow.",
        artifact=scorecard_workflow,
    )


def check_slsa_self_assessment(root: Path, *, slsa_self_assessment_doc: str) -> ControlCheck:
    doc = root / slsa_self_assessment_doc
    if not doc.is_file():
        return ControlCheck(
            name="slsa_self_assessment",
            status="FAIL",
            details=f"Missing {slsa_self_assessment_doc}.",
            artifact=slsa_self_assessment_doc,
        )
    text = doc.read_text(encoding="utf-8")
    required_terms = (
        "SLSA v1.2",
        "Build Track",
        "GitHub Artifact Attestations",
        "test_artifacts/supply-chain/github-attestations",
        "No higher-level SLSA claim is made",
    )
    missing = [term for term in required_terms if term not in text]
    if missing:
        return ControlCheck(
            name="slsa_self_assessment",
            status="FAIL",
            details="SLSA self-assessment is missing required statements: " + ", ".join(missing),
            artifact=slsa_self_assessment_doc,
        )
    return ControlCheck(
        name="slsa_self_assessment",
        status="PASS",
        details="SLSA posture is documented as a scoped self-assessment with explicit non-claims.",
        artifact=slsa_self_assessment_doc,
    )
