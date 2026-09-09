"""Bridge downloaded governance artifacts to GitHub attestation verification."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path
from typing import Any


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling policy module when this file is executed by path."""

    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


governance_attestation = load_sibling(
    "dpone_agent_governance_artifact_attestation", "governance_artifact_attestation.py"
)
governance_content = load_sibling("dpone_agent_governance_artifact_content", "governance_artifact_content.py")
GitHubArtifactAttestationEvidence = governance_attestation.GitHubArtifactAttestationEvidence
verify_governance_subject = governance_attestation.verify_governance_subject


def fetch_artifact_attestation(
    archive_bytes: bytes | None,
    *,
    repo: str,
    token: str,
    signer_workflow: str,
    require_attestation: bool,
) -> Any | None:
    """Verify attestation for a governance JSON subject extracted from an archive."""

    if not require_attestation:
        return None
    if archive_bytes is None:
        return governance_attestation.error_evidence("Governance artifact archive bytes are unavailable.")
    subject = governance_content.governance_gate_file_bytes_from_archive_bytes(archive_bytes)
    if subject.content is None:
        return governance_attestation.error_evidence(
            [f"Governance artifact attestation subject is unavailable: {error}" for error in subject.errors]
        )
    with tempfile.TemporaryDirectory(prefix="dpone-agent-attestation-") as directory:
        subject_path = Path(directory) / governance_content.GOVERNANCE_GATE_FILENAME
        subject_path.write_bytes(subject.content)
        return verify_governance_subject(
            subject_path,
            repo=repo,
            token=token,
            signer_workflow=signer_workflow,
        )


def default_signer_workflow(repo: str) -> str:
    """Return the default signer workflow identity for dpone CI evidence."""

    return f"{repo}/.github/workflows/ci.yml"
