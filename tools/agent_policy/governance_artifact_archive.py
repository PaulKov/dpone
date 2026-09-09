"""Build byte-level evidence for downloaded governance artifact archives."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from dataclasses import dataclass
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


governance_content = load_sibling("dpone_agent_governance_artifact_content", "governance_artifact_content.py")


@dataclass(frozen=True)
class GovernanceArtifactArchiveEvidence:
    """Local evidence derived from the exact downloaded artifact archive bytes."""

    sha256: str | None
    size_bytes: int | None
    content: Any


def evidence_from_archive_bytes(content: bytes) -> GovernanceArtifactArchiveEvidence:
    """Return local archive fingerprint evidence and parsed content evidence."""

    return GovernanceArtifactArchiveEvidence(
        sha256=f"sha256:{hashlib.sha256(content).hexdigest()}",
        size_bytes=len(content),
        content=governance_content.content_from_archive_bytes(content),
    )


def error_evidence(error: str) -> GovernanceArtifactArchiveEvidence:
    """Return archive evidence for a failed download or missing archive URL."""

    return GovernanceArtifactArchiveEvidence(
        sha256=None,
        size_bytes=None,
        content=governance_content.GovernanceArtifactContentEvidence(
            schema_version=None,
            status=None,
            control_surface_changed=None,
            changed_paths=[],
            head_commit=None,
            checks=[],
            errors=[error],
        ),
    )
