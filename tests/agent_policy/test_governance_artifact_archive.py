from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


governance_artifact_archive = _load(
    "dpone_agent_governance_artifact_archive_test",
    "tools/agent_policy/governance_artifact_archive.py",
)


def _archive() -> bytes:
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "control_surface_changed": True,
        "changed_paths": ["tools/agent_policy/governance_gate.py"],
        "head_commit": "merge123",
        "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("agent_governance_gate.json", json.dumps(payload))
    return buffer.getvalue()


def test_archive_evidence_records_downloaded_bytes_fingerprint() -> None:
    raw = _archive()

    evidence = governance_artifact_archive.evidence_from_archive_bytes(raw)

    assert evidence.sha256 == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert evidence.size_bytes == len(raw)
    assert evidence.content.status == "PASS"
    assert evidence.content.changed_paths == ["tools/agent_policy/governance_gate.py"]
