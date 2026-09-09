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


governance_artifact_content = _load(
    "dpone_agent_governance_artifact_content",
    "tools/agent_policy/governance_artifact_content.py",
)
artifact_resource_limits = _load(
    "dpone_agent_artifact_resource_limits",
    "tools/agent_policy/artifact_resource_limits.py",
)


def _archive(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "PASS",
        "control_surface_changed": True,
        "changed_paths": ["tools/agent_policy/pr_receipt.py"],
        "head_commit": "abc123",
        "checks": [
            {
                "name": "changed_control_surface_red_team",
                "status": "PASS",
                "artifact": "evals/agent/red_team_scenarios.yml",
                "details": "Agent-control paths changed and the executable red-team catalog validates.",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_governance_artifact_content_parses_valid_archive() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive({"agent_governance_gate.json": json.dumps(_payload())})
    )

    assert evidence.schema_version == 1
    assert evidence.status == "PASS"
    assert evidence.control_surface_changed is True
    assert evidence.changed_paths == ["tools/agent_policy/pr_receipt.py"]
    assert evidence.head_commit == "abc123"
    assert evidence.check_status("changed_control_surface_red_team") == "PASS"
    assert evidence.errors == []


def test_governance_artifact_content_accepts_nested_uploaded_path() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive({"test_artifacts/agent-policy/agent_governance_gate.json": json.dumps(_payload())})
    )

    assert evidence.status == "PASS"
    assert evidence.errors == []


def test_governance_artifact_content_exposes_exact_json_subject_bytes() -> None:
    raw_json = json.dumps(_payload(), sort_keys=True).encode("utf-8")

    extracted = governance_artifact_content.governance_gate_file_bytes_from_archive_bytes(
        _archive({"test_artifacts/agent-policy/agent_governance_gate.json": raw_json.decode("utf-8")})
    )

    assert extracted.content == raw_json
    assert extracted.errors == []

    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive({"agent_governance_gate.json": raw_json.decode("utf-8")})
    )
    assert evidence.subject_sha256 == hashlib.sha256(raw_json).hexdigest()


def test_governance_artifact_content_subject_bytes_fail_on_duplicate_json() -> None:
    extracted = governance_artifact_content.governance_gate_file_bytes_from_archive_bytes(
        _archive(
            {
                "agent_governance_gate.json": json.dumps(_payload()),
                "nested/agent_governance_gate.json": json.dumps(_payload()),
            }
        )
    )

    assert extracted.content is None
    assert any("multiple agent_governance_gate.json" in error for error in extracted.errors)


def test_governance_artifact_content_rejects_compressed_oversized_subject() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "agent_governance_gate.json",
            b"x" * (artifact_resource_limits.MAX_ZIP_MEMBER_BYTES + 1),
        )

    extracted = governance_artifact_content.governance_gate_file_bytes_from_archive_bytes(buffer.getvalue())

    assert extracted.content is None
    assert any("member size limit" in error for error in extracted.errors)


def test_governance_artifact_content_fails_when_json_is_missing() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(_archive({"other.json": "{}"}))

    assert evidence.status is None
    assert any("agent_governance_gate.json" in error for error in evidence.errors)


def test_governance_artifact_content_fails_when_json_is_ambiguous() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive(
            {
                "agent_governance_gate.json": json.dumps(_payload()),
                "nested/agent_governance_gate.json": json.dumps(_payload()),
            }
        )
    )

    assert any("multiple" in error.lower() for error in evidence.errors)


def test_governance_artifact_content_fails_on_invalid_json() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive({"agent_governance_gate.json": "{not-json"})
    )

    assert any("invalid JSON" in error for error in evidence.errors)


def test_governance_artifact_content_fails_on_non_object_json() -> None:
    evidence = governance_artifact_content.content_from_archive_bytes(
        _archive({"agent_governance_gate.json": json.dumps([])})
    )

    assert any("must be a JSON object" in error for error in evidence.errors)


def test_governance_artifact_content_validates_receipt_paths_and_red_team_check() -> None:
    evidence = governance_artifact_content.content_from_payload(_payload())

    errors = governance_artifact_content.validate_content_for_receipt(
        content=evidence,
        expected_changed_paths=["tools/agent_policy/pr_receipt.py"],
        control_surface_changed=True,
        governance_artifact_name="agent-governance-gate",
        head_sha="abc123",
    )

    assert errors == []


def test_governance_artifact_content_rejects_stale_changed_paths() -> None:
    evidence = governance_artifact_content.content_from_payload(_payload(changed_paths=[]))

    errors = governance_artifact_content.validate_content_for_receipt(
        content=evidence,
        expected_changed_paths=["tools/agent_policy/pr_receipt.py"],
        control_surface_changed=True,
        governance_artifact_name="agent-governance-gate",
        head_sha="abc123",
    )

    assert any("changed_paths" in error and "missing" in error for error in errors)


def test_governance_artifact_content_rejects_mismatched_head_commit() -> None:
    evidence = governance_artifact_content.content_from_payload(_payload(head_commit="wrong-head"))

    errors = governance_artifact_content.validate_content_for_receipt(
        content=evidence,
        expected_changed_paths=["tools/agent_policy/pr_receipt.py"],
        control_surface_changed=True,
        governance_artifact_name="agent-governance-gate",
        head_sha="abc123",
    )

    assert any("head_commit" in error and "abc123" in error for error in errors)


def test_governance_artifact_content_rejects_non_pass_red_team_check() -> None:
    evidence = governance_artifact_content.content_from_payload(
        _payload(checks=[{"name": "changed_control_surface_red_team", "status": "N/A"}])
    )

    errors = governance_artifact_content.validate_content_for_receipt(
        content=evidence,
        expected_changed_paths=["tools/agent_policy/pr_receipt.py"],
        control_surface_changed=True,
        governance_artifact_name="agent-governance-gate",
        head_sha="abc123",
    )

    assert any("changed_control_surface_red_team" in error and "PASS" in error for error in errors)
