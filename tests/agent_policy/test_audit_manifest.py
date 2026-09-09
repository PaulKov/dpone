from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


audit_manifest = _load("dpone_agent_audit_manifest", "tools/agent_policy/audit_manifest.py")

GOVERNANCE_CONTENT = {
    "schema_version": 1,
    "status": "PASS",
    "control_surface_changed": True,
    "changed_paths": ["tools/agent_policy/governance_gate.py"],
    "head_commit": "merge123",
    "checks": [
        {
            "name": "changed_control_surface_red_team",
            "status": "PASS",
            "artifact": None,
            "details": None,
        }
    ],
    "errors": [],
}

GOVERNANCE_CONTENT_CHAIN = {
    "archive_sha256": "sha256:" + "a" * 64,
    "archive_size_bytes": 1719,
    "content_status": "PASS",
    "content_control_surface_changed": True,
    "content_changed_paths": ["tools/agent_policy/governance_gate.py"],
    "content_checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
}

ATTESTATION_CHAIN = {
    "status": "PASS",
    "predicate_type": "https://slsa.dev/provenance/v1",
    "subject_sha256": "c" * 64,
    "source_repository": "PaulKov/dpone",
    "source_ref": "refs/pull/301/merge",
    "source_digest": "abc123",
    "signer_workflow": "PaulKov/dpone/.github/workflows/ci.yml",
    "issuer": "https://token.actions.githubusercontent.com",
    "verified_timestamps_count": 1,
    "runner_environment": "github-hosted",
    "errors": [],
}


def test_pr_receipt_manifest_binds_receipt_to_pr_event() -> None:
    receipt = {
        "schema_version": 2,
        "status": "PASS",
        "errors": [],
        "warnings": [],
        "traceability": {
            "approved_source": "#275",
            "approved_source_kind": "issue",
            "validation_rows": [
                {
                    "check": "Focused tests",
                    "status": "PASS",
                    "command": "uv run pytest tests/agent_policy/test_pr_receipt.py -q",
                    "notes": "33 passed",
                }
            ],
            "validation_statuses": ["PASS"],
            "non_pass_reasons": [],
            "owner_attestation": {
                "owner_review": True,
                "required_checks": True,
                "admin_bypass": True,
                "governance_receipt": True,
            },
            "governance_receipt_referenced": True,
        },
        "github_evidence": {
            "head_sha": "abc123",
            "required_checks": ["Agent PR receipt", "Quality checks (3.12)"],
            "check_runs": [],
            "statuses": [],
            "artifacts": [
                {
                    "name": "agent-governance-gate",
                    "workflow_run_head_sha": "abc123",
                    "workflow_run_id": 42,
                    "expired": False,
                    "url": "https://github.example/artifacts/42",
                    "artifact_id": 42,
                    "digest": "sha256:" + "a" * 64,
                    "size_in_bytes": 1719,
                    "archive_sha256": "sha256:" + "a" * 64,
                    "archive_size_bytes": 1719,
                    "created_at": "2026-07-12T17:00:00Z",
                    "expires_at": "2026-10-10T17:00:00Z",
                    "content": GOVERNANCE_CONTENT,
                    "attestation": ATTESTATION_CHAIN,
                }
            ],
            "errors": [],
        },
        "evidence_chain": {
            "head_sha": "abc123",
            "required_checks": [
                {
                    "name": "Quality checks (3.12)",
                    "source": "check_run",
                    "id": 123,
                    "workflow_run_id": 42,
                    "status": "completed",
                    "conclusion": "success",
                    "state": None,
                    "url": "https://github.example/checks/123",
                    "completed_at": "2026-07-12T17:00:00Z",
                }
            ],
            "governance_artifact": {
                "name": "agent-governance-gate",
                "artifact_id": 42,
                "workflow_run_id": 42,
                "workflow_run_head_sha": "abc123",
                "digest": "sha256:" + "a" * 64,
                "expired": False,
                "url": "https://github.example/artifacts/42",
                **GOVERNANCE_CONTENT_CHAIN,
                "attestation": ATTESTATION_CHAIN,
            },
        },
    }
    event = {
        "pull_request": {
            "number": 301,
            "head": {"ref": "codex/agent-pr-receipt-v2", "sha": "abc123"},
            "base": {"ref": "master", "sha": "base123"},
            "merge_commit_sha": "merge123",
        }
    }

    manifest = audit_manifest.build_pr_receipt_manifest(
        receipt=receipt,
        event=event,
        repository="PaulKov/dpone",
        workflow="Agent PR receipt",
        run_id="29202243209",
        run_attempt="1",
        artifact_name="agent-pr-receipt",
        retention_days=90,
        generated_at="2026-07-12T17:34:32Z",
    )

    assert manifest == {
        "schema_version": 1,
        "kind": "agent_pr_receipt",
        "repository": "PaulKov/dpone",
        "workflow": "Agent PR receipt",
        "run_id": "29202243209",
        "run_attempt": "1",
        "artifact_name": "agent-pr-receipt",
        "artifact_retention_days": 90,
        "generated_at": "2026-07-12T17:34:32Z",
        "pr_number": 301,
        "base_ref": "master",
        "base_sha": "base123",
        "head_ref": "codex/agent-pr-receipt-v2",
        "head_sha": "abc123",
        "merge_sha": "merge123",
        "receipt_status": "PASS",
        "receipt_errors": [],
        "receipt_warnings": [],
        "traceability_source": "#275",
        "traceability_source_kind": "issue",
        "traceability_statuses": ["PASS"],
        "traceability_non_pass_reasons": [],
        "evidence_chain_head_sha": "abc123",
        "evidence_chain_required_checks": [
            {
                "name": "Quality checks (3.12)",
                "source": "check_run",
                "id": 123,
                "workflow_run_id": 42,
                "status": "completed",
                "conclusion": "success",
                "state": None,
                "url": "https://github.example/checks/123",
                "completed_at": "2026-07-12T17:00:00Z",
            }
        ],
        "evidence_chain_governance_artifact": {
            "name": "agent-governance-gate",
            "artifact_id": 42,
            "workflow_run_id": 42,
            "workflow_run_head_sha": "abc123",
            "digest": "sha256:" + "a" * 64,
            "expired": False,
            "url": "https://github.example/artifacts/42",
            **GOVERNANCE_CONTENT_CHAIN,
            "attestation": ATTESTATION_CHAIN,
        },
        "required_checks": ["Agent PR receipt", "Quality checks (3.12)"],
        "evidence_artifacts": [
            {
                "name": "agent-governance-gate",
                "workflow_run_head_sha": "abc123",
                "workflow_run_id": 42,
                "expired": False,
                "url": "https://github.example/artifacts/42",
                "artifact_id": 42,
                "digest": "sha256:" + "a" * 64,
                "size_in_bytes": 1719,
                "archive_sha256": "sha256:" + "a" * 64,
                "archive_size_bytes": 1719,
                "created_at": "2026-07-12T17:00:00Z",
                "expires_at": "2026-10-10T17:00:00Z",
                "content": GOVERNANCE_CONTENT,
                "attestation": ATTESTATION_CHAIN,
            }
        ],
    }
    Draft202012Validator(json.loads((ROOT / "evals/agent/audit-manifest.schema.json").read_text())).validate(manifest)


def test_pr_receipt_manifest_cli_writes_json(tmp_path: Path) -> None:
    receipt_path = tmp_path / "agent_pr_receipt.json"
    event_path = tmp_path / "event.json"
    output_path = tmp_path / "agent_audit_manifest.json"
    receipt_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "status": "N/A",
                "errors": [],
                "warnings": [],
                "traceability": None,
                "github_evidence": None,
                "evidence_chain": None,
            }
        ),
        encoding="utf-8",
    )
    event_path.write_text(json.dumps({"pull_request": {"number": 7, "head": {"sha": "head"}}}), encoding="utf-8")

    code = audit_manifest.main(
        [
            "pr-receipt",
            "--receipt",
            str(receipt_path),
            "--event",
            str(event_path),
            "--repository",
            "PaulKov/dpone",
            "--workflow",
            "Agent PR receipt",
            "--run-id",
            "100",
            "--run-attempt",
            "1",
            "--artifact-name",
            "agent-pr-receipt",
            "--retention-days",
            "90",
            "--generated-at",
            "2026-07-12T17:34:32Z",
            "--output",
            str(output_path),
        ]
    )
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["schema_version"] == 1
    assert payload["pr_number"] == 7
    assert payload["head_sha"] == "head"
    assert payload["receipt_status"] == "N/A"
    assert payload["traceability_source"] is None
    assert payload["traceability_statuses"] == []
    assert payload["evidence_chain_head_sha"] is None
    assert payload["evidence_chain_required_checks"] == []
    assert payload["evidence_chain_governance_artifact"] is None
