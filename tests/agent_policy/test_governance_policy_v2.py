"""Non-live SS-46 cutover: governance policy v2 parser and projection compare."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


policy_v2 = _load("dpone_agent_governance_policy_v2", "tools/agent_policy/governance_policy_v2.py")
projection = _load("dpone_agent_governance_projection", "tools/agent_policy/governance_projection.py")

FIXTURE = ROOT / "tests/fixtures/agent_policy/governance_policy_v2_fixture.yml"


def _fixture_payload() -> dict[str, Any]:
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def test_parser_returns_immutable_branch_and_release_views() -> None:
    parsed = policy_v2.parse_governance_policy(_fixture_payload())
    branch = parsed.branch_view
    release = parsed.release_view
    assert branch.mode == "solo_maintainer"
    assert branch.ruleset.id == 18806829
    assert branch.ruleset.version_id == 9001
    assert branch.ruleset.target == "branch"
    assert branch.context_names == ("Agent PR receipt", "Analyze Python")
    assert release.repository.protected_base_ref == "refs/heads/master"
    assert release.tag_ruleset.target == "tag"
    assert release.immutable_releases_required is True
    with pytest.raises(AttributeError):
        branch.mode = "multi_reviewer"  # type: ignore[misc]


def test_parser_rejects_placeholder_and_unknown_keys() -> None:
    payload = _fixture_payload()
    payload["branch_governance"]["ruleset"]["version_id"] = "<UNVERIFIED_BRANCH_RULESET_VERSION_ID>"
    with pytest.raises(ValueError, match="placeholder"):
        policy_v2.parse_governance_policy(payload)

    payload = _fixture_payload()
    payload["extra"] = True
    with pytest.raises(ValueError, match="unknown"):
        policy_v2.parse_governance_policy(payload)


def test_projection_compare_binds_ss46_fields() -> None:
    parsed = policy_v2.parse_governance_policy(_fixture_payload())
    expected = projection.canonical_ruleset_projection(parsed.branch_view.ruleset)
    live_ok = {
        "id": 18806829,
        "version_id": 9001,
        "name": "protect-master",
        "target": "branch",
        "enforcement": "active",
        "conditions": {"include": ["refs/heads/master"], "exclude": []},
        "bypass_actors": [
            {"actor_id": 1, "actor_type": "RepositoryRole", "bypass_mode": "always"},
        ],
        "required_status_checks": {
            "strict": True,
            "checks": [
                {
                    "context": "Agent PR receipt",
                    "integration_id": 15368,
                    "producer": {
                        "kind": "github_actions_workflow",
                        "workflow_path": ".github/workflows/agent-pr-receipt.yml",
                        "workflow_id": 111,
                    },
                },
                {
                    "context": "Analyze Python",
                    "integration_id": 15368,
                    "producer": {
                        "kind": "github_actions_workflow",
                        "workflow_path": ".github/workflows/ci.yml",
                        "workflow_id": 222,
                    },
                },
            ],
        },
    }
    assert projection.compare_ruleset_projection(expected, live_ok) == ()

    live_drift = dict(live_ok)
    live_drift["version_id"] = 9002
    codes = {item.code for item in projection.compare_ruleset_projection(expected, live_drift)}
    assert "RULESET_VERSION_DRIFT" in codes

    live_bypass = dict(live_ok)
    live_bypass["bypass_actors"] = []
    codes = {item.code for item in projection.compare_ruleset_projection(expected, live_bypass)}
    assert "BYPASS_ACTOR_DRIFT" in codes

    live_producer = json.loads(json.dumps(live_ok))
    live_producer["required_status_checks"]["checks"][0]["producer"]["workflow_id"] = 999
    codes = {item.code for item in projection.compare_ruleset_projection(expected, live_producer)}
    assert "REQUIRED_CONTEXT_PRODUCER_MISMATCH" in codes


def test_release_schemas_validate_minimal_fixtures() -> None:
    receipt_schema = json.loads(
        (ROOT / "docs/schemas/release/release-receipt-envelope-v2.schema.json").read_text(encoding="utf-8")
    )
    bundle_schema = json.loads(
        (ROOT / "docs/schemas/release/release-public-bundle-v2.schema.json").read_text(encoding="utf-8")
    )
    evidence_schema = json.loads(
        (ROOT / "docs/schemas/release/release-evidence-v2.schema.json").read_text(encoding="utf-8")
    )
    governance_schema = json.loads(
        (ROOT / "evals/agent/github-governance-policy-v2.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(governance_schema).validate(_fixture_payload())

    digest = "sha256:" + ("a" * 64)
    receipt = {
        "schema": "dpone.release-receipt-envelope.v2",
        "schema_version": 2,
        "receipt_id": digest,
        "receipt_type": "governance_snapshot",
        "stream": {
            "release_identity_id": digest,
            "release_authority_id": digest,
            "sequence": 0,
            "previous": "GENESIS",
        },
        "scope": {"kind": "release", "release_identity_id": digest},
        "attempt": {"attempt_id": digest, "queue_entry_id": digest},
        "lease": {"lease_id": digest, "fencing_token": 1},
        "producer": {
            "kind": "github_actions_job",
            "repository_id": 1,
            "workflow_id": 2,
            "workflow_path": ".github/workflows/controller.yml",
            "workflow_sha": "b" * 40,
            "run_id": 3,
            "run_attempt": 1,
            "job_name": "governance-a",
            "environment": "none",
        },
        "timestamps": {
            "observed_at": "2026-07-19T00:00:00Z",
            "committed_at": "2026-07-19T00:00:01Z",
        },
        "payload": {"kind": "GOVERNANCE_SNAPSHOT", "snapshot": "A", "snapshot_sha256": digest},
        "payload_sha256": digest,
    }
    Draft202012Validator(receipt_schema).validate(receipt)

    bundle = {
        "schema": "dpone.release-public-bundle.v2",
        "schema_version": 2,
        "candidate_id": digest,
        "manifest_sha256": digest,
        "distributions": [
            {
                "project": "dpone",
                "filename": "dpone-1.0.0-py3-none-any.whl",
                "size": 1,
                "sha256": "c" * 64,
            }
        ],
        "asset_names": ["dpone-1.0.0-py3-none-any.whl"],
    }
    Draft202012Validator(bundle_schema).validate(bundle)

    evidence = {
        "schema": "dpone.release-evidence.v2",
        "schema_version": 2,
        "status": "PASS",
        "decision": "GO",
        "state": "CLOSED",
        "release_identity_id": digest,
        "release_authority_id": digest,
        "candidate_id": digest,
        "attempt_id": digest,
        "authorization": {
            "authorization_id": digest,
            "authorization_state": "AUTHORIZED",
            "lease_id": digest,
            "fencing_token": 1,
        },
        "governance_snapshots": {
            "A": {
                "read_count": 2,
                "snapshot_sha256": digest,
                "protected_base_sha": "d" * 40,
            },
            "B": {
                "read_count": 2,
                "snapshot_sha256": digest,
                "protected_base_sha": "d" * 40,
            },
            "C": {
                "read_count": 2,
                "snapshot_sha256": digest,
                "protected_base_sha": "d" * 40,
            },
        },
    }
    Draft202012Validator(evidence_schema).validate(evidence)


def test_production_policy_remains_v1_until_atomic_cutover() -> None:
    production = yaml.safe_load((ROOT / ".agents/policy/github-branch-protection.yml").read_text(encoding="utf-8"))
    assert production["schema_version"] == 1
    with pytest.raises(ValueError, match="schema_version"):
        policy_v2.parse_governance_policy(production)


def test_frozen_policy_loader_parses_v2_fixture_projection() -> None:
    frozen = _load("dpone_agent_release_frozen_policy_v2", "tools/agent_policy/release_frozen_policy.py")
    requirements = frozen.parse_policy_requirements(FIXTURE.read_text(encoding="utf-8"))
    assert requirements.schema_version == 2
    assert requirements.ruleset_id == 18806829
    assert requirements.protected_branch == "master"
    assert requirements.ruleset_projection is not None
    assert requirements.ruleset_projection["version_id"] == 9001


def test_evaluate_snapshot_applies_ss46_projection_blockers() -> None:
    evaluate = _load("dpone_agent_release_commit_evaluate_v2", "tools/agent_policy/release_commit_evaluate.py")
    models = _load("dpone_agent_release_commit_models_v2", "tools/agent_policy/release_commit_models.py")
    live = _load("dpone_agent_governance_live_ruleset_v2", "tools/agent_policy/governance_live_ruleset.py")
    parsed = policy_v2.parse_governance_policy(_fixture_payload())
    expected = projection.canonical_ruleset_projection(parsed.branch_view.ruleset)
    live_payload = {
        "id": 18806829,
        "name": "protect-master",
        "target": "branch",
        "enforcement": "active",
        "current_version": {"id": 9002},
        "conditions": {"ref_name": {"include": ["refs/heads/master"], "exclude": []}},
        "bypass_actors": [
            {"actor_id": 1, "actor_type": "RepositoryRole", "bypass_mode": "always"},
        ],
        "rules": [
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "required_status_checks": [
                        {"context": "Agent PR receipt", "integration_id": 15368},
                        {"context": "Analyze Python", "integration_id": 15368},
                    ],
                },
            }
        ],
    }
    snapshot = models.LiveSnapshot(
        "active",
        (
            models.RequiredContext("Agent PR receipt", 15368),
            models.RequiredContext("Analyze Python", 15368),
        ),
        (),
        live.live_ruleset_projection(live_payload),
    )
    report = evaluate.evaluate_snapshot(
        snapshot,
        "PaulKov/dpone",
        "a" * 40,
        18806829,
        1,
        parsed.branch_view.context_names,
        policy_projection=expected,
    )
    assert report.status == "FAIL"
    assert "RULESET_VERSION_DRIFT" in {item.code for item in report.blockers}
