"""Canonical live-ruleset evidence for delayed release publication."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

snapshot = importlib.import_module("tools.agent_policy.release_ruleset_snapshot")
baseline_contract = importlib.import_module("tools.agent_policy.release_authority_baseline")

ROOT = Path(__file__).resolve().parents[2]

REPOSITORY = "PaulKov/dpone"
COMMIT_SHA = "a" * 40
RULESET_ID = 18806829
VERSION_ID = 43592502
APP_ID = 15368
CONTEXTS = ["Agent PR receipt", "Quality checks (3.12)"]
UPDATED_AT = "2026-07-19T18:37:47.596000Z"
PROVIDER_UPDATED_AT = "2026-07-19T21:37:47.596+03:00"


def _policy(path: Path) -> Path:
    payload = {
        "schema_version": 1,
        "ruleset": {
            "id": RULESET_ID,
            "version_id": VERSION_ID,
            "updated_at": UPDATED_AT,
            "name": "protect-master",
            "target": "branch",
            "enforcement": "active",
            "conditions": {"include": ["refs/heads/master"], "exclude": []},
            "branches": ["master"],
            "pull_request": {
                "required": True,
                "allowed_merge_methods": ["merge", "squash"],
                "dismiss_stale_reviews_on_push": True,
                "require_code_owner_review": False,
                "require_extra_approval_for_unattributed_changes": True,
                "require_last_push_approval": False,
                "required_approving_review_count": 0,
                "required_review_thread_resolution": True,
                "required_reviewers": [],
            },
            "required_status_checks": {
                "strict": True,
                "do_not_enforce_on_create": False,
                "integration_id": APP_ID,
                "checks": CONTEXTS,
            },
            "protected_actions": {"block_deletion": True, "block_force_push": True},
            "bypass": {
                "allowed_repository_roles": ["admin"],
                "actors": [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}],
                "disallowed_for": ["agent", "automation", "broad_team"],
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _report(path: Path, *, policy: Path) -> Path:
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "decision": "GO",
        "repository": REPOSITORY,
        "commit_sha": COMMIT_SHA,
        "ruleset_id": RULESET_ID,
        "policy_sha256": hashlib.sha256(policy.read_bytes()).hexdigest(),
        "contexts": [{"context": context, "integration_id": APP_ID, "status": "PASS"} for context in CONTEXTS],
        "blockers": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _live_ruleset(*, strict: bool = True, privileged: bool = False) -> dict[str, Any]:
    checks = [{"context": context, "integration_id": APP_ID} for context in CONTEXTS]
    payload: dict[str, Any] = {
        "id": RULESET_ID,
        "name": "protect-master",
        "target": "branch",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["refs/heads/master"], "exclude": []}},
        "updated_at": PROVIDER_UPDATED_AT,
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {
                "type": "pull_request",
                "parameters": {
                    "allowed_merge_methods": ["merge", "squash"],
                    "dismiss_stale_reviews_on_push": True,
                    "require_code_owner_review": False,
                    "require_extra_approval_for_unattributed_changes": True,
                    "require_last_push_approval": False,
                    "required_approving_review_count": 0,
                    "required_review_thread_resolution": True,
                    "required_reviewers": [],
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "do_not_enforce_on_create": False,
                    "strict_required_status_checks_policy": strict,
                    "required_status_checks": checks,
                },
            },
        ],
    }
    if privileged:
        payload["bypass_actors"] = [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}]
        payload["version_id"] = VERSION_ID
    return payload


def _wire_live(monkeypatch: Any, live: dict[str, Any]) -> None:
    def github_json(path: str, *, token: str, fresh: bool = False) -> dict[str, Any]:
        assert token == "approved-token"
        assert path == f"repos/{REPOSITORY}/rulesets/{RULESET_ID}"
        assert fresh is True
        return copy.deepcopy(live)

    monkeypatch.setattr(snapshot.github_api, "github_json", github_json)


def _capture(tmp_path: Path, monkeypatch: Any, live: dict[str, Any]) -> dict[str, Any]:
    policy = _policy(tmp_path / "policy.yml")
    _wire_live(monkeypatch, live)
    return snapshot.capture(
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        required_check_report=_report(tmp_path / "checks.json", policy=policy),
        policy=policy,
        token="approved-token",
    )


def test_snapshot_binds_read_only_projection_to_privileged_frozen_baseline(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    payload = _capture(tmp_path, monkeypatch, _live_ruleset())

    assert payload["status"] == "PASS"
    assert "version_id" not in payload["projection"]
    assert "bypass_actors" not in payload["projection"]
    assert payload["projection"]["updated_at"] == UPDATED_AT
    assert payload["privileged_fields_observed"] == {"bypass_actors": False, "version_id": False}
    assert len(payload["policy_projection_sha256"]) == 64
    assert payload["projection"]["conditions"]["include"] == ["refs/heads/master"]
    assert payload["projection"]["required_status_checks"]["strict"] is True
    assert [rule["type"] for rule in payload["projection"]["rules"]] == [
        "deletion",
        "non_fast_forward",
        "pull_request",
        "required_status_checks",
    ]


def test_checked_in_release_authority_baseline_is_exact_yaml_projection() -> None:
    policy = ROOT / ".agents/policy/github-branch-protection.yml"
    baseline_path = ROOT / ".agents/policy/github-branch-protection.release-authority.json"
    policy_payload = yaml.safe_load(policy.read_text(encoding="utf-8"))
    ruleset = policy_payload["ruleset"]
    expected_checks = sorted(
        (
            {"context": context, "integration_id": ruleset["required_status_checks"]["integration_id"]}
            for context in ruleset["required_status_checks"]["checks"]
        ),
        key=lambda item: item["context"],
    )
    full_projection, policy_sha256 = snapshot._policy_projection(  # noqa: SLF001 - trusted-dev parity.
        policy,
        ruleset_id=ruleset["id"],
        expected_checks=expected_checks,
    )
    baseline = baseline_contract.load_release_authority_baseline(baseline_path, policy_path=policy)

    assert baseline.ruleset_id == ruleset["id"]
    assert baseline.policy_sha256 == policy_sha256
    assert baseline.integration_ids == {item["context"]: item["integration_id"] for item in expected_checks}
    assert baseline.projection == snapshot.live_ruleset.observable_ruleset_projection(full_projection)
    assert baseline.privileged == {
        "bypass_actors": full_projection["bypass_actors"],
        "updated_at": full_projection["updated_at"],
        "version_id": full_projection["version_id"],
    }
    assert (
        baseline.policy_projection_sha256
        == hashlib.sha256(
            json.dumps(full_projection, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
    )


@pytest.mark.parametrize("drift", ["context", "strict", "conditions", "target", "rule", "updated_at"])
def test_snapshot_rejects_any_security_authority_drift(
    tmp_path: Path,
    monkeypatch: Any,
    drift: str,
) -> None:
    live = _live_ruleset()
    if drift == "context":
        live["rules"][-1]["parameters"]["required_status_checks"].pop()
    elif drift == "strict":
        live["rules"][-1]["parameters"]["strict_required_status_checks_policy"] = False
    elif drift == "conditions":
        live["conditions"]["ref_name"]["include"] = ["refs/heads/not-master"]
    elif drift == "target":
        live["target"] = "tag"
    elif drift == "rule":
        live["rules"] = [rule for rule in live["rules"] if rule["type"] != "non_fast_forward"]
    else:
        live["updated_at"] = "2026-07-19T21:37:48.596+03:00"

    with pytest.raises(ValueError, match="does not match"):
        _capture(tmp_path, monkeypatch, live)


@pytest.mark.parametrize("field", ["bypass_actors", "version_id"])
def test_snapshot_rejects_privileged_drift_when_provider_exposes_it(
    field: str,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    live = _live_ruleset(privileged=True)
    if field == "bypass_actors":
        live[field].append({"actor_id": 99, "actor_type": "Team", "bypass_mode": "always"})
    else:
        live[field] = VERSION_ID + 1

    with pytest.raises(ValueError, match=field):
        _capture(tmp_path, monkeypatch, live)
