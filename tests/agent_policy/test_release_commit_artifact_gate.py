"""Closed-artifact contracts for the checkout-free publication fence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._release_commit_artifact_gate_helpers import (
    APP_ID,
    COMMIT_SHA,
    CONTEXTS,
    POLICY,
    REPOSITORY,
    RULESET_ID,
    RULESET_PROJECTION,
    closed_files,
    context,
    gate,
    receipt,
)


def test_authorization_receipt_binds_policy_commit_and_producers(tmp_path: Path, monkeypatch: Any) -> None:
    report = closed_files(tmp_path, monkeypatch)

    authorized = gate.load_authorized_policy(
        report_path=report,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
    )

    assert authorized.ruleset_id == RULESET_ID
    assert authorized.context_names == tuple(sorted(CONTEXTS))
    assert authorized.integration_ids == {name: APP_ID for name in CONTEXTS}
    assert authorized.policy_sha256 == hashlib.sha256(POLICY).hexdigest()


@pytest.mark.parametrize("mutation", ["digest", "baseline", "projection"])
def test_authorization_ruleset_snapshot_rejects_internal_binding_tamper(
    mutation: str,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    report = closed_files(tmp_path, monkeypatch)
    snapshot_path = tmp_path / "exact_ruleset_projection.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if mutation == "digest":
        snapshot["policy_projection_sha256"] = "f" * 64
    elif mutation == "baseline":
        snapshot["privileged_baseline"]["version_id"] = 8
    else:
        snapshot["projection"]["enforcement"] = "disabled"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    authorized = gate.load_authorized_policy(
        report_path=report,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
    )

    with pytest.raises(ValueError):
        gate._authorized_ruleset_projection(  # noqa: SLF001 - closed-artifact negative contract.
            snapshot_path,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            authorized=authorized,
        )


def test_self_consistent_weakened_receipt_and_snapshot_cannot_forge_sealed_authority(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    payload = receipt()
    payload["contexts"] = [payload["contexts"][0]]
    report = closed_files(tmp_path, monkeypatch, payload)
    snapshot_path = tmp_path / "exact_ruleset_projection.json"
    forged = json.loads(snapshot_path.read_text(encoding="utf-8"))
    producer = {
        "context": payload["contexts"][0]["context"],
        "integration_id": payload["contexts"][0]["integration_id"],
    }
    forged["projection"]["required_status_checks"]["checks"] = [producer]
    forged["projection"]["rules"][0]["parameters"]["required_status_checks"] = [producer]
    forged["required_check_report_sha256"] = hashlib.sha256(report.read_bytes()).hexdigest()
    full = {**forged["projection"], "bypass_actors": [], "version_id": 7}
    forged["policy_projection_sha256"] = hashlib.sha256(
        json.dumps(full, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    snapshot_path.write_text(json.dumps(forged), encoding="utf-8")

    with pytest.raises(ValueError, match="sealed release authority"):
        gate.load_authorized_policy(
            report_path=report,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
        )


@pytest.mark.parametrize("mutation", ["policy", "commit", "producer", "duplicate"])
def test_stale_or_ambiguous_authorization_fails_closed(
    mutation: str,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    payload = receipt()
    if mutation == "commit":
        payload["commit_sha"] = "b" * 40
    elif mutation == "producer":
        payload["contexts"][0]["integration_id"] = 0
    elif mutation == "duplicate":
        payload["contexts"].append(dict(payload["contexts"][0]))
    report = closed_files(tmp_path, monkeypatch, payload)
    if mutation == "policy":
        (tmp_path / gate.POLICY_FILENAME).write_bytes(POLICY + b"# changed\n")

    with pytest.raises(ValueError):
        gate.load_authorized_policy(
            report_path=report,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
        )


def test_fresh_gate_rejects_changed_github_app_identity(tmp_path: Path, monkeypatch: Any) -> None:
    report = closed_files(tmp_path, monkeypatch)
    output = tmp_path / "fresh.json"
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "credential-must-not-enter-receipt")
    changed = tuple(context(name, app_id=APP_ID + 1) for name in sorted(CONTEXTS))
    current = gate.release_gate.GateReport(
        "PASS",
        REPOSITORY,
        COMMIT_SHA,
        RULESET_ID,
        1,
        changed,
        (),
        hashlib.sha256(POLICY).hexdigest(),
    )
    monkeypatch.setattr(
        gate.release_gate,
        "fetch_live_snapshot",
        lambda **_kwargs: gate.release_gate.LiveSnapshot("active", (), (), RULESET_PROJECTION),
    )

    def _poll(*_args: Any, **kwargs: Any) -> Any:
        kwargs["fetcher"](
            repo=REPOSITORY,
            commit_sha=COMMIT_SHA,
            ruleset_id=RULESET_ID,
            token="credential-must-not-enter-receipt",
        )
        return current

    monkeypatch.setattr(gate.release_gate, "poll_release_gate", _poll)

    code = gate.main(
        [
            "--repo",
            REPOSITORY,
            "--commit-sha",
            COMMIT_SHA,
            "--authorization-report",
            str(report),
            "--authorization-ruleset-report",
            str(tmp_path / "exact_ruleset_projection.json"),
            "--output",
            str(output),
            "--github-token-env",
            "TEST_GITHUB_TOKEN",
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert code == 1
    assert payload["status"] == "FAIL"
    assert "credential-must-not-enter-receipt" not in output.read_text(encoding="utf-8")


def test_fresh_gate_rejects_ruleset_projection_drift(tmp_path: Path, monkeypatch: Any) -> None:
    report = closed_files(tmp_path, monkeypatch)
    output = tmp_path / "fresh.json"
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "approved-token")
    current_contexts = tuple(context(name) for name in sorted(CONTEXTS))
    current = gate.release_gate.GateReport(
        "PASS",
        REPOSITORY,
        COMMIT_SHA,
        RULESET_ID,
        1,
        current_contexts,
        (),
        hashlib.sha256(POLICY).hexdigest(),
    )
    drifted = {**RULESET_PROJECTION, "updated_at": "2026-07-19T18:37:48.596000Z"}
    monkeypatch.setattr(
        gate.release_gate,
        "fetch_live_snapshot",
        lambda **_kwargs: gate.release_gate.LiveSnapshot("active", (), (), drifted),
    )

    def _poll(*_args: Any, **kwargs: Any) -> Any:
        kwargs["fetcher"](
            repo=REPOSITORY,
            commit_sha=COMMIT_SHA,
            ruleset_id=RULESET_ID,
            token="approved-token",
        )
        return current

    monkeypatch.setattr(gate.release_gate, "poll_release_gate", _poll)

    code = gate.main(
        [
            "--repo",
            REPOSITORY,
            "--commit-sha",
            COMMIT_SHA,
            "--authorization-report",
            str(report),
            "--authorization-ruleset-report",
            str(tmp_path / "exact_ruleset_projection.json"),
            "--output",
            str(output),
            "--github-token-env",
            "TEST_GITHUB_TOKEN",
        ]
    )

    assert code == 1
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "FAIL"


@pytest.mark.parametrize("field", ["bypass_actors", "version_id"])
def test_fresh_gate_rejects_exposed_privileged_ruleset_drift(
    field: str,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    report = closed_files(tmp_path, monkeypatch)
    output = tmp_path / "fresh.json"
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "approved-token")
    current_contexts = tuple(context(name) for name in sorted(CONTEXTS))
    current = gate.release_gate.GateReport(
        "PASS",
        REPOSITORY,
        COMMIT_SHA,
        RULESET_ID,
        1,
        current_contexts,
        (),
        hashlib.sha256(POLICY).hexdigest(),
    )
    live = {**RULESET_PROJECTION, "bypass_actors": [], "version_id": 7}
    live[field] = (
        [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}] if field == "bypass_actors" else 8
    )
    monkeypatch.setattr(
        gate.release_gate,
        "fetch_live_snapshot",
        lambda **_kwargs: gate.release_gate.LiveSnapshot("active", (), (), live),
    )

    def _poll(*_args: Any, **kwargs: Any) -> Any:
        kwargs["fetcher"](
            repo=REPOSITORY,
            commit_sha=COMMIT_SHA,
            ruleset_id=RULESET_ID,
            token="approved-token",
        )
        return current

    monkeypatch.setattr(gate.release_gate, "poll_release_gate", _poll)

    code = gate.main(
        [
            "--repo",
            REPOSITORY,
            "--commit-sha",
            COMMIT_SHA,
            "--authorization-report",
            str(report),
            "--authorization-ruleset-report",
            str(tmp_path / "exact_ruleset_projection.json"),
            "--output",
            str(output),
            "--github-token-env",
            "TEST_GITHUB_TOKEN",
        ]
    )

    assert code == 1
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "FAIL"
