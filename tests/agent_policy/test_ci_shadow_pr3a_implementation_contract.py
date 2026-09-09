from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-closure-pr3a-ci-hygiene.yml"
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
BASE = "bc3d56093b4adb2eae94a475f20d5e3e1721b1ec"
APPROVED_SPEC_BLOB = "bb90ad15bcf647d25d9406e3358e2be7aba00ed5"


def _payload() -> dict[str, object]:
    parsed = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    return parsed


def _git_output(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_pr3a_implementation_contract_binds_merged_approved_base() -> None:
    payload = _payload()
    metadata = SPEC.read_text(encoding="utf-8").split("## Executive summary", maxsplit=1)[0]

    assert payload["base_commit"] == BASE
    assert "- Status: APPROVED" in metadata
    assert _git_output("rev-parse", f"{BASE}:docs/feature-design-ci-shadow-pr3a-ci-hygiene.md") == APPROVED_SPEC_BLOB
    assert _git_output("hash-object", str(SPEC)) == APPROVED_SPEC_BLOB
    dependencies = payload["dependencies"]
    assert isinstance(dependencies, list)
    assert any(
        isinstance(item, str) and "Merged approved PR 3A security-amendment commit " + BASE in item
        for item in dependencies
    )
    assert any(
        isinstance(item, str) and "github-actions were read back exactly on 2026-08-10" in item for item in dependencies
    )


def test_pr3a_implementation_contract_delegates_only_unshared_surfaces() -> None:
    payload = _payload()
    owned = set(payload["owned_paths"])
    forbidden = set(payload["forbidden_paths"])

    assert {
        "docs/ci-cd.md",
        "docs/cicd/workflows.md",
        "tests/test_ci_shadow_pr3a_actionlint.py",
        "tests/test_ci_shadow_pr3a_pages_implementation.py",
        "tests/test_ci_shadow_pr3a_pages_guard.py",
        "tests/test_ci_shadow_pr3a_label_probe.py",
        "tests/test_ci_shadow_pr3a_label_evidence.py",
        "tests/agent_policy/test_ci_shadow_pr3a_implementation_contract.py",
    } <= owned
    assert {
        "pyproject.toml",
        "uv.lock",
        "CHANGELOG.md",
        "src",
        ".github/dependabot.yml",
        ".github/workflows",
        ".agents/policy/workflow-security.yml",
        "docs/quality-metrics.md",
        "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md",
        "tools/ci/pr3a_label_readback_evidence.py",
        "test_artifacts/ci-shadow-pr3a-live/label-readback-evidence.json",
    } <= forbidden
    assert payload["shared_file_owner"] == "/root"
    assert owned.isdisjoint(forbidden)

    integrator_owned = set(payload["integrator_owned_paths"])
    assert {
        ".github/dependabot.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/airflow-pack-compat.yml",
        ".github/workflows/airflow-pack-compat-nightly.yml",
        ".github/workflows/pages.yml",
        ".github/workflows/dependency-review.yml",
        ".agents/policy/workflow-security.yml",
        "CHANGELOG.md",
        "docs/quality-metrics.md",
        "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md",
        "tools/ci/pr3a_label_readback_evidence.py",
        "test_artifacts/agent-policy/agent_governance_gate.json",
        "test_artifacts/ci-shadow-pr3a-live/label-readback-evidence.json",
    } == integrator_owned
    assert all(
        any(path == boundary or path.startswith(boundary + "/") for boundary in forbidden) for path in integrator_owned
    )


def test_pr3a_pages_provider_observation_is_certification_only() -> None:
    payload = _payload()
    evidence = payload["certification_evidence"]["pages_deployment"]

    assert evidence["mode"] == "certification-only read-only observer; no production observer is introduced by PR 3A"
    assert evidence["command_source"] == "docs/cicd/runbooks.md#verify-pages-deployment-evidence"
    assert evidence["local_output"] == "test_artifacts/ci-shadow-pr3a-live/pages-deployment-evidence.json"
    assert "mocked tests never count as hosted PASS" in evidence["durable_binding"]


def test_pr3a_label_readback_has_durable_provider_evidence() -> None:
    payload = _payload()
    evidence = payload["certification_evidence"]["label_readback"]

    assert evidence == {
        "mode": "owner-authorized read-only github.com provider observation; no label mutation",
        "command_source": "docs/cicd/runbooks.md#collect-and-verify-pr3a-label-evidence",
        "producer": "tools/ci/pr3a_label_readback_evidence.py",
        "local_output": "test_artifacts/ci-shadow-pr3a-live/label-readback-evidence.json",
        "durable_binding": (
            "The v3 producer preserves bounded diagnostic response bytes and metadata; offline verification "
            "proves receipt integrity, live verification re-reads github.com and decides readiness only from "
            "the three exact ordered names, and the PR body plus issue 512 record the exact Git blob and file "
            "SHA-256."
        ),
    }


def test_pr3a_implementation_contract_requires_fail_closed_evidence() -> None:
    payload = _payload()
    criteria = " ".join(payload["acceptance_criteria"])
    checks = payload["required_checks"]

    assert "release-sensitive workflows remain byte-identical" in criteria
    assert "missing, stale, foreign, duplicate, rerun, partial-pagination" in criteria
    assert any("full non-live xdist suite" in command for command in checks["broad"])
    assert any("pinned actionlint 1.7.12" in command for command in checks["broad"])
    assert any("unavailable evidence is UNVERIFIED, never PASS" in command for command in checks["live"])
    assert any("verify-live" in command and "exact approved order" in command for command in checks["live"])
