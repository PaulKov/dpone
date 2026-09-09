from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-pr3a-security-amendment.yml"
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
RUNBOOK = ROOT / "docs/cicd/runbooks.md"
BASE = "dd45dd85b3c807a439f5917ad09fe147a9a8bce7"


def _payload() -> dict[str, object]:
    payload = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_security_amendment_is_path_scoped_and_precedes_implementation() -> None:
    payload = _payload()

    assert payload["base_commit"] == BASE
    assert payload["specification"] == SPEC.relative_to(ROOT).as_posix()
    assert set(payload["owned_paths"]) == {
        "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md",
        "docs/feature-design-ci-pr-gate-exact-sha-evidence.md",
        "docs/cicd/runbooks.md",
        "docs/quality-metrics.md",
        "tools/ci/pr3a_label_readback_evidence.py",
        "tests/test_ci_shadow_pr3a_contracts.py",
        "tests/test_ci_shadow_pr3a_pages_guard.py",
        "tests/test_ci_shadow_pr3a_label_evidence.py",
        "tests/agent_policy/test_ci_shadow_pr3a_security_amendment_contract.py",
        "test_artifacts/agent-policy/dpone-ci-shadow-pr3a-security-amendment.yml",
    }
    assert {
        ".github/workflows",
        ".github/dependabot.yml",
        ".agents/policy",
        "src",
        "packages",
        "CHANGELOG.md",
    } <= set(payload["forbidden_paths"])


def test_security_amendment_freezes_names_only_authority() -> None:
    payload = _payload()
    criteria = " ".join(payload["acceptance_criteria"])

    assert "exact ordered label-name projection" in criteria
    assert "diagnostic only" in criteria
    assert "controlled UNVERIFIED" in criteria
    assert "implementation PR cannot modify this amended specification" in criteria


def test_security_amendment_runbook_uses_validated_immutable_inputs() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "--pull-request <PR_NUMBER>" not in runbook
    assert 'PR_NUMBER="${PR_NUMBER:?export the positive pull-request number}"' in runbook
    assert (
        'IMPLEMENTATION_BASE="${IMPLEMENTATION_BASE:?export the exact approved 40-hex implementation base}"' in runbook
    )
    assert 'git cat-file -t "${IMPLEMENTATION_BASE}"' in runbook
    assert '[[ "${object_type}" != "commit" ]]' in runbook
    assert 'git merge-base --is-ancestor "${IMPLEMENTATION_BASE}" HEAD' in runbook
    assert "wrote verified PR3A label evidence:" in runbook


def _collect_shell_block() -> str:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    section = runbook.split("## Collect and verify PR3A label evidence", 1)[1]
    return section.split("```bash", 1)[1].split("```", 1)[0].strip()


def _git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_security_amendment_runbook_stops_before_collection_for_invalid_bases(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "dpone tests")
    _git(repo, "commit", "--allow-empty", "--quiet", "-m", "main")
    main = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "--annotate", "--message", "not a commit", "annotated", main)
    annotated_tag = _git(repo, "rev-parse", "annotated")
    tree = _git(repo, "rev-parse", f"{main}^{{tree}}")
    _git(repo, "switch", "--orphan", "unrelated")
    _git(repo, "commit", "--allow-empty", "--quiet", "-m", "unrelated")
    unrelated = _git(repo, "rev-parse", "HEAD")
    _git(repo, "switch", "--detach", "--quiet", main)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    uv_log = tmp_path / "uv-called"
    fake_uv = bin_dir / "uv"
    fake_uv.write_text('#!/bin/sh\nprintf called >"${UV_LOG}"\n', encoding="utf-8")
    fake_uv.chmod(0o755)

    expected_cases = (
        ("0" * 40, "UNVERIFIED: IMPLEMENTATION_BASE is not a local commit.\n"),
        (annotated_tag, "UNVERIFIED: IMPLEMENTATION_BASE is not a local commit.\n"),
        (tree, "UNVERIFIED: IMPLEMENTATION_BASE is not a local commit.\n"),
        (unrelated, "UNVERIFIED: IMPLEMENTATION_BASE is not an ancestor of HEAD.\n"),
    )
    for implementation_base, expected_stderr in expected_cases:
        uv_log.unlink(missing_ok=True)
        env = os.environ.copy()
        env.update(
            {
                "IMPLEMENTATION_BASE": implementation_base,
                "PATH": f"{bin_dir}:{env['PATH']}",
                "PR_NUMBER": "531",
                "UV_LOG": os.fspath(uv_log),
            }
        )
        result = subprocess.run(
            ["bash", "-c", _collect_shell_block()],
            cwd=repo,
            check=False,
            capture_output=True,
            env=env,
            text=True,
        )

        assert result.returncode == 1
        assert result.stdout == ""
        assert result.stderr == expected_stderr
        assert not uv_log.exists()
