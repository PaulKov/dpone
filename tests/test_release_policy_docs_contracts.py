"""Keep operator and agent entrypoints on the approved PyPI-only authority."""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


@pytest.mark.parametrize("workflow", ["pypi-release.yml", "pypi-rehearsal.yml"])
def test_copyable_controller_dispatch_is_version_only(workflow: str) -> None:
    guide = _read("docs/release.md")
    commands = re.findall(r"```bash\n(.*?)```", guide, flags=re.DOTALL)
    dispatches = [block for block in commands if f"gh workflow run {workflow}" in block]
    assert len(dispatches) == 1
    command = dispatches[0].replace("\\\n", " ")
    assert "--repo PaulKov/dpone-release-controller" in command
    assert "--ref master" in command
    assert re.findall(r"-f\s+(\w+)=", command) == ["version"]
    assert "version=X.Y.Z" in command


@pytest.mark.parametrize(
    "path",
    ["docs/release.md", "docs/cicd/release-and-pages.md", "docs/cicd/runbooks.md"],
)
def test_current_guides_do_not_restore_old_publisher_or_enable_upload_retries(path: str) -> None:
    guide = _read(path)
    assert "Add or restore" not in guide
    assert "skip-existing: true" not in guide
    assert "pypi-release.yml" in guide
    assert "retro_pypi_verification" in guide


def test_release_report_separates_readiness_from_publication_observation() -> None:
    report = _read("docs/agent-templates/release-evidence-report.md")
    for field in (
        "Requested operation:",
        "Controller commit:",
        "Controller run ID / attempt:",
        "Readiness decision:",
        "Publication observation:",
        "Scope / applicability",
        "retro_pypi_verification.json",
    ):
        assert field in report


def test_retrospective_recovery_preserves_successful_run_requirement() -> None:
    guide = " ".join(_read("docs/release.md").split())
    assert "successful overall controller run" in guide
    assert "cannot turn it into a `PASS`" in guide
    assert "separate approved controller change" in guide
    assert "OBSERVATION_ID/retro_pypi_verification.json" in guide
    assert "do not reuse a prior observation path" in guide


def test_agent_routes_preserve_read_only_audit_and_retrospective_mode() -> None:
    role = tomllib.loads(_read(".codex/agents/dpone-release-auditor.toml"))
    assert role["sandbox_mode"] == "read-only"
    assert "retrospective" in role["developer_instructions"]
    skill = _read(".agents/skills/prepare-dpone-release/SKILL.md")
    assert "retrospective" in skill
    assert "separate publication authorization" in skill
    assert "docs/release.md" in _read("AGENTS.md")


def test_release_intake_requires_operation_and_controller_identity() -> None:
    template = yaml.safe_load(_read(".github/ISSUE_TEMPLATE/release_readiness.yml"))
    fields = {field.get("id"): field for field in template["body"]}
    assert fields["operation"]["validations"]["required"] is True
    assert fields["controller"]["validations"]["required"] is True
    assert "retrospective verification" in fields["operation"]["attributes"]["options"]


def test_handoff_and_evidence_docs_do_not_override_current_publication_authority() -> None:
    handoff = _read("docs/feature-specs/single-pypi-publisher-handoff.md")
    assert "superseded" in handoff
    assert "oidc-pypi-release-controller.md" in handoff
    evidence = _read("docs/release-evidence.md")
    assert "## Canonical pre-tag workflow" in evidence  # Preserve inbound links.
    assert "Legacy tagged-release contract" in evidence
    assert "not the ordinary PyPI publication gate" in evidence
