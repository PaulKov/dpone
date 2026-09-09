from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_quality_and_snapshot_jobs() -> None:
    data = load_gitlab_ci_config()
    for job_name in [
        "lint_ruff",
        "typecheck_mypy",
        "unit_tests",
        "quality_gates",
        "wheel_smoke_test",
        "publish_dev_snapshot",
        "snapshot_install_smoke",
    ]:
        assert job_name in data


def test_ci_stage_order_runs_pre_test_before_build() -> None:
    data = load_gitlab_ci_config()
    stages = data["stages"]
    assert stages.index("pre-test") < stages.index("build")


def test_ci_snapshot_publish_uses_version_tool_and_dotenv() -> None:
    text = load_gitlab_ci_text()
    assert (
        'SNAPSHOT_VER="$(python tools/compute_snapshot_version.py --pyproject pyproject.toml --pipeline-iid "$CI_PIPELINE_IID")"'
        in text
    )
    assert "tools/compute_snapshot_version.py" in text
    assert "reports:\n      dotenv: snapshot.env" in text
    assert "DPONE_PACKAGE_SPEC" in text
    assert "tools/package_smoke.py" in text


def test_publish_dev_snapshot_rules_cover_master_snapshot_smokes() -> None:
    data = load_gitlab_ci_config()

    publish_job = data["publish_dev_snapshot"]
    assert ".rules_snapshot_publish" in publish_job["extends"]

    rules = data[".rules_snapshot_publish"]["rules"]
    assert any(
        rule.get("if")
        == '$CI_PIPELINE_SOURCE == "schedule" && ($CI_COMMIT_BRANCH == "master" || $CI_COMMIT_BRANCH == "develop")'
        and rule.get("when") == "on_success"
        for rule in rules
    )
    assert any(
        rule.get("if") == '$CI_COMMIT_BRANCH == "master"'
        and rule.get("when") == "manual"
        and rule.get("allow_failure") is True
        for rule in rules
    )


def test_uv_job_template_uses_pull_only_cache_policy() -> None:
    data = load_gitlab_ci_config()
    template = data[".uv_job_template"]
    assert template["cache"]["policy"] == "pull"
    assert template["cache"]["key"] == "uv-${CI_PROJECT_ID}"
    assert template["cache"]["paths"] == [".cache/uv/"]


def test_unit_tests_job_targets_tests_directory_and_has_preflight_guard() -> None:
    data = load_gitlab_ci_config()
    unit_job = data["unit_tests"]
    script = "\n".join(unit_job["script"])
    assert "uv run pytest tests --ignore=tests/integration -ra --junitxml=reports/unit-tests.xml" in script
    assert "mkdir -p reports" in script
    assert "find tests -type f -name 'test_*.py' -print -quit" in script
    assert "tests/ is missing or does not contain pytest test files" in script
    assert unit_job["artifacts"]["reports"]["junit"] == "reports/unit-tests.xml"
    assert "reports/unit-tests.xml" in unit_job["artifacts"]["paths"]
