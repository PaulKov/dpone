from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_similarweb_jobs_and_manual_only_live_rules() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()
    rules = data[".rules_similarweb_live_main_manual"]["rules"]

    assert len(rules) == 3
    assert "schedule" not in "\n".join(rule["if"] for rule in rules if "if" in rule)
    assert "merge_request_event" in "\n".join(rule["if"] for rule in rules if "if" in rule)

    mock_job = data["integration_similarweb_mock"]
    assert mock_job["stage"] == "integration-test"
    assert ".rules_main_and_mr" in mock_job["extends"]
    assert "tests/integration/similarweb/test_similarweb_mock_integration.py" in "\n".join(mock_job["script"])

    live_job = data["integration_similarweb_live"]
    assert live_job["stage"] == "integration-test"
    assert ".rules_similarweb_live_main_manual" in live_job["extends"]
    assert live_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert live_job["variables"]["DPONE_IT_SW_DOMAINS"] == "travel.example.com"
    assert live_job["variables"]["DPONE_IT_SW_LIMIT"] == "5"
    assert live_job["variables"]["DPONE_IT_SW_PAGE_SIZE"] == "5"
    assert live_job["variables"]["DPONE_IT_SW_MIN_KEYWORDS_COUNT"] == "1"
    assert "tests/integration/similarweb" in "\n".join(live_job["script"])

    snapshot_job = data["snapshot_similarweb_live_smoke"]
    assert snapshot_job["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_job["extends"]
    assert ".rules_similarweb_live_main_manual" in snapshot_job["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_job["needs"])
    assert snapshot_job["variables"]["DPONE_IT_SW_DOMAINS"] == "travel.example.com"
    assert snapshot_job["variables"]["DPONE_IT_SW_LIMIT"] == "5"
    assert snapshot_job["variables"]["DPONE_IT_SW_PAGE_SIZE"] == "5"
    assert snapshot_job["variables"]["DPONE_IT_SW_MIN_KEYWORDS_COUNT"] == "1"
    script = "\n".join(snapshot_job["script"])
    assert "python tools/ci/vault_jwt_preflight.py" in script
    assert "dpone-runtime-exec python tools/similarweb_live_smoke.py" in script
    assert "[vault,gcp]==" in script
    assert "VAULT_ADDR" in text
    assert "VAULT_AUTH_ROLE_DEV" in text
    assert "VAULT_JWT_AUDIENCE" in text
    assert "DPONE_IT_SW_DOMAINS" in text


def test_similarweb_dev_install_examples_exist_and_contain_required_keys() -> None:
    env_example = ROOT / "examples" / "dev_install" / "similarweb_snapshot.env.example"
    commands = ROOT / "examples" / "dev_install" / "similarweb_command_examples.md"
    tool = ROOT / "tools" / "similarweb_live_smoke.py"

    assert env_example.exists()
    assert commands.exists()
    assert tool.exists()

    text = env_example.read_text(encoding="utf-8")
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URL=https://pypi.org/simple" in text
    assert ("DPONE_PACKAGE_EXTRA_INDEX_URLS=https://pypi.org/simple") in text
    assert "ENV_CODE=dev" in text
    assert "DPONE_IT_SW_VAULT_PATH=api/similarweb" in text
    assert "DPONE_IT_SW_DOMAINS=travel.example.com" in text
