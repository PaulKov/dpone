from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_openexchangerates_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()

    mock_job = data["integration_openexchangerates_mock"]
    assert mock_job["stage"] == "integration-test"
    assert ".rules_main_and_mr" in mock_job["extends"]
    assert "tests/integration/openexchangerates/test_openexchangerates_mock_integration.py" in "\n".join(
        mock_job["script"]
    )

    live_job = data["integration_openexchangerates_live"]
    assert live_job["stage"] == "integration-test"
    assert ".rules_vault_live_schedule_main_manual" in live_job["extends"]
    assert live_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert live_job["variables"]["DPONE_IT_OXR_SYMBOLS"] == "ARS,RUB,EUR"
    assert "tests/integration/openexchangerates" in "\n".join(live_job["script"])

    snapshot_job = data["snapshot_openexchangerates_live_smoke"]
    assert snapshot_job["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_job["extends"]
    assert ".rules_vault_live_schedule_main_manual" in snapshot_job["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_job["needs"])
    assert snapshot_job["variables"]["DPONE_IT_OXR_SYMBOLS"] == "ARS,RUB,EUR"
    script = "\n".join(snapshot_job["script"])
    assert "tools/openexchangerates_live_smoke.py" in script
    assert "[vault,gcp]==" in script
    assert "VAULT_ADDR" in text


def test_openexchangerates_dev_install_examples_exist_and_contain_required_keys() -> None:
    env_example = ROOT / "examples" / "dev_install" / "openexchangerates_snapshot.env.example"
    commands = ROOT / "examples" / "dev_install" / "openexchangerates_command_examples.md"
    tool = ROOT / "tools" / "openexchangerates_live_smoke.py"

    assert env_example.exists()
    assert commands.exists()
    assert tool.exists()

    text = env_example.read_text(encoding="utf-8")
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in text
    assert "ENV_CODE=dev" in text
    assert "DPONE_IT_OXR_VAULT_PATH=api/openexchangerates" in text
    assert "DPONE_IT_OXR_SYMBOLS=ARS,RUB,EUR" in text
