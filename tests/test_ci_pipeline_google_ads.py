from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_google_ads_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()
    vault_rules = cast(dict[str, Any], data[".rules_vault_live_schedule_main_manual"])["rules"]

    google_ads_template = cast(dict[str, Any], data[".uv_dev_vault_google_ads_job_template"])
    assert ".vault_jwt_job_template" in google_ads_template["extends"]
    assert "uv sync --group dev --extra vault --extra google_ads -P vault-kv-client" in "\n".join(
        google_ads_template["before_script"]
    )

    mock_job = cast(dict[str, Any], data["integration_google_ads_mock"])
    assert mock_job["stage"] == "integration-test"
    assert ".rules_main_and_mr" in mock_job["extends"]
    assert "tests/integration/google_ads/test_google_ads_mock_integration.py" in "\n".join(mock_job["script"])

    live_job = cast(dict[str, Any], data["integration_google_ads_live"])
    assert live_job["stage"] == "integration-test"
    assert ".uv_dev_vault_google_ads_job_template" in live_job["extends"]
    assert ".rules_vault_live_schedule_main_manual" in live_job["extends"]
    assert live_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert "tests/integration/google_ads" in "\n".join(live_job["script"])
    assert len(vault_rules) == 4
    assert "merge_request_event" in "\n".join(rule["if"] for rule in vault_rules if "if" in rule)

    snapshot_job = cast(dict[str, Any], data["snapshot_google_ads_live_smoke"])
    assert snapshot_job["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_job["extends"]
    assert ".rules_vault_live_schedule_main_manual" in snapshot_job["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_job["needs"])
    script = "\n".join(snapshot_job["script"])
    assert "tools/google_ads_live_smoke.py" in script
    assert "[vault,gcp,google_ads]==" in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URL="$PUBLIC_PYPI_SIMPLE"' in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URLS="https://pypi.org/simple"' in script
    assert "VAULT_ADDR" in text
    assert "VAULT_AUTH_ROLE_DEV" in text
    assert "VAULT_AUTH_ROLE_PROD" in text
    assert "VAULT_JWT_AUDIENCE" in text
    assert "VAULT_ID_TOKEN" in text
    assert 'test -n "${VAULT_ID_TOKEN:-}"' in script
    assert 'test -n "${VAULT_AUTH_ROLE:-}"' in script
    assert "python tools/ci/vault_jwt_preflight.py" in script


def test_google_ads_dev_install_examples_exist_and_contain_required_keys() -> None:
    env_example = ROOT / "examples" / "dev_install" / "google_ads_snapshot.env.example"
    commands = ROOT / "examples" / "dev_install" / "google_ads_command_examples.md"
    tool = ROOT / "tools" / "google_ads_live_smoke.py"
    docs = ROOT / "docs" / "google-ads.md"

    assert env_example.exists()
    assert commands.exists()
    assert tool.exists()
    assert docs.exists()

    text = env_example.read_text(encoding="utf-8")
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp,google_ads]==" in text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URL=https://pypi.org/simple" in text
    assert ("DPONE_PACKAGE_EXTRA_INDEX_URLS=https://pypi.org/simple") in text
    assert "ENV_CODE=dev" in text
    assert "DPONE_IT_GA_VAULT_PATH=api/google_ads" in text
    assert "DPONE_IT_GA_RESOURCE=ads_stats" in text
    assert "DPONE_IT_GA_SELECTORS=app.ads_stats" in text
