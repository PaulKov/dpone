from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_google_sheets_and_yandex_webmaster_mock_jobs() -> None:
    data = load_gitlab_ci_config()

    google_job = data["integration_google_sheets_mock"]
    assert google_job["stage"] == "integration-test"
    assert ".rules_main_and_mr" in google_job["extends"]
    assert "tests/integration/google_sheets/test_google_sheets_mock_integration.py" in "\n".join(google_job["script"])

    yandex_job = data["integration_yandex_webmaster_mock"]
    assert yandex_job["stage"] == "integration-test"
    assert ".rules_main_and_mr" in yandex_job["extends"]
    assert "tests/integration/yandex_webmaster/test_yandex_webmaster_mock_integration.py" in "\n".join(
        yandex_job["script"]
    )


def test_ci_has_google_sheets_and_yandex_webmaster_live_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()

    gcp_template = data[".uv_dev_vault_gcp_job_template"]
    assert ".vault_jwt_job_template" in gcp_template["extends"]
    assert "uv sync --group dev --extra vault --extra gcp -P vault-kv-client" in "\n".join(
        gcp_template["before_script"]
    )

    google_rules = data[".rules_google_sheets_live_main_manual"]["rules"]
    assert len(google_rules) == 3
    assert "schedule" not in "\n".join(rule["if"] for rule in google_rules if "if" in rule)
    assert "DPONE_IT_GS_SPREADSHEET_ID" in "\n".join(rule["if"] for rule in google_rules if "if" in rule)
    assert "DPONE_IT_GS_SPREADSHEET_URL" in "\n".join(rule["if"] for rule in google_rules if "if" in rule)

    yandex_rules = data[".rules_yandex_webmaster_live_main_manual"]["rules"]
    assert len(yandex_rules) == 3
    assert "schedule" not in "\n".join(rule["if"] for rule in yandex_rules if "if" in rule)

    google_live = data["integration_google_sheets_live"]
    assert ".uv_dev_vault_gcp_job_template" in google_live["extends"]
    assert ".rules_google_sheets_live_main_manual" in google_live["extends"]
    assert google_live["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert 'test -n "${DPONE_IT_GS_SPREADSHEET_ID:-}${DPONE_IT_GS_SPREADSHEET_URL:-}"' in "\n".join(
        google_live["script"]
    )
    assert "tests/integration/google_sheets" in "\n".join(google_live["script"])

    yandex_live = data["integration_yandex_webmaster_live"]
    assert ".uv_dev_vault_job_template" in yandex_live["extends"]
    assert ".rules_yandex_webmaster_live_main_manual" in yandex_live["extends"]
    assert yandex_live["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert yandex_live["variables"]["DPONE_IT_YANDEX_WEBMASTER_HOST_ID"] == "https:travel.example.com:443"
    assert "tests/integration/yandex_webmaster" in "\n".join(yandex_live["script"])

    google_smoke = data["snapshot_google_sheets_live_smoke"]
    assert ".vault_jwt_job_template" in google_smoke["extends"]
    assert ".rules_google_sheets_live_main_manual" in google_smoke["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in google_smoke["needs"])
    google_smoke_script = "\n".join(google_smoke["script"])
    assert "tools/google_sheets_live_smoke.py" in google_smoke_script
    assert "[vault,gcp]==" in google_smoke_script

    yandex_smoke = data["snapshot_yandex_webmaster_live_smoke"]
    assert ".vault_jwt_job_template" in yandex_smoke["extends"]
    assert ".rules_yandex_webmaster_live_main_manual" in yandex_smoke["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in yandex_smoke["needs"])
    assert yandex_smoke["variables"]["DPONE_IT_YANDEX_WEBMASTER_HOST_ID"] == "https:travel.example.com:443"
    yandex_smoke_script = "\n".join(yandex_smoke["script"])
    assert "tools/yandex_webmaster_live_smoke.py" in yandex_smoke_script
    assert "[vault,gcp]==" in yandex_smoke_script

    assert "DPONE_IT_GS_SPREADSHEET_ID" in text
    assert "DPONE_IT_GS_SPREADSHEET_URL" in text
    assert "DPONE_IT_YANDEX_WEBMASTER_HOST_ID" in text


def test_google_sheets_and_yandex_webmaster_docs_examples_and_live_tools_exist() -> None:
    assert (ROOT / "docs" / "google-sheets.md").exists()
    assert (ROOT / "docs" / "yandex-webmaster.md").exists()
    assert (ROOT / "examples" / "batch" / "landing_google_sheets_api.batch.yaml").exists()
    assert (ROOT / "examples" / "batch" / "landing_yandex_webmaster_api.batch.yaml").exists()
    assert (ROOT / "tools" / "google_sheets_live_smoke.py").exists()
    assert (ROOT / "tools" / "yandex_webmaster_live_smoke.py").exists()
    assert (ROOT / "examples" / "dev_install" / "google_sheets_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "google_sheets_command_examples.md").exists()
    assert (ROOT / "examples" / "dev_install" / "yandex_webmaster_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "yandex_webmaster_command_examples.md").exists()

    google_env = (ROOT / "examples" / "dev_install" / "google_sheets_snapshot.env.example").read_text(encoding="utf-8")
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in google_env
    assert "DPONE_IT_GS_VAULT_PATH=api/google_sheets" in google_env
    assert "DPONE_IT_GS_SPREADSHEET_URL=" in google_env

    yandex_env = (ROOT / "examples" / "dev_install" / "yandex_webmaster_snapshot.env.example").read_text(
        encoding="utf-8"
    )
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in yandex_env
    assert "DPONE_IT_YANDEX_WEBMASTER_VAULT_PATH=api/yandex_webmaster" in yandex_env
    assert "DPONE_IT_YANDEX_WEBMASTER_HOST_ID=https:travel.example.com:443" in yandex_env
