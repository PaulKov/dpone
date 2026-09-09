from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_cbr_and_mindbox_live_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()
    vault_rules = data[".rules_vault_live_schedule_main_manual"]["rules"]

    cbr_job = data["integration_cbr_live_public"]
    assert cbr_job["stage"] == "integration-test"
    assert cbr_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert "tests/integration/cbr/test_cbr_live_integration.py" in "\n".join(cbr_job["script"])

    mindbox_job = data["integration_mindbox_live"]
    assert mindbox_job["stage"] == "integration-test"
    assert ".rules_vault_live_schedule_main_manual" in mindbox_job["extends"]
    assert mindbox_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert mindbox_job["variables"]["DPONE_IT_MB_EXPORT_TIMEOUT"] == "3000"
    assert "tests/integration/mindbox" in "\n".join(mindbox_job["script"])
    assert len(vault_rules) == 4
    assert "merge_request_event" in "\n".join(rule["if"] for rule in vault_rules if "if" in rule)

    snapshot_cbr = data["snapshot_cbr_public_smoke"]
    assert snapshot_cbr["stage"] == "smoke-test"
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_cbr["needs"])
    assert "tools/cbr_public_smoke.py" in "\n".join(snapshot_cbr["script"])

    snapshot_mindbox = data["snapshot_mindbox_live_smoke"]
    assert snapshot_mindbox["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_mindbox["extends"]
    assert ".rules_vault_live_schedule_main_manual" in snapshot_mindbox["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_mindbox["needs"])
    assert snapshot_mindbox["variables"]["DPONE_IT_MB_EXPORT_TIMEOUT"] == "3000"
    script = "\n".join(snapshot_mindbox["script"])
    assert "tools/mindbox_live_smoke.py" in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URL="$PUBLIC_PYPI_SIMPLE"' in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URLS="https://pypi.org/simple"' in script
    assert "VAULT_ADDR" in text
    assert "VAULT_AUTH_METHOD" in text
    assert "VAULT_AUTH_ROLE_DEV" in text
    assert "VAULT_AUTH_ROLE_PROD" in text
    assert "VAULT_JWT_AUDIENCE" in text
    assert "VAULT_ID_TOKEN" in text
    assert 'test -n "${VAULT_ID_TOKEN:-}"' in script
    assert 'test -n "${VAULT_AUTH_ROLE:-}"' in script
    assert "python tools/ci/vault_jwt_preflight.py" in script
    assert "tools/ci/vault_login_from_gitlab_jwt.py" not in script
    assert "dpone-mindbox-live" not in text


def test_dev_install_examples_exist_for_cbr_and_mindbox() -> None:
    assert (ROOT / "examples" / "dev_install" / "mindbox_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "mindbox_command_examples.md").exists()
    assert (ROOT / "examples" / "dev_install" / "cbr_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "cbr_command_examples.md").exists()
    assert (ROOT / "tools" / "mindbox_live_smoke.py").exists()
    assert (ROOT / "tools" / "cbr_public_smoke.py").exists()


def test_cbr_and_mindbox_examples_include_key_smoke_prerequisites() -> None:
    mindbox_text = (ROOT / "examples" / "dev_install" / "mindbox_snapshot.env.example").read_text(encoding="utf-8")
    cbr_text = (ROOT / "examples" / "dev_install" / "cbr_snapshot.env.example").read_text(encoding="utf-8")

    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in mindbox_text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URL=https://pypi.org/simple" in mindbox_text
    assert ("DPONE_PACKAGE_EXTRA_INDEX_URLS=https://pypi.org/simple") in mindbox_text
    assert "ENV_CODE=dev" in mindbox_text
    assert "DPONE_IT_MB_VAULT_PATH=api/mindbox" in mindbox_text
    assert "DPONE_IT_MB_EXPORT_TIMEOUT=3000" in mindbox_text

    assert "DPONE_PACKAGE_SPEC=dpone==" in cbr_text
    assert "ENV_CODE=dev" in cbr_text
    assert "DPONE_IT_CBR_RESOURCE=xml_daily_asp" in cbr_text
