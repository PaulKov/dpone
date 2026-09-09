from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_fasttrack_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()
    vault_rules = data[".rules_vault_live_schedule_main_manual"]["rules"]

    mock_job = data["integration_fasttrack_mock"]
    assert mock_job["stage"] == "integration-test"
    assert "tests/integration/fasttrack/test_fasttrack_mock_integration.py" in "\n".join(mock_job["script"])

    live_job = data["integration_fasttrack_live"]
    assert live_job["stage"] == "integration-test"
    assert ".rules_vault_live_schedule_main_manual" in live_job["extends"]
    assert live_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert "tests/integration/fasttrack/test_fasttrack_live_integration.py" in "\n".join(live_job["script"])
    assert len(vault_rules) == 4
    assert "merge_request_event" in "\n".join(rule["if"] for rule in vault_rules if "if" in rule)

    snapshot_job = data["snapshot_fasttrack_live_smoke"]
    assert snapshot_job["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_job["extends"]
    assert ".rules_vault_live_schedule_main_manual" in snapshot_job["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_job["needs"])
    script = "\n".join(snapshot_job["script"])
    assert "tools/fasttrack_live_smoke.py" in script
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
    assert "dpone-fasttrack-live" not in text


def test_fasttrack_dev_install_examples_exist() -> None:
    assert (ROOT / "examples" / "dev_install" / "fasttrack_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "fasttrack_command_examples.md").exists()
    assert (ROOT / "tools" / "fasttrack_live_smoke.py").exists()


def test_fasttrack_dev_install_example_contains_snapshot_live_smoke_keys() -> None:
    env_text = (ROOT / "examples" / "dev_install" / "fasttrack_snapshot.env.example").read_text(encoding="utf-8")
    cmd_text = (ROOT / "examples" / "dev_install" / "fasttrack_command_examples.md").read_text(encoding="utf-8")

    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in env_text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URL=https://pypi.org/simple" in env_text
    assert ("DPONE_PACKAGE_EXTRA_INDEX_URLS=https://pypi.org/simple") in env_text
    assert "ENV_CODE=dev" in env_text
    assert "DPONE_IT_FT_VAULT_PATH=api/fasttrack" in env_text
    assert "DPONE_IT_FT_TIMEOUT=60" in env_text
    assert "source examples/dev_install/fasttrack_snapshot.env.example" in cmd_text
    assert '--project-root "$DPONE_PROJECT_DIR"' in cmd_text
