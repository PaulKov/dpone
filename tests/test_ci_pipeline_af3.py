from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_declares_integration_live_marker() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    markers = data["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("integration_live:") for marker in markers)


def test_ci_has_live_appsflyer_jobs() -> None:
    data = load_gitlab_ci_config()
    text = load_gitlab_ci_text()
    uv_template = data[".uv_job_template"]
    vault_template = data[".vault_jwt_job_template"]
    uv_vault_template = data[".uv_dev_vault_job_template"]
    appsflyer_rules = data[".rules_appsflyer_live_schedule_main_manual"]["rules"]

    assert "UV_INDEX_VAULT_CLIENT_USERNAME" not in uv_template.get("variables", {})
    assert "UV_INDEX_VAULT_CLIENT_PASSWORD" not in uv_template.get("variables", {})
    assert vault_template["id_tokens"]["VAULT_ID_TOKEN"]["aud"] == "$VAULT_JWT_AUDIENCE"
    assert vault_template["variables"]["VAULT_AUTH_METHOD"] == "jwt"
    assert vault_template["variables"]["VAULT_AUTH_PATH"] == "jwt_v2"
    assert vault_template["variables"]["ENV_CODE"] == "dev"
    assert ".vault_jwt_job_template" in uv_vault_template["extends"]
    before_script = "\n".join(uv_vault_template["before_script"])
    assert 'case "${ENV_CODE:-dev}"' in before_script
    assert 'test -n "${VAULT_ID_TOKEN:-}"' in before_script
    assert 'test -n "${VAULT_AUTH_ROLE:-}"' in before_script
    assert "python tools/ci/vault_jwt_preflight.py" in before_script
    assert "uv sync --group dev --extra vault -P vault-kv-client" in before_script
    assert "tools/ci/vault_login_from_gitlab_jwt.py" not in before_script
    assert "VAULT_AUTH_ROLE_DEV" in text
    assert "VAULT_AUTH_ROLE_PROD" in text
    assert len(appsflyer_rules) == 4
    assert "merge_request_event" in "\n".join(rule["if"] for rule in appsflyer_rules if "if" in rule)

    live_job = data["integration_appsflyer_live"]
    assert live_job["stage"] == "integration-test"
    assert ".rules_appsflyer_live_schedule_main_manual" in live_job["extends"]
    assert live_job["variables"]["DPONE_RUN_INTEGRATION_LIVE"] == "1"
    assert "pytest -m integration_live tests/integration/appsflyer" in "\n".join(live_job["script"])

    snapshot_job = data["snapshot_appsflyer_live_smoke"]
    assert snapshot_job["stage"] == "smoke-test"
    assert ".vault_jwt_job_template" in snapshot_job["extends"]
    assert ".rules_appsflyer_live_schedule_main_manual" in snapshot_job["extends"]
    assert any(need["job"] == "publish_dev_snapshot" for need in snapshot_job["needs"])
    script = "\n".join(snapshot_job["script"])
    assert "python tools/ci/vault_jwt_preflight.py" in script
    assert "dpone-runtime-exec python tools/appsflyer_live_smoke.py" in script
    assert "[vault,gcp]==" in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URL="$PUBLIC_PYPI_SIMPLE"' in script
    assert 'DPONE_PACKAGE_EXTRA_INDEX_URLS="https://pypi.org/simple"' in script
    assert "VAULT_ADDR" in text
    assert "VAULT_AUTH_METHOD" in text
    assert "VAULT_AUTH_ROLE_DEV" in text
    assert "VAULT_AUTH_ROLE_PROD" in text
    assert "VAULT_JWT_AUDIENCE" in text
    assert "VAULT_ID_TOKEN" in text
    assert "DPONE_IT_AF_APP_IDS" in text
    assert "tools/ci/vault_jwt_preflight.py" in text
    assert "tools/ci/vault_login_from_gitlab_jwt.py" not in text
    assert "dpone-appsflyer-live" not in text
    assert "dpone-mindbox-live" not in text
    assert "dpone-fasttrack-live" not in text


def test_examples_include_appsflyer_dev_install_files() -> None:
    assert (ROOT / "examples" / "dev_install" / "appsflyer_snapshot.env.example").exists()
    assert (ROOT / "examples" / "dev_install" / "appsflyer_command_examples.md").exists()
    assert (ROOT / "tools" / "appsflyer_live_smoke.py").exists()


def test_appsflyer_dev_install_example_contains_required_live_smoke_keys() -> None:
    text = (ROOT / "examples" / "dev_install" / "appsflyer_snapshot.env.example").read_text(encoding="utf-8")
    assert "DPONE_PACKAGE_SPEC=dpone[vault,gcp]==" in text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URL=https://pypi.org/simple" in text
    assert "DPONE_PACKAGE_EXTRA_INDEX_URLS=https://pypi.org/simple" in text
    assert "ENV_CODE=dev" in text
    assert "DPONE_IT_AF_APP_IDS=" in text
    assert "DPONE_IT_AF_VAULT_PATH=api/appsflyer" in text
