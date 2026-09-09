from __future__ import annotations

from pathlib import Path

import yaml

from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter

ROOT = Path(__file__).resolve().parents[1]


def _load_manifest(path: Path):
    loader = ConfigLoader(path.parent, manifest_loader=ManifestLoaderRouter())
    return loader.get_manifest(path, metadata_only=True)


def test_mindbox_rollout_manifest_exists_and_loads() -> None:
    manifest_path = ROOT / "examples" / "batch" / "landing_mindbox_api.batch.yaml"
    assert manifest_path.exists()
    manifest = _load_manifest(manifest_path)
    assert manifest is not None
    assert len(manifest.processes) == 6
    tables = {proc.config.load_config.target_table for proc in manifest.processes}
    assert "app__getactions" in tables
    assert "app__operationslogs" in tables


def test_cbr_rollout_manifest_exists_and_loads() -> None:
    manifest_path = ROOT / "examples" / "batch" / "landing_cbr_api.batch.yaml"
    assert manifest_path.exists()
    manifest = _load_manifest(manifest_path)
    assert manifest is not None
    assert len(manifest.processes) == 1
    assert manifest.processes[0].config.load_config.target_table == "app__xml_daily_asp"


def test_fasttrack_rollout_manifest_exists_and_loads() -> None:
    manifest_path = ROOT / "examples" / "batch" / "landing_fasttrack_api.batch.yaml"
    assert manifest_path.exists()
    manifest = _load_manifest(manifest_path)
    assert manifest is not None
    assert len(manifest.processes) == 3
    tables = {proc.config.load_config.target_table for proc in manifest.processes}
    assert "default__cascade_transactions" in tables
    assert "default__chat_sessions" in tables
    assert "default__flex_cms_ratings" in tables


def test_openexchangerates_rollout_manifest_exists_and_loads() -> None:
    manifest_path = ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml"
    assert manifest_path.exists()
    manifest = _load_manifest(manifest_path)
    assert manifest is not None
    assert len(manifest.processes) == 1
    process = manifest.processes[0].config.load_config
    assert process.target_schema == "landing__openexchangerates__api"
    assert process.target_table == "default__historical_rates_daily"


def test_similarweb_rollout_manifest_exists_and_loads() -> None:
    manifest_path = ROOT / "examples" / "batch" / "landing_similarweb_api.batch.yaml"
    assert manifest_path.exists()
    manifest = _load_manifest(manifest_path)
    assert manifest is not None
    assert len(manifest.processes) == 1
    process = manifest.processes[0].config.load_config
    assert process.target_schema == "landing__similarweb__api"
    assert process.target_table == "default__keywords"


def test_fasttrack_rollout_values_define_schedule_runtime_wrapper_and_vault() -> None:
    payload = yaml.safe_load((ROOT / "deploy" / "argo" / "fasttrack" / "values.base.yaml").read_text(encoding="utf-8"))
    assert payload["fasttrack"]["schedule"] == "0 1 * * *"
    assert payload["fasttrack"]["selectors"] == [
        "default.cascade_transactions",
        "default.chat_sessions",
        "default.flex_cms_ratings",
    ]
    assert payload["fasttrack"]["vaultPath"] == "api/fasttrack"
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_mindbox_rollout_values_define_schedule_runtime_wrapper_and_vault() -> None:
    payload = yaml.safe_load((ROOT / "deploy" / "argo" / "mindbox" / "values.base.yaml").read_text(encoding="utf-8"))
    assert payload["mindbox"]["schedule"] == "20 4 * * *"
    assert "app.getactions" in payload["mindbox"]["selectors"]
    assert payload["mindbox"]["vaultPath"] == "api/mindbox"
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_similarweb_rollout_values_define_schedule_runtime_wrapper_and_vault() -> None:
    payload = yaml.safe_load((ROOT / "deploy" / "argo" / "similarweb" / "values.base.yaml").read_text(encoding="utf-8"))
    assert payload["similarweb"]["schedule"] == "0 3 3 * *"
    assert payload["similarweb"]["selectors"] == ["default.keywords"]
    assert payload["similarweb"]["vaultPath"] == "api/similarweb"
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_openexchangerates_rollout_values_define_schedule_runtime_wrapper_and_vault() -> None:
    payload = yaml.safe_load(
        (ROOT / "deploy" / "argo" / "openexchangerates" / "values.base.yaml").read_text(encoding="utf-8")
    )
    assert payload["openexchangerates"]["schedule"] == "5 4 * * *"
    assert payload["openexchangerates"]["selectors"] == ["default.historical_rates_daily"]
    assert payload["openexchangerates"]["vaultPath"] == "api/openexchangerates"
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_cbr_rollout_values_define_schedule_runtime_wrapper() -> None:
    payload = yaml.safe_load((ROOT / "deploy" / "argo" / "cbr" / "values.base.yaml").read_text(encoding="utf-8"))
    assert payload["cbr"]["schedule"] == "10 7 * * *"
    assert payload["cbr"]["selectors"] == ["app.xml_daily_asp"]
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_runtime_env_examples_contain_expected_modes() -> None:
    fasttrack_dev = (ROOT / "deploy" / "argo" / "fasttrack" / "runtime.dev.env.example").read_text(encoding="utf-8")
    fasttrack_prod = (ROOT / "deploy" / "argo" / "fasttrack" / "runtime.prod.env.example").read_text(encoding="utf-8")
    assert "DPONE_INSTALL_MODE=snapshot" in fasttrack_dev
    assert "ENV_CODE=dev" in fasttrack_dev
    assert "DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT=true" in fasttrack_dev
    assert "DPONE_INSTALL_MODE=baked" in fasttrack_prod
    assert "ENV_CODE=prod" in fasttrack_prod
    assert "DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT=true" in fasttrack_prod
    mindbox_dev = (ROOT / "deploy" / "argo" / "mindbox" / "runtime.dev.env.example").read_text(encoding="utf-8")
    mindbox_prod = (ROOT / "deploy" / "argo" / "mindbox" / "runtime.prod.env.example").read_text(encoding="utf-8")
    cbr_dev = (ROOT / "deploy" / "argo" / "cbr" / "runtime.dev.env.example").read_text(encoding="utf-8")
    cbr_prod = (ROOT / "deploy" / "argo" / "cbr" / "runtime.prod.env.example").read_text(encoding="utf-8")
    openexchangerates_dev = (ROOT / "deploy" / "argo" / "openexchangerates" / "runtime.dev.env.example").read_text(
        encoding="utf-8"
    )
    openexchangerates_prod = (ROOT / "deploy" / "argo" / "openexchangerates" / "runtime.prod.env.example").read_text(
        encoding="utf-8"
    )
    similarweb_dev = (ROOT / "deploy" / "argo" / "similarweb" / "runtime.dev.env.example").read_text(encoding="utf-8")
    similarweb_prod = (ROOT / "deploy" / "argo" / "similarweb" / "runtime.prod.env.example").read_text(encoding="utf-8")
    assert "DPONE_INSTALL_MODE=snapshot" in mindbox_dev
    assert "ENV_CODE=dev" in mindbox_dev
    assert "DPONE_INSTALL_MODE=baked" in mindbox_prod
    assert "ENV_CODE=prod" in mindbox_prod
    assert "DPONE_INSTALL_MODE=snapshot" in cbr_dev
    assert "ENV_CODE=dev" in cbr_dev
    assert "DPONE_INSTALL_MODE=baked" in cbr_prod
    assert "ENV_CODE=prod" in cbr_prod
    assert "DPONE_INSTALL_MODE=snapshot" in openexchangerates_dev
    assert "ENV_CODE=dev" in openexchangerates_dev
    assert "DPONE_INSTALL_MODE=baked" in openexchangerates_prod
    assert "ENV_CODE=prod" in openexchangerates_prod
    assert "DPONE_INSTALL_MODE=snapshot" in similarweb_dev
    assert "ENV_CODE=dev" in similarweb_dev
    assert "DPONE_INSTALL_MODE=baked" in similarweb_prod
    assert "ENV_CODE=prod" in similarweb_prod


def test_live_smoke_env_examples_contain_expected_provider_settings() -> None:
    fasttrack_text = (ROOT / "deploy" / "argo" / "fasttrack" / "live-smoke.dev.env.example").read_text(encoding="utf-8")
    assert "DPONE_IT_FT_VAULT_PATH=api/fasttrack" in fasttrack_text
    assert "DPONE_IT_FT_SELECTORS=default.flex_cms_ratings" in fasttrack_text
    assert "DPONE_IT_FT_CATEGORY=b934e608-aa3d-4359-84c0-169d264bc76d" in fasttrack_text
    mindbox_text = (ROOT / "deploy" / "argo" / "mindbox" / "live-smoke.dev.env.example").read_text(encoding="utf-8")
    cbr_text = (ROOT / "deploy" / "argo" / "cbr" / "live-smoke.dev.env.example").read_text(encoding="utf-8")
    openexchangerates_text = (ROOT / "deploy" / "argo" / "openexchangerates" / "live-smoke.dev.env.example").read_text(
        encoding="utf-8"
    )
    similarweb_text = (ROOT / "deploy" / "argo" / "similarweb" / "live-smoke.dev.env.example").read_text(
        encoding="utf-8"
    )
    assert "DPONE_IT_MB_VAULT_PATH=api/mindbox" in mindbox_text
    assert "DPONE_IT_MB_SELECTORS=app.getactions,app.getorders" in mindbox_text
    assert "DPONE_IT_MB_DAYS_BACK=1" in mindbox_text
    assert "DPONE_IT_MB_SINCE_DATETIME_UTC=" in mindbox_text
    assert "DPONE_IT_MB_TILL_DATETIME_UTC=" in mindbox_text
    assert "default: yesterday 12:00:00Z" in mindbox_text
    assert "default: yesterday 13:00:00Z" in mindbox_text
    assert "DPONE_IT_CBR_SELECTORS=app.xml_daily_asp" in cbr_text
    assert "DPONE_IT_CBR_DAYS_BACK=3" in cbr_text
    assert "DPONE_IT_OXR_VAULT_PATH=api/openexchangerates" in openexchangerates_text
    assert "DPONE_IT_OXR_SELECTORS=default.historical_rates_daily" in openexchangerates_text
    assert "DPONE_IT_OXR_SYMBOLS=ARS,RUB,EUR" in openexchangerates_text
    assert "DPONE_IT_SW_VAULT_PATH=api/similarweb" in similarweb_text
    assert "DPONE_IT_SW_SELECTORS=default.keywords" in similarweb_text
    assert "DPONE_IT_SW_LIMIT=5" in similarweb_text


def test_live_smoke_tools_prefer_rollout_manifests() -> None:
    fasttrack_tool = (ROOT / "tools" / "fasttrack_live_smoke.py").read_text(encoding="utf-8")
    assert "examples/batch/landing_fasttrack_api.batch.yaml" in fasttrack_tool
    mindbox_tool = (ROOT / "tools" / "mindbox_live_smoke.py").read_text(encoding="utf-8")
    cbr_tool = (ROOT / "tools" / "cbr_public_smoke.py").read_text(encoding="utf-8")
    openexchangerates_tool = (ROOT / "tools" / "openexchangerates_live_smoke.py").read_text(encoding="utf-8")
    similarweb_tool = (ROOT / "tools" / "similarweb_live_smoke.py").read_text(encoding="utf-8")
    assert "examples/batch/landing_mindbox_api.batch.yaml" in mindbox_tool
    assert "examples/batch/landing_cbr_api.batch.yaml" in cbr_tool
    assert "examples/batch/landing_openexchangerates_api.batch.yaml" in openexchangerates_tool
    assert "examples/batch/landing_similarweb_api.batch.yaml" in similarweb_tool
