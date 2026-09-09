from __future__ import annotations

from pathlib import Path

import yaml

from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_MANIFEST = ROOT / "examples" / "batch" / "landing_openexchangerates_api.batch.yaml"
VALUES_BASE = ROOT / "deploy" / "argo" / "openexchangerates" / "values.base.yaml"
VALUES_DEV = ROOT / "deploy" / "argo" / "openexchangerates" / "values.dev.yaml"
VALUES_PROD = ROOT / "deploy" / "argo" / "openexchangerates" / "values.prod.yaml"
RUNTIME_DEV = ROOT / "deploy" / "argo" / "openexchangerates" / "runtime.dev.env.example"
RUNTIME_PROD = ROOT / "deploy" / "argo" / "openexchangerates" / "runtime.prod.env.example"
LIVE_SMOKE_DEV = ROOT / "deploy" / "argo" / "openexchangerates" / "live-smoke.dev.env.example"


def test_rollout_manifest_exists_and_loads() -> None:
    assert CANONICAL_MANIFEST.exists()
    loader = ConfigLoader(CANONICAL_MANIFEST.parent, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(CANONICAL_MANIFEST, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 1
    process = manifest.processes[0].config.load_config
    assert process.target_schema == "landing__openexchangerates__api"
    assert process.target_table == "default__historical_rates_daily"


def test_rollout_values_define_schedule_and_runtime_wrapper() -> None:
    payload = yaml.safe_load(VALUES_BASE.read_text(encoding="utf-8"))
    assert payload["openexchangerates"]["schedule"] == "5 4 * * *"
    assert payload["openexchangerates"]["selectors"] == ["default.historical_rates_daily"]
    assert payload["airflow"]["scheduler"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["worker"]["command"][0] == "dpone-runtime-exec"
    assert payload["airflow"]["triggerer"]["command"][0] == "dpone-runtime-exec"


def test_dev_and_prod_env_examples_contain_expected_runtime_modes() -> None:
    dev_text = RUNTIME_DEV.read_text(encoding="utf-8")
    prod_text = RUNTIME_PROD.read_text(encoding="utf-8")
    assert "DPONE_INSTALL_MODE=snapshot" in dev_text
    assert "DPONE_PACKAGE_SPEC=" in dev_text
    assert "ENV_CODE=dev" in dev_text
    assert "DPONE_INSTALL_MODE=baked" in prod_text
    assert "ENV_CODE=prod" in prod_text
    assert VALUES_DEV.exists()
    assert VALUES_PROD.exists()


def test_live_smoke_env_points_to_openexchangerates_settings() -> None:
    text = LIVE_SMOKE_DEV.read_text(encoding="utf-8")
    assert "DPONE_IT_OXR_VAULT_PATH=api/openexchangerates" in text
    assert "DPONE_IT_OXR_SYMBOLS=ARS,RUB,EUR" in text
    assert "DPONE_IT_OXR_SELECTORS=default.historical_rates_daily" in text


def test_live_smoke_tool_prefers_rollout_manifest() -> None:
    tool_path = ROOT / "tools" / "openexchangerates_live_smoke.py"
    text = tool_path.read_text(encoding="utf-8")
    assert "examples/batch/landing_openexchangerates_api.batch.yaml" in text
