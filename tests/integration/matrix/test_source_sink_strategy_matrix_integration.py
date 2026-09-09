from __future__ import annotations

import os
from pathlib import Path

import pytest

from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX, IntegrationMatrixCase, matrix_case_selected

ROOT = Path(__file__).resolve().parents[3]
pytestmark = [pytest.mark.integration, pytest.mark.integration_matrix, pytest.mark.integration_matrix_mock]


def _matrix_enabled() -> bool:
    return os.environ.get("DPONE_RUN_INTEGRATION_MATRIX", "0").strip().lower() in {"1", "true", "yes", "on"}


def _local_run_mode() -> bool:
    return os.environ.get("DPONE_MATRIX_RUN_MODE", "mock_contract") in {"mock_local", "real_local"}


def _expected_change_count(row_count: int, ratio: float) -> int:
    return max(1, int(row_count * ratio))


def _expected_delete_count(row_count: int, ratio: float) -> int:
    return max(1, int(row_count * ratio))


@pytest.mark.parametrize("case", DEFAULT_INTEGRATION_MATRIX.cases, ids=lambda case: case.case_id)
def test_source_sink_strategy_matrix_preflight(case: IntegrationMatrixCase, tmp_path: Path) -> None:
    if not _matrix_enabled():
        pytest.skip("Set DPONE_RUN_INTEGRATION_MATRIX=1 to run the manual source->sink strategy matrix.")
    if not matrix_case_selected(case, env=os.environ):
        pytest.skip("Case filtered out by DPONE_MATRIX_SOURCE/SINK/STRATEGY/CASE_ID.")
    if _local_run_mode() and not case.local_service_supported:
        pytest.skip("Case is documented-contract only in local matrix modes; run vendor_live for a real target.")

    guide = ROOT / "docs" / case.guide
    assert guide.exists()
    assert case.strategy in {
        "full_refresh",
        "incremental_append",
        "incremental_merge",
        "replace",
        "partition_replace",
        "snapshot_diff",
        "scd2",
        "backfill",
        "xmin",
        "cdc",
    }
    assert case.install_extras
    assert case.required_profiles
    assert case.external_credentials_required is False

    manifest = case.example_manifest(name_prefix="matrix_preflight")
    assert manifest["source"]["type"] == case.source
    assert manifest["sink"]["type"] == case.sink
    assert manifest["sink"]["strategy"]["mode"] == case.sink_strategy

    artifact = tmp_path / f"{case.case_id}.json"
    artifact.write_text(case.to_json(), encoding="utf-8")
    assert artifact.read_text(encoding="utf-8").startswith("{")

    artifact_dir_raw = os.environ.get("DPONE_MATRIX_ARTIFACT_DIR", "").strip()
    if artifact_dir_raw:
        artifact_dir = Path(artifact_dir_raw)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / f"{case.case_id}.json").write_text(case.to_json(), encoding="utf-8")


@pytest.mark.parametrize("case", DEFAULT_INTEGRATION_MATRIX.cases, ids=lambda case: case.case_id)
def test_source_sink_strategy_matrix_mock_strategy_behavior(case: IntegrationMatrixCase) -> None:
    if not _matrix_enabled():
        pytest.skip("Set DPONE_RUN_INTEGRATION_MATRIX=1 to run the manual source->sink strategy matrix.")
    if not matrix_case_selected(case, env=os.environ):
        pytest.skip("Case filtered out by DPONE_MATRIX_SOURCE/SINK/STRATEGY/CASE_ID.")
    if _local_run_mode() and not case.local_service_supported:
        pytest.skip("Case is documented-contract only in local matrix modes; run vendor_live for a real target.")

    result = case.simulate_mock_strategy_behavior()

    assert result.passed is True
    assert result.case_id == case.case_id
    assert result.strategy == case.strategy
    assert result.expected_rows == result.actual_rows
    assert result.wide_column_count >= 100
    assert result.configured_row_count >= 1
    assert result.changed_row_count == _expected_change_count(result.configured_row_count, result.change_ratio)
    assert result.deleted_row_count == _expected_delete_count(result.configured_row_count, result.delete_ratio)
    assert result.expected_row_count == result.actual_row_count
    assert result.expected_checksum == result.actual_checksum

    artifact_dir_raw = os.environ.get("DPONE_MATRIX_ARTIFACT_DIR", "").strip()
    if artifact_dir_raw:
        artifact_dir = Path(artifact_dir_raw)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / f"{case.case_id}__behavior.json").write_text(result.to_json(), encoding="utf-8")
