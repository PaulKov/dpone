from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_integration_matrix_registry_covers_every_source_sink_and_strategy() -> None:
    from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX

    source_sink_pairs = {(case.source, case.sink) for case in DEFAULT_INTEGRATION_MATRIX.cases}
    assert len(source_sink_pairs) == 30
    assert len(DEFAULT_INTEGRATION_MATRIX.cases) == 237
    runbook = (ROOT / "docs/testing/integration-matrix.md").read_text(encoding="utf-8")
    assert f"{len(DEFAULT_INTEGRATION_MATRIX.cases)} source -> sink -> strategy cases" in runbook

    for source, sink in source_sink_pairs:
        strategies = {case.strategy for case in DEFAULT_INTEGRATION_MATRIX.for_pair(source, sink)}
        expected = {"full_refresh", "incremental_append", "incremental_merge", "replace", "snapshot_diff"}
        if sink in {"mssql", "postgres", "clickhouse", "bigquery"}:
            expected |= {"partition_replace", "scd2", "backfill"}
        if source == "postgres":
            expected |= {"xmin", "cdc"}
        if source == "mssql":
            expected |= {"cdc"}
        assert strategies == expected


def test_integration_matrix_distinguishes_mock_local_and_vendor_live_credentials() -> None:
    from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX

    assert all(case.external_credentials_required is False for case in DEFAULT_INTEGRATION_MATRIX.cases)
    assert len(DEFAULT_INTEGRATION_MATRIX.local_service_cases()) == 186
    assert len(DEFAULT_INTEGRATION_MATRIX.documented_contract_cases()) == 51

    bigquery_cases = [case for case in DEFAULT_INTEGRATION_MATRIX.cases if case.sink == "bigquery"]
    assert bigquery_cases
    assert all(case.local_service_supported is False for case in bigquery_cases)
    assert all("bigquery_vendor_live" in case.live_profiles for case in bigquery_cases)

    mssql_cases = [case for case in DEFAULT_INTEGRATION_MATRIX.cases if case.sink == "mssql" or case.source == "mssql"]
    assert mssql_cases
    assert all("mssql_local" in case.required_profiles for case in mssql_cases)


def test_integration_matrix_guides_match_source_sink_documentation() -> None:
    from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX

    matrix_doc = (ROOT / "docs" / "source-sink-matrix.md").read_text(encoding="utf-8")
    load_doc = (ROOT / "docs" / "load-strategies.md").read_text(encoding="utf-8")

    for case in DEFAULT_INTEGRATION_MATRIX.unique_pairs():
        guide_path = ROOT / "docs" / case.guide
        assert guide_path.exists(), f"Missing source->sink guide for {case.source}->{case.sink}: {case.guide}"
        assert case.guide in matrix_doc
        assert case.guide in load_doc


def test_integration_matrix_mock_strategy_behavior_contracts() -> None:
    from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX

    for case in DEFAULT_INTEGRATION_MATRIX.cases:
        result = case.simulate_mock_strategy_behavior(row_count=128)
        assert result.passed is True
        assert result.expected_rows == result.actual_rows
        assert result.wide_column_count >= 100
        assert result.configured_row_count == 128
        assert result.changed_row_count == 25
        assert result.deleted_row_count == 6

    full_refresh = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")[0].simulate_mock_strategy_behavior(
        row_count=128
    )
    assert full_refresh.source_row_count == 128
    assert full_refresh.actual_row_count == 128
    assert "row_count_matches_full_source" in full_refresh.quality_checks

    incremental_append = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")[1].simulate_mock_strategy_behavior(
        row_count=128
    )
    assert incremental_append.source_row_count == 25
    assert incremental_append.actual_row_count == 153
    assert "append_count_matches_delta" in incremental_append.quality_checks

    incremental_merge = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")[2].simulate_mock_strategy_behavior(
        row_count=128
    )
    assert incremental_merge.source_row_count == 25
    assert incremental_merge.actual_row_count == 128
    assert "delete_keys_absent" in incremental_merge.quality_checks

    replace = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")[3].simulate_mock_strategy_behavior(row_count=128)
    assert replace.source_row_count == 19
    assert replace.actual_row_count == 128

    partition_replace = [
        case
        for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")
        if case.strategy == "partition_replace"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert partition_replace.source_row_count == 19
    assert partition_replace.actual_row_count == 128
    assert "delete_keys_absent" in partition_replace.quality_checks

    kafka_merge = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "kafka")[2].simulate_mock_strategy_behavior(
        row_count=128
    )
    assert {event["op"] for event in kafka_merge.actual_rows} == {"delete", "upsert"}
    assert kafka_merge.actual_row_count == 25

    postgres_xmin = [
        case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql") if case.strategy == "xmin"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert postgres_xmin.source_row_count == 25
    assert postgres_xmin.actual_row_count == 128
    assert "delete_keys_absent" in postgres_xmin.quality_checks

    postgres_cdc = [
        case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql") if case.strategy == "cdc"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert postgres_cdc.source_row_count == 25
    assert postgres_cdc.actual_row_count == 128
    assert "delete_keys_absent" in postgres_cdc.quality_checks

    mssql_cdc_to_kafka = [
        case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("mssql", "kafka") if case.strategy == "cdc"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert {event["op"] for event in mssql_cdc_to_kafka.actual_rows} == {"insert", "update", "delete"}
    assert mssql_cdc_to_kafka.actual_row_count == 25

    snapshot_diff = [
        case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql") if case.strategy == "snapshot_diff"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert snapshot_diff.source_row_count == 128
    assert snapshot_diff.actual_row_count == 128
    assert "snapshot_diff_row_hash_matches" in snapshot_diff.quality_checks

    scd2 = [case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql") if case.strategy == "scd2"][
        0
    ].simulate_mock_strategy_behavior(row_count=128)
    assert scd2.source_row_count == 128
    assert scd2.actual_row_count == 147
    assert "scd2_current_rows_match_source" in scd2.quality_checks

    backfill = [
        case for case in DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql") if case.strategy == "backfill"
    ][0].simulate_mock_strategy_behavior(row_count=128)
    assert backfill.source_row_count == 19
    assert backfill.actual_row_count == 128
    assert "backfill_chunk_state_committed" in backfill.quality_checks


def test_integration_matrix_mock_volume_and_ratio_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX

    case = DEFAULT_INTEGRATION_MATRIX.for_pair("postgres", "mssql")[2]
    monkeypatch.setenv("DPONE_MATRIX_MOCK_ROW_COUNT", "100")
    monkeypatch.setenv("DPONE_MATRIX_CHANGE_RATIO", "0.30")
    monkeypatch.setenv("DPONE_MATRIX_DELETE_RATIO", "0.10")

    result = case.simulate_mock_strategy_behavior()
    assert result.configured_row_count == 100
    assert result.changed_row_count == 30
    assert result.deleted_row_count == 10
    assert result.actual_row_count == 100

    monkeypatch.delenv("DPONE_MATRIX_MOCK_ROW_COUNT")
    monkeypatch.delenv("DPONE_MATRIX_CHANGE_RATIO")
    monkeypatch.delenv("DPONE_MATRIX_DELETE_RATIO")
    monkeypatch.setenv("DPONE_MATRIX_ROW_COUNT", "25000")
    aliased = case.simulate_mock_strategy_behavior()
    assert aliased.configured_row_count == 25000
    assert aliased.changed_row_count == 5000
    assert aliased.deleted_row_count == 1250

    overridden = case.simulate_mock_strategy_behavior(row_count=64, change_ratio=0.25, delete_ratio=0.125)
    assert overridden.configured_row_count == 64
    assert overridden.changed_row_count == 16
    assert overridden.deleted_row_count == 8
    assert overridden.actual_row_count == 64

    with pytest.raises(ValueError, match="DPONE_MATRIX_MOCK_ROW_COUNT"):
        case.simulate_mock_strategy_behavior(row_count=100_001)

    with pytest.raises(ValueError, match="DPONE_MATRIX_DELETE_RATIO"):
        case.simulate_mock_strategy_behavior(row_count=100, change_ratio=0.05, delete_ratio=0.10)


def test_pyproject_declares_integration_matrix_marker() -> None:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    markers = data["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("integration_matrix:") for marker in markers)


def test_github_actions_has_manual_integration_matrix_workflow() -> None:
    workflow_path = ROOT / ".github" / "workflows" / "integration-matrix.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    triggers = workflow.get("on") or workflow.get(True)
    assert "workflow_dispatch" in triggers
    assert "push" not in triggers
    assert "pull_request" not in triggers

    job = workflow["jobs"]["integration-matrix"]
    assert job["env"]["DPONE_RUN_INTEGRATION_MATRIX"] == "1"
    assert job["env"]["DPONE_MATRIX_RUN_MODE"] == "${{ inputs.run_mode }}"
    assert job["env"]["DPONE_MATRIX_ARTIFACT_DIR"] == "test_artifacts/integration_matrix"
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert (
        "docker compose -f docker/docker-compose.integration.yml up -d postgres clickhouse mssql kafka schema-registry"
        in script
    )
    assert "ACCEPT_EULA=Y apt-get install" in script
    assert "pytest -m integration_matrix tests/integration/matrix" in script


def test_testing_runbook_docs_cover_matrix_layers_and_failure_recovery() -> None:
    docs = [
        ROOT / "docs" / "testing" / "index.md",
        ROOT / "docs" / "testing" / "integration-matrix.md",
        ROOT / "docs" / "testing" / "local-mssql-mock-matrix.md",
    ]
    for path in docs:
        text = path.read_text(encoding="utf-8")
        assert "mock_local" in text
        assert "vendor_live" in text
        assert "Runbook" in text
        assert "Failure" in text or "failure" in text

    detailed = (ROOT / "docs" / "testing" / "integration-matrix.md").read_text(encoding="utf-8")
    for required in (
        "Mock strategy behavior layer",
        "Fixtures and volumes",
        "Mock source/target fixtures",
        "Volume profile",
        "Artifact schema",
        "What mock_contract proves",
        "What mock_contract does not prove",
        "Failure: strategy behavior artifact differs from expected rows",
        "DPONE_MATRIX_CASE_ID",
    ):
        assert required in detailed
