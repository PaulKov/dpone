"""Contracts for interval-aware Airflow handoff and data-aware outlets."""

from __future__ import annotations

import sys
import types

from dpone.gitops.airflow_interval_env import AIRFLOW_INTERVAL_ENV_TEMPLATES, airflow_interval_env_vars
from dpone.gitops.airflow_runtime_models import AIRFLOW_CONTEXT_ENV, read_airflow_runtime_evidence
from dpone.gitops.airflow_xcom_outcome import GitOpsAirflowXComOutcomeBuilder


def test_interval_env_templates_cover_the_run_interval_contract() -> None:
    assert set(AIRFLOW_INTERVAL_ENV_TEMPLATES) == {
        "DPONE_DAG_ID",
        "DPONE_DAG_RUN_ID",
        "DPONE_TRY_NUMBER",
        "DPONE_LOGICAL_DATE",
        "DPONE_INTERVAL_START",
        "DPONE_INTERVAL_END",
        "DPONE_PARTITION_KEY",
    }
    assert AIRFLOW_INTERVAL_ENV_TEMPLATES["DPONE_INTERVAL_START"] == (
        "{{ data_interval_start | ts if data_interval_start is defined and data_interval_start else '' }}"
    )
    assert AIRFLOW_INTERVAL_ENV_TEMPLATES["DPONE_INTERVAL_END"] == (
        "{{ data_interval_end | ts if data_interval_end is defined and data_interval_end else '' }}"
    )
    assert AIRFLOW_INTERVAL_ENV_TEMPLATES["DPONE_TRY_NUMBER"] == "{{ ti.try_number | string }}"
    assert "dag_run.partition_key" in AIRFLOW_INTERVAL_ENV_TEMPLATES["DPONE_PARTITION_KEY"]
    # Fresh copies keep pack builders free of shared mutable state.
    assert airflow_interval_env_vars() is not AIRFLOW_INTERVAL_ENV_TEMPLATES


def test_run_spec_context_env_metadata_includes_interval_names() -> None:
    for name in ("DPONE_INTERVAL_START", "DPONE_INTERVAL_END", "DPONE_LOGICAL_DATE"):
        assert name in AIRFLOW_CONTEXT_ENV


def test_pod_contract_kpo_kwargs_carry_templated_interval_env() -> None:
    from dpone.gitops.airflow_pod_contract import (
        GitOpsAirflowPodContractBuilder,
        GitOpsAirflowPodContractInput,
    )

    contract = GitOpsAirflowPodContractBuilder().build(
        GitOpsAirflowPodContractInput(
            bundle_path="bundle.json",
            run_spec_path="run-spec.json",
            runtime_profile_path="runtime-profile.json",
            pod_spec_path="pod-spec.yaml",
            kpo_kwargs_path="kpo-kwargs.json",
            bundle={},
            run_spec={},
            runtime_profile={"image": "img", "namespace": "etl"},
        )
    )

    assert contract.kpo_kwargs["env_vars"] == airflow_interval_env_vars()
    assert contract.kpo_kwargs["do_xcom_push"] is True


def test_compact_pack_kpo_kwargs_carry_templated_interval_env() -> None:
    from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition

    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="manifests/orders.yaml",
        domain=None,
        catalog_path="catalog.yaml",
        effective_config={"image": "img", "namespace": "etl"},
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(workload=workload, output_path="packs/orders/airflow-pack.json")

    assert report.kpo_kwargs["env_vars"] == airflow_interval_env_vars()


def test_xcom_summary_carries_interval_and_bounded_backfill_sections(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_INTERVAL_START", "2025-01-01T00:00:00+00:00")
    monkeypatch.setenv("DPONE_INTERVAL_END", "2025-01-02T00:00:00+00:00")
    evidence = read_airflow_runtime_evidence({"status": "passed", "steps": []})
    inline_payload = {
        "result": {
            "details": {
                "backfill": {
                    "run_key": "abc123",
                    "chunks_total": 5,
                    "chunks_selected": 1,
                    "chunks_committed": 5,
                    "chunks_failed": 0,
                    "operation_status": "success",
                    "retry_policy": "failed_only",
                    "verification": {"check": "row_count_parity", "status": "passed", "mismatched_chunks": []},
                    "chunks": [{"index": 1}] * 5,  # must NOT leak into XCom
                }
            }
        }
    }

    summary = GitOpsAirflowXComOutcomeBuilder().build(
        evidence=evidence,
        runtime_evidence_path="evidence.json",
        runtime_evidence_sha256="sha256:0",
        inline_payload=inline_payload,
    )
    payload = summary.to_jsonable()

    assert payload["interval"]["interval_start"] == "2025-01-01T00:00:00+00:00"
    assert payload["backfill"]["chunks_committed"] == 5
    assert payload["backfill"]["retry_policy"] == "failed_only"
    assert payload["backfill"]["operation_status"] == "success"
    assert payload["backfill"]["chunks_selected"] == 1
    assert "chunks" not in payload["backfill"]


def test_pack_tasks_attach_asset_outlets_when_declared(monkeypatch) -> None:
    sys.path.insert(0, "packages/dpone-airflow-pack/src")
    try:
        from dpone_airflow_pack.asset_outlets import build_asset_outlets, outlet_uris_from_pack
    finally:
        sys.path.pop(0)

    pack = {"airflow": {"execution": {"outlets": ["clickhouse://analytics/orders", " ", 42]}}}
    assert outlet_uris_from_pack(pack) == ("clickhouse://analytics/orders",)
    assert outlet_uris_from_pack({}) == ()

    class _FakeAsset:
        def __init__(self, uri: str) -> None:
            self.uri = uri

    fake_sdk = types.ModuleType("airflow.sdk")
    fake_sdk.Asset = _FakeAsset
    fake_airflow = types.ModuleType("airflow")
    fake_airflow.sdk = fake_sdk
    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", fake_sdk)

    outlets = build_asset_outlets(("clickhouse://analytics/orders",))

    assert [outlet.uri for outlet in outlets] == ["clickhouse://analytics/orders"]


def test_asset_outlets_skip_safely_without_airflow(monkeypatch) -> None:
    sys.path.insert(0, "packages/dpone-airflow-pack/src")
    try:
        from dpone_airflow_pack.asset_outlets import build_asset_outlets
    finally:
        sys.path.pop(0)

    monkeypatch.setitem(sys.modules, "airflow", None)
    monkeypatch.setitem(sys.modules, "airflow.sdk", None)
    monkeypatch.setitem(sys.modules, "airflow.datasets", None)

    assert build_asset_outlets(("clickhouse://analytics/orders",)) == []
