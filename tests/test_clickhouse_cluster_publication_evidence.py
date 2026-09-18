from __future__ import annotations

import json

import pytest

from tests.integration.clickhouse_cluster import conftest as cluster_conftest
from tests.integration.clickhouse_cluster import evidence


def test_receipt_merges_out_of_order_scenarios_without_false_complete_status(tmp_path, monkeypatch) -> None:
    receipt = tmp_path / "receipt.json"
    monkeypatch.setattr(evidence, "RECEIPT", receipt)

    evidence.record_scenario("terminal_partial", "passed_live", server_version="24.8")
    evidence.record_scenario(
        "keeper_cas_and_log_comment",
        "passed_live",
        server_version="24.8",
        details={"keeper_version_after": 1},
    )

    observed = json.loads(receipt.read_text())
    assert observed["scenario_results"]["terminal_partial"] == "passed_live"
    assert observed["scenario_results"]["keeper_cas_and_log_comment"] == "passed_live"
    assert observed["scenario_results"]["normal_existing_and_absent_target"] == "unverified"
    assert observed["keeper_version_after"] == 1
    assert len(observed["source_commit"]) == 40
    assert len(observed["fixture_digest"]) == 64
    assert observed["status"] == "unverified"


def test_receipt_pass_requires_every_canonical_scenario_to_pass_live(tmp_path, monkeypatch) -> None:
    receipt = tmp_path / "receipt.json"
    monkeypatch.setattr(evidence, "RECEIPT", receipt)

    for scenario in evidence.SCENARIOS:
        evidence.record_scenario(scenario, "passed_live", server_version="24.8")
    assert json.loads(receipt.read_text())["status"] == "passed_live"

    evidence.record_scenario("queue_status_and_host_matrix", "passed_fault_injection", server_version="24.8")
    assert json.loads(receipt.read_text())["status"] == "unverified"


def test_receipt_reset_discards_stale_session_evidence(tmp_path, monkeypatch) -> None:
    receipt = tmp_path / "receipt.json"
    receipt.write_text("{}")
    monkeypatch.setattr(evidence, "RECEIPT", receipt)

    evidence.reset_receipt()

    assert not receipt.exists()


def test_external_receipt_is_separate_scoped_and_per_scenario(tmp_path, monkeypatch) -> None:
    receipt = tmp_path / "external-receipt.json"
    monkeypatch.setattr(evidence, "EXTERNAL_RECEIPT", receipt)

    evidence.record_external_scenario(
        "external_replication_fresh_cleanup",
        "PASS",
        server_version="24.8",
        details={"production_composition": True},
    )

    observed = json.loads(receipt.read_text())
    assert observed["evidence_scope"] == "local_synthetic"
    assert observed["production_certification"] == "UNVERIFIED"
    assert set(observed["scenarios"]) == set(evidence.EXTERNAL_SCENARIOS)
    scenario = observed["scenarios"]["external_replication_fresh_cleanup"]
    assert scenario == {
        "status": "PASS",
        "evidence_scope": "local_synthetic",
        "details": {"production_composition": True},
    }
    assert "external_replication_fresh_cleanup" not in evidence.SCENARIOS


def test_session_discards_stale_evidence_before_readiness_can_fail(tmp_path, monkeypatch) -> None:
    receipt = tmp_path / "receipt.json"
    receipt.write_text('{"status":"passed_live"}')
    monkeypatch.setattr(evidence, "RECEIPT", receipt)

    def fail_readiness() -> None:
        raise AssertionError("synthetic startup failure")

    monkeypatch.setattr(cluster_conftest, "_wait_for_distributed_ddl", fail_readiness)
    with pytest.raises(AssertionError, match="synthetic startup failure"):
        cluster_conftest._prepare_cluster_publication_session()

    assert not receipt.exists()
