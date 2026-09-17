from __future__ import annotations

import json

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
