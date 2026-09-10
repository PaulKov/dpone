"""Isolated SWITCH component fixtures; never normal native-route admission evidence."""

from __future__ import annotations

import pytest
from tests.integration.mssql.clickhouse_mssql_delivery_support import live_factory
from tools.native_delivery_live_support.artifacts import ArtifactStore
from tools.native_delivery_live_support.execution import DeliveryClock, ExecutionAdapter
from tools.native_delivery_live_support.maintenance import record_owner
from tools.native_delivery_live_support.profiles import Dataset, exact_multiset, multiset_summary
from tools.native_delivery_live_support.runner import run_benchmark

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]


def test_real_isolated_switch_empty_window_and_receipt_first_recovery(tmp_path):
    factory, config, route = live_factory("partition_replace", "isolated_switch")
    report = run_benchmark(
        adapter=ExecutionAdapter(factory, "candidate"),
        dataset=Dataset("narrow", 64),
        config=config,
        route=route,
        store=ArtifactStore(tmp_path / "run.json"),
    )
    assert report["route"]["mode"] == "isolated_switch"
    assert report["fidelity_receipt"]["status"] == report["recovery_receipt"]["status"] == "PASS"
    assert all(sample["status"] == "PASS" for sample in report["samples"])


@pytest.mark.parametrize("fault", ["nonempty_switch_out", "layout_drift", "owner_drift", "between_switches"])
def test_real_switch_rejection_or_transaction_rollback_preserves_target(tmp_path, fault):
    factory, _, route = live_factory("partition_replace", "isolated_switch")
    session = factory.open(Dataset("narrow", 16), case=fault, clock=DeliveryClock())
    store = ArtifactStore(tmp_path / "switch-fixture.json")
    known, passed = False, False
    artifact = {
        "schema_version": 1,
        "kind": "native-delivery-switch-fixture",
        "route": route,
        "execution": "live",
        "fault": fault,
        "status": "FAIL",
    }
    try:
        record_owner(store, session, fault)
        session.arm_fault(fault)
        before = session.snapshot()
        rows, outside = exact_multiset(before.rows), exact_multiset(before.outside_rows)
        with pytest.raises(Exception):
            session.run()
        after = session.snapshot()
        known = after.commit_known
        observed, observed_outside = exact_multiset(after.rows), exact_multiset(after.outside_rows)
        passed = (
            known
            and rows == observed
            and outside == observed_outside
            and before.publications == after.publications
            and fault not in before.fault_events
            and fault in after.fault_events
        )
        artifact.update(
            {
                "schema_version": 1,
                "kind": "native-delivery-switch-fixture",
                "route": route,
                "execution": "live",
                "fault": fault,
                "expected": multiset_summary(rows),
                "observed": multiset_summary(observed),
                "outside_expected": multiset_summary(outside),
                "outside_observed": multiset_summary(observed_outside),
            }
        )
    finally:
        try:
            if known:
                session.cleanup()
        except Exception:
            passed = False
        finally:
            try:
                session.close()
            except Exception:
                passed = False
        artifact["status"] = "PASS" if passed else "FAIL"
        store.publish(artifact)
    assert passed
