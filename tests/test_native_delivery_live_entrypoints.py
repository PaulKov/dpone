"""Regress actual opt-in test entrypoints using only local protocol doubles."""

from __future__ import annotations

from copy import deepcopy

import pytest
from tools.native_delivery_live_support.artifacts import ArtifactStore
from tools.native_delivery_live_support.execution import ExecutionAdapter
from tools.native_delivery_live_support.hermetic import LIMITS, HermeticRouteFactory, HermeticRouteSession
from tools.native_delivery_live_support.profiles import Dataset
from tools.native_delivery_live_support.runner import configuration, route_record, run_benchmark

from tests.integration.mssql import test_clickhouse_mssql_bounded_native_delivery_integration as bounded
from tests.integration.mssql import test_clickhouse_mssql_partition_switch_integration as switch


@pytest.fixture(params=["bounded", "switch"])
def entrypoint(request, monkeypatch, tmp_path):
    module = bounded if request.param == "bounded" else switch
    mode = "bounded_native" if module is bounded else "isolated_switch"

    def invoke(report, *, execution="live"):
        # live_factory itself is replaced; no approval, connection or discovery
        # code executes. The double's label tests the assertion boundary only.
        factory = HermeticRouteFactory()
        factory.execution = execution
        monkeypatch.setattr(
            module, "live_factory", lambda *_: (factory, configuration(LIMITS), route_record("partition_replace", mode))
        )
        monkeypatch.setattr(module, "run_benchmark", lambda **_: deepcopy(report))
        if module is bounded:
            bounded.test_real_bounded_delivery_fidelity_and_recovery(tmp_path, "narrow", "partition_replace")
        else:
            switch.test_real_isolated_switch_empty_window_and_receipt_first_recovery(tmp_path)

    return invoke, mode


def passing_report(mode):
    return {
        "status": "PASS",
        "subject": {"dirty": False},
        "producer": {"dirty": False},
        "route": {"mode": mode},
        "fidelity_receipt": {"status": "PASS"},
        "recovery_receipt": {"status": "PASS"},
        "samples": [{"status": "PASS"} for _ in range(4)],
    }


def assert_redacted_failure(invoke, report, **kwargs):
    with pytest.raises(pytest.fail.Exception) as error:
        invoke(report, **kwargs)
    assert str(error.value) == "DDA live fixture failed; inspect retained sanitized artifacts and approved factory"
    assert error.value.__context__ is None
    assert error.value.__cause__ is None


@pytest.mark.parametrize("dirty", [None, "subject", "producer"])
def test_live_entrypoint_accepts_complete_pass_or_explicit_dirty_development(entrypoint, dirty):
    invoke, mode = entrypoint
    report = passing_report(mode)
    if dirty:
        report["status"] = "UNVERIFIED"
        report[dirty]["dirty"] = True
    invoke(report)


@pytest.mark.parametrize("status", ["FAIL", "SKIP", "UNVERIFIED", "unknown", None])
def test_live_entrypoint_rejects_final_failure_absence_or_clean_unverified(entrypoint, status):
    invoke, mode = entrypoint
    report = passing_report(mode)
    if status is None:
        del report["status"]
    else:
        report["status"] = status
    assert_redacted_failure(invoke, report)


@pytest.mark.parametrize("dirty", [1, "true", [], None])
def test_live_entrypoint_requires_boolean_dirty_evidence(entrypoint, dirty):
    invoke, mode = entrypoint
    report = passing_report(mode)
    report["status"] = "UNVERIFIED"
    report["subject"]["dirty"] = dirty
    assert_redacted_failure(invoke, report)


@pytest.mark.parametrize("status", ["PASS", "UNVERIFIED"])
@pytest.mark.parametrize(
    "broken", ["fidelity_receipt", "recovery_receipt", "missing_receipt", "empty", "partial", "failed_sample"]
)
def test_live_entrypoint_requires_every_component_proof(entrypoint, status, broken):
    invoke, mode = entrypoint
    report = passing_report(mode)
    report["status"] = status
    report["subject"]["dirty"] = True
    if broken in {"fidelity_receipt", "recovery_receipt"}:
        report[broken]["status"] = "FAIL"
    elif broken == "missing_receipt":
        del report["fidelity_receipt"]
    elif broken == "failed_sample":
        report["samples"][-1]["status"] = "FAIL"
    else:
        report["samples"] = report["samples"][: 0 if broken == "empty" else 3]
    assert_redacted_failure(invoke, report)


@pytest.mark.parametrize("status", ["PASS", "UNVERIFIED"])
def test_hermetic_execution_cannot_obtain_live_allowance(entrypoint, status):
    invoke, mode = entrypoint
    report = passing_report(mode)
    report["status"] = status
    report["producer"]["dirty"] = True
    assert_redacted_failure(invoke, report, execution="hermetic")


def test_actual_producer_late_identity_failure_is_rejected_by_live_entrypoint(entrypoint, tmp_path):
    invoke, mode = entrypoint

    class DriftingSession(HermeticRouteSession):
        def cleanup(self):
            super().cleanup()
            if self.case == "trial-003":
                factory.layout = "d" * 64

    class DriftingFactory(HermeticRouteFactory):
        def open(self, dataset, *, case, clock):
            session = DriftingSession(dataset, case, clock)
            self.sessions.append(session)
            return session

    factory = DriftingFactory()
    report = run_benchmark(
        adapter=ExecutionAdapter(factory, "candidate"),
        dataset=Dataset("narrow", 8),
        config=configuration(LIMITS),
        route=route_record("partition_replace", mode),
        store=ArtifactStore(tmp_path / "late-drift.json"),
    )
    assert report["status"] == "FAIL"
    assert report["fidelity_receipt"]["status"] == report["recovery_receipt"]["status"] == "PASS"
    assert len(report["samples"]) == 4
    assert all(sample["status"] == "PASS" for sample in report["samples"])
    assert "Producer, subject or environment identity changed during execution." in report["limitations"]
    assert_redacted_failure(invoke, report)
