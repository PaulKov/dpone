"""The offline example exercises real local authority without claiming SQL execution."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/native/raw-window-runtime.py"


def example():
    spec = importlib.util.spec_from_file_location("raw_window_example", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_offline_example_is_repeatable_and_explicitly_unverified(tmp_path):
    for _ in range(2):
        result = subprocess.run(
            [sys.executable, str(EXAMPLE), "--work-dir", str(tmp_path)],
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["status"] == "composition_required"
        assert report["live_preflight"] == "not_run"
        assert report["certification_status"] == "unverified"
        assert report["local_lease_exclusion"] == "PASS"
        assert report["local_cas"] == "PASS"
        assert report["source_read_mode"] == "raw_single_query"
        assert report["source_queries"] == report["target_publications"] == 0
        assert (tmp_path / "example-authority.sqlite3").is_file()


def test_factory_requires_all_authorities(tmp_path):
    module = example()
    with pytest.raises(TypeError):
        module.compose_runtime(module.synthetic_config(), store=None, target_id="synthetic")


def test_plan_binds_authored_policy_without_guessing_identity():
    module = example()
    config = module.synthetic_config()
    plan = module.bind_plan(
        config,
        run_id="run",
        target_id="target",
        source_query_id="query",
        window_fingerprint="window",
        schema_fingerprint="schema",
        wire_fingerprint="layout",
    )
    assert plan.source_read_mode == "raw_single_query"
    assert plan.to_dict()["source_read_mode"] == "raw_single_query"
    del config.options["native_transfer"]["source_read"]
    legacy = module.bind_plan(
        config,
        run_id="run",
        target_id="target",
        source_query_id="query",
        window_fingerprint="window",
        schema_fingerprint="schema",
        wire_fingerprint="layout",
    )
    assert "source_read_mode" not in legacy.to_dict()


def test_construction_hook_uses_real_runtime_and_preserves_adapters(tmp_path, monkeypatch):
    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
    from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime

    module = example()
    config = module.synthetic_config()

    def unavailable(*args):
        raise AssertionError("offline construction must not invoke deployment authority")

    adapters = {
        "store": SQLiteWindowStore(tmp_path / "state.sqlite3", clock=lambda: 1),
        "target_id": "synthetic-target",
        **dict.fromkeys(("bindings", "source", "quality", "evidence", "advance_state"), unavailable),
    }
    runtime = module.compose_runtime(config, **adapters)
    assert isinstance(runtime, NativeMssqlRuntime)
    assert runtime.source is unavailable
    assert runtime.bindings is unavailable
    observed = []

    def observe_run(self, load_config, *, owner):
        observed.append((load_config, owner, self.target_id))
        return "construction-hook-only"

    monkeypatch.setattr(NativeMssqlRuntime, "run", observe_run)
    assert module.run_window(config, owner="persisted-owner", **adapters) == "construction-hook-only"
    assert observed == [(config, "persisted-owner", "synthetic-target")]


def test_live_requires_approval_before_loading_environment(tmp_path, monkeypatch):
    module = example()
    monkeypatch.delenv("DPONE_DDA_DISPOSABLE_APPROVED", raising=False)
    with pytest.raises(ValueError, match="local_approval_required"):
        module.run_local_example(tmp_path, scenario="completed-stage")
    assert list(tmp_path.iterdir()) == []


def test_completed_stage_scenario_attaches_same_invocation_with_source_disabled():
    module = example()
    events = []

    class Session:
        invocation_id = "same-invocation"

        def arm_fault(self, fault):
            events.append(fault)

        def run(self):
            raise RuntimeError("local_fixture.injected:after_eof")

        def journal_data(self):
            return {"phase": "stage_complete", "complete": True, "completion_metadata": {"frozen": True}}

        def close(self):
            events.append("close")

    class Recovered:
        invocation_id = "same-invocation"
        config = module.synthetic_config()
        plan = type("Plan", (), {"source_read_mode": "raw_single_query"})()

        def recover(self, *, source_allowed):
            events.append(("recover", source_allowed))

    class Factory:
        def attach(self, invocation_id):
            events.append(("attach", invocation_id))
            return Recovered()

    attached = module.recover_completed(Factory(), Session())
    assert attached.invocation_id == "same-invocation"
    assert events == ["after_eof", "close", ("attach", "same-invocation"), ("recover", False)]


def test_completed_stage_scenario_rejects_unrelated_failure():
    module = example()

    class Session:
        def arm_fault(self, fault):
            pass

        def run(self):
            raise RuntimeError("unexpected real failure")

    with pytest.raises(RuntimeError, match="unexpected real failure"):
        module.recover_completed(None, Session())


@pytest.mark.parametrize("mode", [None, "raw_single_query"])
def test_fixture_inventory_preserves_source_policy_and_attach_rejects_change(tmp_path, mode):
    from types import SimpleNamespace
    from uuid import UUID

    from tools.native_delivery_live_support.runner import configuration, route_record
    from tools.native_delivery_local.configuration import load_configuration
    from tools.native_delivery_local.factory import LocalRouteFactory
    from tools.native_delivery_local.inventory import Inventory, InventoryStore

    from dpone.manifest.mssql_native_policy import native_source_read_mode

    module = example()
    execution = module.synthetic_config().options["native_transfer"]["execution"]
    limits = {**execution["native_chunks"], "parallelism": execution["chunking"]["parallelism"]}
    config = configuration(limits)
    invocation = "00000000000000000000000000000001"
    value = Inventory(
        invocation,
        "unicode",
        32,
        0,
        "partition_replace",
        "dda_synthetic",
        "dda_synthetic",
        "dda_synthetic",
        "dda_" + invocation,
        "business",
        config,
        {},
        {},
        source_read_mode=mode,
    )
    store = InventoryStore(tmp_path)
    store.create(value)
    restored = store.load(invocation)
    assert restored == value
    assert native_source_read_mode(load_configuration(restored)) == mode
    payload = json.loads((tmp_path / invocation / "inventory.json").read_text())["payload"]
    assert ("source_read_mode" in payload) == (mode is not None)
    factory = LocalRouteFactory(
        configuration=config,
        route=route_record("partition_replace", "bounded_native"),
        environment=SimpleNamespace(root=tmp_path),
        source_read_mode=None if mode else "raw_single_query",
    )
    # No provisioned seal or database credentials exist: rejection must precede Session.
    with pytest.raises(ValueError, match="recovery_configuration_changed"):
        factory.attach(str(UUID(hex=invocation)))


def test_incomplete_scenario_cleans_old_invocation_before_new_admission(monkeypatch):
    from types import SimpleNamespace

    module = example()
    events = []
    unchanged = SimpleNamespace(rows=({"id": 1},), outside_rows=(), publications=0, commit_known=True)

    class Session:
        invocation_id = "old"

        def snapshot(self):
            return unchanged

        def arm_fault(self, fault):
            events.append(fault)

        def run(self):
            raise RuntimeError("local_fixture.injected:during_source")

        def journal_data(self):
            return {"complete": False}

        def cleanup(self):
            events.append("cleanup")

        def close(self):
            events.append("close")

    replacement = SimpleNamespace(
        invocation_id="new", config=module.synthetic_config(), plan=SimpleNamespace(source_read_mode="raw_single_query")
    )

    class Factory:
        def open(self, dataset, *, case, clock):
            events.append(("open", case))
            return replacement

    assert module.replace_incomplete(Factory(), Session(), None) is replacement
    assert events == ["during_source", "cleanup", "close", ("open", "new-after-incomplete")]


def test_live_cli_denies_unapproved_environment_without_artifacts(tmp_path, monkeypatch):
    monkeypatch.delenv("DPONE_DDA_DISPOSABLE_APPROVED", raising=False)
    result = subprocess.run(
        [sys.executable, str(EXAMPLE), "--live", "--scenario", "first-load", "--work-dir", str(tmp_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "status": "FAIL",
        "error_type": "ValueError",
        "certification_status": "unverified",
    }
    assert result.stderr == ""
    assert list(tmp_path.iterdir()) == []


def test_live_factory_binds_raw_inventory_before_provision(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.profiles import Dataset
    from tools.native_delivery_live_support.runner import configuration, route_record
    from tools.native_delivery_local import factory as factory_module
    from tools.native_delivery_local.configuration import load_configuration

    from dpone.manifest.mssql_native_policy import native_source_read_mode

    module = example()
    execution = module.synthetic_config().options["native_transfer"]["execution"]
    limits = {**execution["native_chunks"], "parallelism": execution["chunking"]["parallelism"]}
    observed = []

    def provision(environment, store, inventory):
        restored = store.load(inventory.invocation_id)
        observed.append(native_source_read_mode(load_configuration(restored)))
        return {}

    monkeypatch.setattr(factory_module, "provision", provision)
    monkeypatch.setattr(factory_module, "Session", lambda environment, store, inventory, clock: inventory)
    factory = factory_module.LocalRouteFactory(
        configuration=configuration(limits),
        route=route_record("partition_replace", "bounded_native"),
        source_read_mode="raw_single_query",
        environment=SimpleNamespace(root=tmp_path, database="dda_synthetic", source_database="dda_synthetic"),
    )
    opened = factory.open(Dataset("unicode", 32), case="test-only", clock=DeliveryClock())
    assert opened.source_read_mode == "raw_single_query"
    assert observed == ["raw_single_query"]


def test_pre_eof_fault_is_a_valid_observation():
    from tools.native_delivery_live_support.execution import Snapshot
    from tools.native_delivery_local.faults import Faults

    faults = Faults()
    faults.arm("during_source")
    with pytest.raises(RuntimeError, match="local_fixture.injected:during_source"):
        faults.fire("during_source")
    observed = Snapshot(rows=(), fault_events=tuple(faults.events))
    assert observed.fault_events == ("during_source",)
    assert not observed.pipeline_complete
    assert observed.publications == 0
