"""Bootstrap discovery shares protected verification and never invents an attempt."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_context as module
from dpone.contracts.composition_dispatcher_binding import CompositionDispatcherBinding
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_dispatcher_context import DISPATCHER, SHA, _staged


def staged(monkeypatch, tmp_path):
    loader, arguments, body, runtime, calls = _staged(monkeypatch, tmp_path)
    loader._identity = CompositionDispatcherBinding(DISPATCHER, "dispatcher", service_policy_sha256=SHA)
    runtime.connection_registry["connections"]["registry-target"]["connection"]["composition_dispatcher"] = {
        "schema": "dpone.composition-dispatcher-binding.v2",
        "dispatcher_id": DISPATCHER,
        "connection_ref": "api-dispatcher",
        "service_policy_sha256": SHA,
    }
    plan = module.reopen_composition_plan(tmp_path, "unused")
    monkeypatch.setattr(module, "_select_attempt", lambda *_: pytest.fail("bootstrap invented an attempt"))
    return loader, body, runtime, calls, plan


def alias(runtime, plan, name, *, dispatcher=DISPATCHER):
    reference = "registry-" + name
    runtime.binding_set["bindings"][name] = {"connection_ref": reference}
    entry = deepcopy(runtime.connection_registry["connections"]["registry-target"])
    entry["connection"]["composition_dispatcher"]["dispatcher_id"] = dispatcher
    runtime.connection_registry["connections"][reference] = entry
    plan.writes.append(SimpleNamespace(connector="clickhouse", kind="transfer", connection_ref=name))
    return entry["connection"]["composition_dispatcher"]


def test_bootstrap_derives_sorted_unique_matching_aliases_once_without_credentials(monkeypatch, tmp_path):
    loader, body, runtime, calls, plan = staged(monkeypatch, tmp_path)
    alias(runtime, plan, "z-target")
    alias(runtime, plan, "a-target")
    alias(runtime, plan, "other", dispatcher="22222222-2222-4222-8222-222222222222")
    plan.writes.append(plan.writes[0])
    result = loader.load_bootstrap(SHA)
    assert tuple(context.target_binding_ref for context in result) == ("a-target", "target", "z-target")
    assert all(context.plan is plan and context.runtime is runtime for context in result)
    assert all(context.binding.identity_kind == "policy" for context in result)
    assert all(context.occurrence.activation_id == body["deployment_identity"]["activation_id"] for context in result)
    assert len(calls) == 1 and len(calls[0]) == 3


@pytest.mark.parametrize("bad", ["kind", "hash", "open"])
def test_any_same_dispatcher_bad_binding_rejects_entire_bootstrap(monkeypatch, tmp_path, bad):
    loader, _, runtime, _, plan = staged(monkeypatch, tmp_path)
    binding = alias(runtime, plan, "z-second")
    if bad == "kind":
        binding["schema"] = "dpone.composition-dispatcher-binding.v1"
        binding["service_configuration_sha256"] = binding.pop("service_policy_sha256")
    elif bad == "hash":
        binding["service_policy_sha256"] = "sha256:" + "f" * 64
    else:
        binding["extra"] = True
    with pytest.raises(CompositionAdmissionError):
        loader.load_bootstrap(SHA)


@pytest.mark.parametrize("empty", ["no_writes", "other_dispatcher", "no_binding"])
def test_bootstrap_requires_at_least_one_enrolled_matching_alias(monkeypatch, tmp_path, empty):
    loader, _, runtime, _, plan = staged(monkeypatch, tmp_path)
    entry = runtime.connection_registry["connections"]["registry-target"]["connection"]
    if empty == "no_writes":
        plan.writes.clear()
    elif empty == "other_dispatcher":
        entry["composition_dispatcher"]["dispatcher_id"] = "22222222-2222-4222-8222-222222222222"
    else:
        del entry["composition_dispatcher"]
    with pytest.raises(CompositionAdmissionError):
        loader.load_bootstrap(SHA)


@pytest.mark.parametrize("drift", ["plan", "metadata", "authority", "runtime", "api"])
def test_bootstrap_uses_existing_protected_verification(monkeypatch, tmp_path, drift):
    loader, body, runtime, _, plan = staged(monkeypatch, tmp_path)
    authority = SHA
    if drift == "plan":
        plan.sources.subject_sha256 = "sha256:" + "f" * 64
    elif drift == "metadata":
        body["plan_sha256"] = "sha256:" + "f" * 64
    elif drift == "authority":
        authority = "sha256:" + "f" * 64
    elif drift == "runtime":
        from dataclasses import replace

        loader._runtime_loader = SimpleNamespace(load=lambda _: replace(runtime, environment="foreign"))
    else:
        runtime.connection_registry["connections"]["registry-api"]["type"] = "mssql"
    with pytest.raises(CompositionAdmissionError):
        loader.load_bootstrap(authority)
