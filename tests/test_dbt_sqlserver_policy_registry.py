"""Finite graph selection preserves legacy bytes and rejects unavailable families."""

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_selected_graph_observation import observe_dbt_selected_graph
from dpone.contracts.dbt_sqlserver_graph_policy import (
    dbt_sqlserver_graph_contract_sha256 as legacy_hash,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    evaluate_dbt_sqlserver_selected_graph as legacy_evaluate,
)
from dpone.contracts.dbt_sqlserver_policy_registration import DbtSqlserverGraphRegistration
from dpone.contracts.dbt_sqlserver_policy_registry import (
    dbt_sqlserver_graph_contract_sha256,
    evaluate_dbt_sqlserver_selected_graph,
    require_graph_registration,
)
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest

LEGACY_ID = "dpone.dbt-sqlserver-selected-graph-policy.v1"
LEGACY_SHA = "sha256:433e19b5637e06a10c0ec3add271fd4e5182a0e278a132a875ccc97fe9ab4b1f"


def test_exact_legacy_registration_is_immutable_and_finite():
    selected = require_graph_registration(graph_policy_id=LEGACY_ID, graph_policy_sha256=LEGACY_SHA)
    assert selected == DbtSqlserverGraphRegistration(LEGACY_ID, LEGACY_SHA)
    with pytest.raises(FrozenInstanceError):
        selected.graph_policy_id = "other"


@pytest.mark.parametrize(
    "identity,digest",
    [
        ("dpone.dbt-sqlserver-selected-graph-policy.v2", LEGACY_SHA),
        (LEGACY_ID, "sha256:" + "0" * 64),
        ("dpone_managed_table", LEGACY_SHA),
        (None, LEGACY_SHA),
        (LEGACY_ID, None),
    ],
)
def test_unknown_managed_or_mixed_pair_has_no_fallback(identity, digest):
    with pytest.raises(ValueError, match="supported SQL Server policy"):
        require_graph_registration(graph_policy_id=identity, graph_policy_sha256=digest)


@pytest.mark.parametrize("explicit", [False, True])
def test_selected_evaluation_and_hash_preserve_exact_legacy_bytes(explicit):
    manifest = _preflight_manifest()
    before = deepcopy(manifest)
    lock = _pack().selection_lock
    selected = (
        require_graph_registration(graph_policy_id=LEGACY_ID, graph_policy_sha256=LEGACY_SHA) if explicit else None
    )
    actual = evaluate_dbt_sqlserver_selected_graph(
        manifest, lock.selected_graph_unique_ids, expected_logical_target=("DWH", "mart"), registration=selected
    )
    assert actual == legacy_evaluate(manifest, lock.selected_graph_unique_ids, expected_logical_target=("DWH", "mart"))
    assert dbt_sqlserver_graph_contract_sha256(
        manifest, lock.selected_graph_unique_ids, registration=selected
    ) == legacy_hash(manifest, lock.selected_graph_unique_ids)
    assert manifest == before


@pytest.mark.parametrize("field,value", [("graph_policy_id", "managed"), ("graph_policy_sha256", "sha256:" + "0" * 64)])
def test_selection_lock_rejects_pair_before_fingerprint_validation(field, value):
    with pytest.raises(DbtPublishingError, match="supported SQL Server policy") as failure:
        replace(_pack().selection_lock, **{field: value})
    assert failure.value.code == "DPONE_DBT_SELECTION_INVALID"


def test_observation_rechecks_pair_and_preserves_target_error_priority():
    lock = _pack().selection_lock
    object.__setattr__(lock, "graph_policy_id", "managed")
    manifest = _preflight_manifest()
    with pytest.raises(DbtPublishingError) as failure:
        observe_dbt_selected_graph(manifest, lock=lock, logical_target=("DWH", "mart"))
    assert failure.value.code == "DPONE_DBT_SELECTION_DRIFT"
    with pytest.raises(DbtPublishingError) as failure:
        observe_dbt_selected_graph(manifest, lock=lock, logical_target=("other", "mart"))
    assert failure.value.code == "DPONE_DBT_TARGET_IDENTITY_MISMATCH"


def test_legacy_lock_graph_and_execution_wire_hashes_are_unchanged():
    from dpone.contracts.dbt_contract_validation import canonical_fingerprint

    pack = _pack()
    # Captured from untouched source commit 5a903c4, not recomputed expectations.
    assert (
        pack.selection_lock.selection_sha256
        == "sha256:c5713d37c832fed4a2e2c8d971e6416692300d2c715b3586f2a1168a7e3c13df"
    )
    assert (
        pack.selection_lock.graph_contract_sha256
        == "sha256:afdd4620effeb973b520f84f0a52807d043c6ab0f05f842acd73644dbb3a1bbb"
    )
    assert (
        canonical_fingerprint(pack.to_dict())
        == "sha256:1124cbb5d8842c729d9abb7798bd13dec2bf0f12ee55488a642a16682ae295c0"
    )


@pytest.mark.parametrize(
    "record",
    [
        DbtSqlserverGraphRegistration("managed", LEGACY_SHA),
        DbtSqlserverGraphRegistration(LEGACY_ID, "sha256:" + "0" * 64),
        {"graph_policy_id": LEGACY_ID, "graph_policy_sha256": LEGACY_SHA},
    ],
)
def test_unregistered_record_rejects_before_manifest_evaluation_or_hash(record):
    for consumer in (evaluate_dbt_sqlserver_selected_graph, dbt_sqlserver_graph_contract_sha256):
        with pytest.raises(ValueError, match="supported SQL Server policy"):
            consumer(None, (), registration=record)


def test_observation_passes_one_resolved_registration_to_both_consumers(monkeypatch):
    import dpone.contracts.dbt_selected_graph_observation as observation

    seen = []
    evaluate = observation.evaluate_dbt_sqlserver_selected_graph
    fingerprint = observation.dbt_sqlserver_graph_contract_sha256

    def observe_evaluate(*args, **kwargs):
        seen.append(kwargs["registration"])
        return evaluate(*args, **kwargs)

    def observe_hash(*args, **kwargs):
        seen.append(kwargs["registration"])
        return fingerprint(*args, **kwargs)

    monkeypatch.setattr(observation, "evaluate_dbt_sqlserver_selected_graph", observe_evaluate)
    monkeypatch.setattr(observation, "dbt_sqlserver_graph_contract_sha256", observe_hash)
    lock = _pack().selection_lock
    result = observation.observe_dbt_selected_graph(_preflight_manifest(), lock=lock, logical_target=("DWH", "mart"))
    result.require_matches(lock, lock.selected_graph_unique_ids)
    assert len(seen) == 2 and seen[0] is seen[1]
    assert seen[0] == require_graph_registration(graph_policy_id=LEGACY_ID, graph_policy_sha256=LEGACY_SHA)


def test_default_calls_legacy_signature_once_without_retry(monkeypatch):
    import dpone.contracts.dbt_sqlserver_policy_registry as registry

    calls = []

    def old_evaluator(manifest, selected_graph_unique_ids, *, expected_logical_target=None):
        calls.append((selected_graph_unique_ids, expected_logical_target))
        raise TypeError("internal legacy error")

    monkeypatch.setattr(registry, "_legacy_evaluate", old_evaluator)
    with pytest.raises(TypeError, match="internal legacy error"):
        registry.evaluate_dbt_sqlserver_selected_graph({}, ("model.a",))
    assert calls == [(("model.a",), None)]


@pytest.mark.parametrize("variant", ["subclass", "enum"])
def test_legacy_string_variants_remain_compatible_in_lookup_and_lock(variant):
    from enum import StrEnum

    class PolicyId(StrEnum):
        LEGACY = LEGACY_ID

    class PolicyDigest(StrEnum):
        LEGACY = LEGACY_SHA

    class LegacyString(str):
        pass

    identity, digest = (
        (PolicyId.LEGACY, PolicyDigest.LEGACY)
        if variant == "enum"
        else (LegacyString(LEGACY_ID), LegacyString(LEGACY_SHA))
    )
    selected = require_graph_registration(graph_policy_id=identity, graph_policy_sha256=digest)
    assert selected.graph_policy_id == LEGACY_ID
    assert selected.graph_policy_sha256 == LEGACY_SHA
    original = _pack().selection_lock
    lock = replace(original, graph_policy_id=identity, graph_policy_sha256=digest)
    assert lock.to_dict() == original.to_dict()
    assert lock.selection_sha256 == original.selection_sha256
    with pytest.raises(ValueError, match="supported SQL Server policy"):
        require_graph_registration(graph_policy_id=LegacyString("unknown"), graph_policy_sha256=digest)
