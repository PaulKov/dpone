"""Publication and runtime share manifest semantics, not execution evidence."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_selected_graph_observation import observe_dbt_selected_graph
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest


def test_observation_is_non_mutating_and_matches_locked_graph():
    pack, manifest = _pack(), _preflight_manifest()
    before = deepcopy(manifest)
    observed = observe_dbt_selected_graph(manifest, lock=pack.selection_lock, logical_target=("DWH", "mart"))
    observed.require_matches(pack.selection_lock, pack.selection_lock.selected_graph_unique_ids)
    assert observed.graph_contract_sha256 == pack.selection_lock.graph_contract_sha256
    assert observed.expected_run_result_unique_ids == pack.selection_lock.expected_run_result_unique_ids
    assert manifest == before


def test_logical_target_mismatch_keeps_priority_over_graph_policy_failure():
    pack, manifest = _pack(), _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["schema"] = "wrong"
    manifest["nodes"]["model.analytics.orders"]["config"]["pre-hook"] = ["delete from unrelated"]
    with pytest.raises(DbtPublishingError) as raised:
        observe_dbt_selected_graph(manifest, lock=pack.selection_lock, logical_target=("DWH", "mart"))
    assert raised.value.code == "DPONE_DBT_TARGET_IDENTITY_MISMATCH"


def test_policy_rejection_is_not_hidden_by_graph_digest_drift():
    pack, manifest = _pack(), _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["config"]["pre-hook"] = ["delete from unrelated"]
    with pytest.raises(DbtPublishingError) as raised:
        observe_dbt_selected_graph(manifest, lock=pack.selection_lock, logical_target=("DWH", "mart"))
    assert raised.value.code == "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED"


@pytest.mark.parametrize("drift", ["selected", "results", "graph"])
def test_every_selection_observation_must_match(drift):
    pack = _pack()
    observed = observe_dbt_selected_graph(
        _preflight_manifest(), lock=pack.selection_lock, logical_target=("DWH", "mart")
    )
    selected = pack.selection_lock.selected_graph_unique_ids
    if drift == "selected":
        selected = selected[:-1]
    elif drift == "results":
        observed = replace(observed, expected_run_result_unique_ids=selected[:-1])
    else:
        observed = replace(observed, graph_contract_sha256="sha256:" + "f" * 64)
    with pytest.raises(DbtPublishingError) as raised:
        observed.require_matches(pack.selection_lock, selected)
    assert raised.value.code == "DPONE_DBT_SELECTION_DRIFT"
