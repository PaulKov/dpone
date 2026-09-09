"""Pure selection policy retains CLI/preview semantics without running dbt."""

import json
from dataclasses import replace

import pytest

from dpone.contracts import dbt_selection as policy
from tests.test_dbt_selection_resolution import _manifest

ROOTS = ("model.analytics.orders",)


@pytest.mark.parametrize("authority", ["dbt_cli", "manifest_preview"])
def test_plan_completes_the_same_graph_for_both_authorities(authority):
    body = _manifest()
    plan = policy.DbtSelectionPlan.from_manifest(body, ROOTS)
    manifest = json.loads(body)
    graph = policy.manifest_preview_selected_graph(manifest, ROOTS)
    selection = plan.complete(manifest, graph, authority=authority)
    assert selection.authority == authority
    assert selection.selectors == ("+fqn:analytics.orders",)
    assert selection.selected_graph_unique_ids == graph
    assert selection.expected_run_result_unique_ids == graph
    assert selection.invocation_target is None
    assert replace(selection, authority="dbt_cli") == plan.complete(manifest, graph, authority="dbt_cli")


@pytest.mark.parametrize("field", ["generated_at", "invocation_id", "env", "user_id"])
def test_manifest_identity_ignores_only_volatile_metadata(field):
    body = _manifest()
    parsed = json.loads(body)
    parsed["metadata"][field] = "another invocation"
    parsed["nodes"][ROOTS[0]]["created_at"] = 123
    plan = policy.DbtSelectionPlan.from_manifest(body, ROOTS)
    plan.require_matching_manifest(json.dumps(parsed).encode())
    parsed["nodes"][ROOTS[0]]["raw_code"] = "select 'changed'"
    with pytest.raises(ValueError, match="does not describe the captured project snapshot"):
        plan.require_matching_manifest(json.dumps(parsed).encode())


@pytest.mark.parametrize("fqn", [None, "analytics.orders", [], ["analytics", "bad name"], ["analytics", 1]])
def test_plan_rejects_unsafe_fqn_before_any_resolution(fqn):
    manifest = json.loads(_manifest())
    manifest["nodes"][ROOTS[0]]["fqn"] = fqn
    with pytest.raises(ValueError, match="no safe exact dbt FQN"):
        policy.DbtSelectionPlan.from_manifest(json.dumps(manifest).encode(), ROOTS)


def test_plan_rejects_colliding_selectors():
    with pytest.raises(ValueError, match="colliding dbt FQN"):
        policy.DbtSelectionPlan.from_manifest(_manifest(), ROOTS + ROOTS)


def test_initial_manifest_and_parsed_manifest_keep_distinct_json_errors():
    with pytest.raises(ValueError, match="selection input must be a strict JSON object"):
        policy.DbtSelectionPlan.from_manifest(b'{"nodes":{},"nodes":{}}', ROOTS)
    plan = policy.DbtSelectionPlan.from_manifest(_manifest(), ROOTS)
    with pytest.raises(ValueError, match="manifest is invalid JSON"):
        plan.require_matching_manifest(b'{"nodes":{},"nodes":{}}')


def test_preview_includes_ancestors_but_semantic_refresh_keeps_exact_roots():
    manifest = json.loads(_manifest())
    preview = policy.manifest_preview_selected_graph(manifest, ROOTS)
    refresh = policy.semantic_refresh_preview_selected_graph(manifest, ROOTS)
    assert set(preview) - set(refresh) == {"model.analytics.stg_orders"}
    assert refresh == (ROOTS[0], "test.analytics.orders_not_null", "unit_test.analytics.orders_unit")


@pytest.mark.parametrize(
    "authority,message",
    [
        ("dbt_cli", "dbt selection omitted a publish-enabled model"),
        ("manifest_preview", "a publish-enabled model cannot be ephemeral"),
    ],
)
def test_missing_publish_result_retains_authority_specific_error(authority, message):
    plan = policy.DbtSelectionPlan.from_manifest(_manifest(), ROOTS)
    manifest = json.loads(_manifest())
    with pytest.raises(ValueError, match=message):
        plan.complete(manifest, ("model.analytics.stg_orders",), authority=authority)


def test_graph_policy_rejection_precedes_missing_publish_result():
    plan = policy.DbtSelectionPlan.from_manifest(_manifest(), ROOTS)
    manifest = json.loads(_manifest())
    manifest["nodes"]["model.analytics.stg_orders"]["config"]["pre-hook"] = ["delete from unrelated"]
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    with pytest.raises(DbtPublishingError):
        plan.complete(manifest, ("model.analytics.stg_orders",), authority="dbt_cli")
