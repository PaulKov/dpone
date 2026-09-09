"""Pin adapter-triggered drops even when no selected node names their macros."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint
from dpone.contracts.dbt_graph_contract import dbt_publish_logical_target
from dpone.contracts.dbt_selected_graph_observation import observe_dbt_selected_graph
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    dbt_sqlserver_graph_contract_sha256,
)
from dpone.contracts.dbt_sqlserver_macro_authority import (
    DBT_SQLSERVER_MACRO_AUTHORITY_INVALID,
    evaluate_dbt_sqlserver_macro_authority,
)
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import (
    DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS,
    DBT_SQLSERVER_TRUSTED_ROOTS,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples/dbt-inline-publishing/fixtures/manifest.v12.json"
DROP_CHAIN = (
    "macro.dbt.drop_relation",
    "macro.dbt.default__drop_relation",
    "macro.dbt.get_drop_sql",
    "macro.dbt_sqlserver.sqlserver__get_drop_sql",
)
DROP_HELPERS = (
    "macro.dbt.statement",
    "macro.dbt_sqlserver.get_use_database_sql",
    "macro.dbt_sqlserver.sqlserver__get_use_database_sql",
    "macro.dbt_sqlserver.information_schema_hints",
    "macro.dbt_sqlserver.sqlserver__information_schema_hints",
    "macro.dbt_sqlserver.get_query_options",
    "macro.dbt.escape_single_quotes",
    "macro.dbt.default__escape_single_quotes",
)
OTHER_ADAPTER_CHAINS = (
    ("macro.dbt.rename_relation", "macro.dbt_sqlserver.sqlserver__rename_relation"),
    (
        "macro.dbt.get_columns_in_relation",
        "macro.dbt_sqlserver.sqlserver__get_columns_in_relation",
        "macro.dbt.sql_convert_columns_in_relation",
    ),
    ("macro.dbt.list_relations_without_caching", "macro.dbt_sqlserver.sqlserver__list_relations_without_caching"),
)
# Observed from a6669cd36ceed063bb1e0c479039d0caf1355e2c and the unchanged
# checked-in fixture, before adding the Python-dispatched drop root.
PRE_DROP_POLICY = "sha256:a285c5ce85de3b18f32173fe2d837d6486b366d67eeaddf8cf56b33be9a94470"
PRE_DROP_GRAPH = "sha256:ffe74da87f74debcfe55ac50ea53f8ba5ef8b842ae2da1fbab832643db1db895"
MODEL_ID = "model.dpone_dbt_demo.competitive_pricing"


@pytest.fixture
def manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_python_dispatched_drop_has_complete_pinned_closure() -> None:
    records = {record[0]: record for record in DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS}
    assert DROP_CHAIN[0] in DBT_SQLSERVER_TRUSTED_ROOTS
    reached: set[str] = set()
    pending = [DROP_CHAIN[0]]
    while pending:
        unique_id = pending.pop()
        if unique_id not in reached:
            reached.add(unique_id)
            pending.extend(records[unique_id][4])

    assert reached == set((*DROP_CHAIN, *DROP_HELPERS))


@pytest.mark.parametrize("chain", OTHER_ADAPTER_CHAINS)
def test_python_dispatched_lifecycle_helpers_have_pinned_roots(chain: tuple[str, ...]) -> None:
    records = {record[0]: record for record in DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS}
    assert chain[0] in DBT_SQLSERVER_TRUSTED_ROOTS
    for parent, child in zip(chain, chain[1:]):
        assert child in records[parent][4]


@pytest.mark.parametrize(
    "unique_id", (*DROP_CHAIN, *DROP_HELPERS, *(node for chain in OTHER_ADAPTER_CHAINS for node in chain))
)
@pytest.mark.parametrize("mutation", ("missing", "body", "dependencies"))
def test_drop_closure_drift_fails_without_selected_node_dependency(
    manifest: dict[str, Any], unique_id: str, mutation: str
) -> None:
    assert evaluate_dbt_sqlserver_macro_authority(manifest, (MODEL_ID,)).passed
    if mutation == "missing":
        manifest["macros"].pop(unique_id)
    elif mutation == "body":
        manifest["macros"][unique_id]["macro_sql"] += "\n-- changed destructive authority\n"
    else:
        manifest["macros"][unique_id]["depends_on"]["macros"].append("macro.untrusted.runtime_sql")

    report = evaluate_dbt_sqlserver_macro_authority(manifest, (MODEL_ID,))

    assert not report.passed
    assert report.projection_sha256 is None
    assert {issue.code for issue in report.issues} == {DBT_SQLSERVER_MACRO_AUTHORITY_INVALID}
    assert any(issue.unique_id == unique_id for issue in report.issues)


@pytest.mark.parametrize(
    "family",
    ("drop_relation", "get_drop_sql", "rename_relation", "get_columns_in_relation", "list_relations_without_caching"),
)
@pytest.mark.parametrize("prefix", ("", "default__", "sqlserver__"))
def test_drop_dispatch_shadow_fails_without_selected_node_dependency(
    manifest: dict[str, Any], family: str, prefix: str
) -> None:
    name = f"{prefix}{family}"
    unique_id = f"macro.analytics_package.{name}"
    manifest["macros"][unique_id] = {
        "unique_id": unique_id,
        "resource_type": "macro",
        "package_name": "analytics_package",
        "name": name,
        "macro_sql": f"{{% macro {name}(relation) %}}{{% endmacro %}}",
        "depends_on": {"macros": []},
    }

    assert evaluate_dbt_sqlserver_macro_authority(_pristine_manifest(), (MODEL_ID,)).passed
    report = evaluate_dbt_sqlserver_macro_authority(manifest, (MODEL_ID,))

    assert not report.passed
    assert report.projection_sha256 is None
    assert any(issue.unique_id == unique_id and issue.field == "name" for issue in report.issues)


def test_candidate_diff_exposes_changed_recursive_drop(manifest: dict[str, Any], tmp_path: Path) -> None:
    unique_id = DROP_CHAIN[-1]
    body = manifest["macros"][unique_id]["macro_sql"]
    assert "fetch_result=true, auto_begin=false" in body
    manifest["macros"][unique_id]["macro_sql"] = body.replace(
        "fetch_result=true, auto_begin=false", "fetch_result=true, auto_begin=true"
    )
    candidate = tmp_path / "candidate.json"
    candidate.write_text(json.dumps(manifest), encoding="utf-8")
    output = tmp_path / "diff.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/dbt_self_service/generate_sqlserver_macro_authority.py"),
            "--candidate-manifest",
            str(candidate),
            "--diff-output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    report = json.loads(output.read_text(encoding="utf-8"))
    assert [row["unique_id"] for row in report["framework_macro_records"]["changed"]] == [unique_id]
    assert report["framework_macro_records"]["added"] == []
    assert report["framework_macro_records"]["removed"] == []
    assert [row["unique_id"] for row in report["new_or_changed_execution_capable_macros"]] == [unique_id]
    assert {
        "trusted_root": DROP_CHAIN[0],
        "target_unique_id": unique_id,
        "path": list(DROP_CHAIN),
    } in report["trusted_root_paths_to_new_or_changed_execution_capable_macros"]


def test_pre_drop_policy_is_rejected_even_with_recomputed_selection_digest(manifest: dict[str, Any]) -> None:
    payload = _selection(manifest).to_dict()
    payload["graph_policy_sha256"] = PRE_DROP_POLICY
    payload.pop("selection_sha256")
    payload["selection_sha256"] = canonical_fingerprint(payload)

    with pytest.raises(DbtPublishingError, match="graph policy does not match") as caught:
        DbtSelectionLock.from_mapping(payload)

    assert caught.value.code == "DPONE_DBT_SELECTION_INVALID"


def test_current_policy_cannot_reuse_pre_drop_graph_projection(manifest: dict[str, Any]) -> None:
    current = _selection(manifest)
    target = dbt_publish_logical_target(manifest, (MODEL_ID,))
    observation = observe_dbt_selected_graph(manifest, lock=current, logical_target=target)
    observation.require_matches(current, (MODEL_ID,))
    payload = current.to_dict()
    payload["graph_contract_sha256"] = PRE_DROP_GRAPH
    payload.pop("selection_sha256")
    payload["selection_sha256"] = canonical_fingerprint(payload)
    stale = DbtSelectionLock.from_mapping(payload)

    with pytest.raises(DbtPublishingError) as caught:
        observe_dbt_selected_graph(manifest, lock=stale, logical_target=target).require_matches(stale, (MODEL_ID,))

    assert caught.value.code == "DPONE_DBT_SELECTION_DRIFT"


def _selection(manifest: dict[str, Any]) -> DbtSelectionLock:
    return DbtSelectionLock.build(
        manifest_sha256=canonical_fingerprint(manifest),
        toolchain_sha256="sha256:" + "a" * 64,
        invocation_context_sha256="sha256:" + "b" * 64,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(manifest, (MODEL_ID,)),
        selectors=(MODEL_ID,),
        selected_graph_unique_ids=(MODEL_ID,),
        expected_run_result_unique_ids=(MODEL_ID,),
        publish_model_unique_ids=(MODEL_ID,),
    )


def _pristine_manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
