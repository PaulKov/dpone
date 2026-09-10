"""Fail closed on unsupported route cells before physical admission is invoked."""

from copy import deepcopy

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_execution import composition_transfer_cell
from tests.test_composition_source_admission import transfer


def test_explicit_postgres_to_mssql_full_refresh_cell():
    assert composition_transfer_cell(transfer()) == "postgres_mssql_full_refresh_v1"


def test_clickhouse_cell_is_not_authority_for_an_ordinary_workload():
    manifest = transfer()
    manifest.pop("state")
    manifest["source"]["type"] = "mssql"
    manifest["sink"]["type"] = "clickhouse"
    with pytest.raises(CompositionAdmissionError, match="transfer_route_capability"):
        composition_transfer_cell(manifest)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda m: m["sink"]["strategy"].update(mode="incremental_merge"),
        lambda m: m["source"].update(type="mysql"),
        lambda m: m["source"].update(query="DELETE FROM synthetic"),
        lambda m: m["source"].update(sql_file="unbounded.sql"),
        lambda m: m.update(pre_run="arbitrary command"),
        lambda m: m.update(backfill={"enabled": True}),
        lambda m: m["sink"]["strategy"].update(custom_predicate="1=1"),
        lambda m: m.pop("state"),
    ],
)
def test_unsupported_source_strategy_and_side_effects_reject_entire_route(mutation):
    manifest = deepcopy(transfer())
    mutation(manifest)
    with pytest.raises(CompositionAdmissionError):
        composition_transfer_cell(manifest)


def test_real_native_model_producer_full_refresh_is_classified_without_rewriting():
    from copy import deepcopy

    from dpone.contracts.composition_execution import composition_generated_transfer_cell
    from dpone.contracts.dbt_publish_models import (
        DbtColumnArtifact,
        DbtModelArtifact,
        DbtPublishIntent,
        DbtPublishProfile,
        DbtPublishStrategyPolicy,
    )
    from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
    from dpone.services.dbt_publish_planning import DbtPublishPlanner

    model = DbtModelArtifact(
        unique_id="model.synthetic.sample",
        name="sample",
        original_file_path="models/sample.sql",
        database="synthetic",
        schema="native",
        alias="sample",
        materialized="table",
        contract_enforced=True,
        columns=("id",),
        column_contracts=(DbtColumnArtifact("id", "int", False),),
        group=None,
        tags=(),
        meta={},
        unique_key=(),
        depends_on=(),
    )
    profile = DbtPublishProfile(
        "synthetic",
        "mssql",
        "native_reader",
        "clickhouse",
        "snapshot_writer",
        "synthetic",
        None,
        "example.invalid/runtime@sha256:" + "a" * 64,
    )
    compiled = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()).compile(
        model,
        DbtPublishIntent(True, "synthetic", "synthetic_flow", strategy_mode="full_refresh"),
        profile,
        DbtPublishStrategyPolicy(
            ("full_refresh",), full_refresh_authorized=True, full_refresh_max_source_bytes=1048576
        ),
    )
    original = deepcopy(compiled.manifest)
    assert composition_generated_transfer_cell(compiled.manifest) == "mssql_clickhouse_full_refresh_v1"
    assert compiled.manifest == original
    assert {"runtime", "quality", "gitops"} <= set(original)
    assert original["sink"]["strategy"]["max_source_bytes"] == 1048576
