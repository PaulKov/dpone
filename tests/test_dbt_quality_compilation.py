from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.dbt_publish_models import DbtPublishStrategyPolicy
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"


def test_dbt_profile_quality_compiles_to_executable_gates() -> None:
    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )

    assert report.passed
    for model in report.models:
        quality = model.manifest["quality"]
        assert quality["acceptance"] == {"enabled": True, "mode": "required"}
        assert quality["gates"] == [
            {
                "id": "source_target_rows",
                "type": "row_count_reconciliation",
                "severity": "error",
                "tolerance": {"mode": "pct", "value": 0.0},
            }
        ]


def test_unknown_dbt_contract_type_fails_before_artifact_generation() -> None:
    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )
    assert report.passed
    compiled = report.models[0]
    unknown_column = replace(
        compiled.model.column_contracts[0],
        data_type="adapter_private_type",
    )
    model = replace(
        compiled.model,
        column_contracts=(unknown_column, *compiled.model.column_contracts[1:]),
    )

    result = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()).compile(
        model,
        compiled.intent,
        compiled.profile,
        DbtPublishStrategyPolicy(
            allowed_strategies=("incremental_merge", "partition_replace"),
        ),
        supported_strategies=("partition_replace",),
    )

    assert "DPONE_DBT_CONTRACT_TYPE_UNSUPPORTED" in {issue.code for issue in result.warnings}
