from __future__ import annotations

import pytest

from dpone.contracts.dbt_publish_models import (
    DbtColumnArtifact,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
)
from dpone.contracts.dbt_semantic_refresh_project_overlay import (
    dbt_project_models_config,
    semantic_refresh_project_overlay,
)
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler

DIGEST = "sha256:" + "a" * 64


def test_platform_overlay_injects_scope_merge_for_exact_selected_fqn() -> None:
    overlay = semantic_refresh_project_overlay((("analytics", "marts", "events"),))

    assert dbt_project_models_config(overlay) == {
        "models": {
            "analytics": {
                "marts": {
                    "events": {
                        "+contract": {"enforced": True},
                        "+incremental_strategy": "dpone_scope_merge",
                        "+materialized": "incremental",
                        "+on_schema_change": "fail",
                    }
                }
            }
        }
    }
    assert overlay["project_overlay_sha256"].startswith("sha256:")


def test_platform_overlay_rejects_parent_child_overlap() -> None:
    with pytest.raises(ValueError, match="overlapping"):
        semantic_refresh_project_overlay(
            (
                ("analytics", "events"),
                ("analytics", "events", "child"),
            )
        )


def test_semantic_model_compiler_never_emits_generic_transfer_manifest() -> None:
    model = DbtModelArtifact(
        unique_id="model.analytics.events",
        name="events",
        original_file_path="models/events.sql",
        database="DWH",
        schema="mart",
        alias="events",
        materialized="table",
        contract_enforced=True,
        columns=("event_id",),
        column_contracts=(DbtColumnArtifact("event_id", "bigint", False),),
        group="analytics",
        tags=(),
        meta={},
        unique_key=("event_id",),
        depends_on=(),
        fqn=("analytics", "events"),
    )
    profile = DbtPublishProfile(
        name="semantic",
        source_type="mssql",
        source_connection_ref="mssql_prod",
        sink_type="clickhouse",
        sink_connection_ref="clickhouse_prod",
        target_schema="mart",
        staging_schema="staging",
        runtime_image=DIGEST,
        semantic_refresh=_semantic_profile(),
    )
    compiler = DbtModelToWorkloadCompiler(planner=_Planner())

    compiled = compiler.compile(
        model,
        DbtPublishIntent(True, "semantic", "daily"),
        profile,
        DbtPublishStrategyPolicy(("incremental_merge",)),
        supported_strategies=(),
    )

    assert compiled.manifest == {
        "schema": "dpone.dbt-semantic-refresh-model-template.v1",
        "mode": "semantic_refresh_v2",
        "model_unique_id": "model.analytics.events",
        "logical_output_asset_uri": "clickhouse://mart/events",
    }
    assert "source" not in compiled.manifest
    assert "sink" not in compiled.manifest


class _Planner:
    def strategy(self, *_args, **_kwargs):
        raise AssertionError("semantic refresh must not invoke the generic strategy planner")

    def strategy_candidates(self, *_args, **_kwargs):
        return ()

    def physical_design(self, *_args, **_kwargs):
        raise AssertionError("semantic refresh must not invoke generic physical design")


def _semantic_profile() -> SemanticRefreshProfilePolicy:
    return SemanticRefreshProfilePolicy.from_mapping(
        {
            "schema": "dpone.semantic-refresh-profile.v1",
            "enabled": True,
            "capability": "scope_stable_event_fact",
            "scope": {"grain": "day", "timezone": "UTC", "interval": "half_open"},
            "mutation": {"protocol": "update_insert_v1", "deletes": "ignore_missing"},
            "initial_load": "require_existing_complete_relation",
            "concurrency": "exclusive_workflow",
            "source_snapshot": "snapshot",
            "publication": {
                "database_engine": "Atomic",
                "table_engine": "MergeTree",
                "replica_count": 1,
                "strategy": "full_table_exchange",
            },
            "workflow_publish_atomicity": "none",
            "automatic_sql_retry": False,
        }
    )
