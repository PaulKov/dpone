from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.cli import main as cli_main
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.contracts.clickhouse_cluster_admission import (
    CLICKHOUSE_CLUSTER_ACCESS_TABLE_UNSUPPORTED,
    CLICKHOUSE_CLUSTER_DDL_SCOPE_REQUIRED,
    CLICKHOUSE_CLUSTER_ENGINE_REQUIRED,
    CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_REQUIRED,
    CLICKHOUSE_CLUSTER_REPLICATION_MODE_INVALID,
    CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED,
    CLICKHOUSE_CLUSTER_STAGING_DATABASE_UNSUPPORTED,
    ClickHouseClusterAdmissionInput,
    evaluate_clickhouse_cluster_admission,
)
from dpone.contracts.dbt_publish_models import DbtPublishStrategyPolicy
from dpone.manifest.models import LoadedManifest
from dpone.manifest.validation import validate_manifest
from dpone.readiness.managed_planning import ExecutionPlanService
from dpone.runtime.decision_audit import RuntimeDecisionContext
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import CLUSTER_RECEIPT_VERSION
from dpone.runtime.sinks.clickhouse_full_refresh_router import ClickHouseFullRefreshPublicationRouter
from dpone.runtime.sinks.clickhouse_full_refresh_staged import finalize_full_refresh
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner
from tests.test_dbt_quality_compilation import DEMO


def _request(**overrides: object) -> ClickHouseClusterAdmissionInput:
    values = {
        "sink_type": "clickhouse",
        "strategy_mode": "full_refresh",
        "max_source_bytes": 1024,
        "engine": "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
        "cluster_name": "analytics_cluster",
        "ddl_scope": "cluster",
        "replication_mode": "internal",
        "access_table_enabled": False,
        "target_database": "analytics",
        "staging_database": "analytics",
    }
    values.update(overrides)
    return ClickHouseClusterAdmissionInput(**values)


def test_cluster_admission_selects_only_complete_bounded_contract() -> None:
    decision = evaluate_clickhouse_cluster_admission(_request())

    assert decision.requested is True
    assert decision.selected is True
    assert decision.blockers == ()
    assert decision.mode == "cluster"
    assert decision.runtime_admission_required is True
    assert decision.no_fallback is True
    assert decision.replication_mode == "internal"


def test_external_cluster_admission_requires_explicit_mode_and_non_replicated_engine() -> None:
    decision = evaluate_clickhouse_cluster_admission(_request(replication_mode="external", engine="MergeTree"))

    assert decision.selected is True
    assert decision.mode == "cluster_external"
    assert decision.replication_mode == "external"
    assert decision.no_fallback is True


def test_external_cluster_admission_rejects_replicated_engine_without_fallback() -> None:
    decision = evaluate_clickhouse_cluster_admission(_request(replication_mode="external"))

    assert decision.selected is False
    assert decision.blockers == (CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_REQUIRED,)
    assert decision.no_fallback is True


def test_cluster_admission_rejects_unknown_replication_mode() -> None:
    decision = evaluate_clickhouse_cluster_admission(_request(replication_mode="automatic"))

    assert decision.selected is False
    assert decision.blockers == (CLICKHOUSE_CLUSTER_REPLICATION_MODE_INVALID,)


def test_local_full_refresh_remains_local_and_legacy_unbounded_compatible() -> None:
    bounded = evaluate_clickhouse_cluster_admission(_request(cluster_name=None, ddl_scope="local"))
    unbounded = evaluate_clickhouse_cluster_admission(
        _request(cluster_name=None, ddl_scope="local", max_source_bytes=None)
    )

    assert bounded.to_dict()["mode"] == "local"
    assert unbounded.to_dict()["mode"] == "local"
    assert bounded.blockers == unbounded.blockers == ()


def test_cluster_admission_reports_every_static_blocker_deterministically() -> None:
    decision = evaluate_clickhouse_cluster_admission(
        _request(
            max_source_bytes=0,
            engine="MergeTree",
            ddl_scope="local",
            access_table_enabled=True,
            staging_database="technical",
        )
    )

    assert decision.requested is True
    assert decision.selected is False
    assert decision.mode == "blocked"
    assert decision.blockers == (
        CLICKHOUSE_CLUSTER_ENGINE_REQUIRED,
        CLICKHOUSE_CLUSTER_STAGING_DATABASE_UNSUPPORTED,
        CLICKHOUSE_CLUSTER_ACCESS_TABLE_UNSUPPORTED,
        CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED,
        CLICKHOUSE_CLUSTER_DDL_SCOPE_REQUIRED,
    )


class _Publisher:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.prepared = 0
        self.published = 0
        self.cleaned = 0

    def is_enabled(self, _config: object) -> bool:
        return self.enabled

    def prepare_admission(self, config: object) -> object:
        self.prepared += 1
        return config

    def publish(self, *_args: object, **_kwargs: object) -> object:
        self.published += 1
        return SimpleNamespace(schema_version="receipt")

    def cleanup(self, _receipt: object) -> None:
        self.cleaned += 1


def _runtime_config(**options: object) -> LoadConfig:
    physical = {
        "storage": {
            "clickhouse": {
                "engine": "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
                "cluster": {"name": "analytics_cluster", "ddl_scope": "cluster"},
            }
        }
    }
    merged = {SOURCE_BYTE_BUDGET_OPTION: 1024, "physical_design": physical, **options}
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source",
        target_schema="analytics",
        target_table="target",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=merged,
    )


def test_router_selects_cluster_without_hidden_enable_flag() -> None:
    local, cluster = _Publisher(), _Publisher()
    router = ClickHouseFullRefreshPublicationRouter(local, cluster)

    router.prepare_admission(_runtime_config())
    router.publish(_runtime_config(), object(), staged_rows=1)

    assert cluster.prepared == cluster.published == 1
    assert local.prepared == local.published == 0


def test_router_publishes_cluster_selection_to_decision_audit() -> None:
    decisions: list[object] = []
    publisher = SimpleNamespace(publish=decisions.append)
    router = ClickHouseFullRefreshPublicationRouter(_Publisher(), _Publisher())

    with RuntimeDecisionContext.activate(publisher):
        router.prepare_admission(_runtime_config())

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision.decision_id == "clickhouse.full_refresh_publication"
    assert decision.requested == "cluster"
    assert decision.selected == "cluster"
    assert decision.fallback_allowed is False


def test_router_fails_closed_and_never_falls_back_for_invalid_cluster_request() -> None:
    local, cluster = _Publisher(), _Publisher()
    router = ClickHouseFullRefreshPublicationRouter(local, cluster)
    invalid = _runtime_config()
    invalid = replace(invalid, options={**invalid.options, SOURCE_BYTE_BUDGET_OPTION: 0})

    with pytest.raises(ValueError, match=CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED):
        router.prepare_admission(invalid)

    assert local.prepared == cluster.prepared == 0


def test_router_missing_cluster_budget_cannot_bypass_admission_or_fall_back() -> None:
    local, cluster = _Publisher(enabled=False), _Publisher()
    router = ClickHouseFullRefreshPublicationRouter(local, cluster)
    invalid = _runtime_config()
    options = dict(invalid.options)
    options.pop(SOURCE_BYTE_BUDGET_OPTION)
    invalid = replace(invalid, options=options)

    assert router.is_enabled(invalid) is True
    with pytest.raises(ValueError, match=CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED):
        router.prepare_admission(invalid)

    assert local.prepared == cluster.prepared == 0


def test_manifest_validation_reports_structured_cluster_blockers_before_runtime() -> None:
    options = {
        "physical_design": {
            "storage": {
                "clickhouse": {
                    "engine": "MergeTree",
                    "cluster": {"name": "analytics_cluster", "ddl_scope": "local"},
                    "access_table": {"name": "target_all"},
                }
            }
        }
    }
    load_config = SimpleNamespace(
        target_schema="analytics",
        target_table="target",
        staging_schema="technical",
        load_strategy=SimpleNamespace(value="full_refresh"),
        options=options,
        reconciliation=False,
    )
    spec = SimpleNamespace(
        name="load_target",
        selector="load_target",
        config=SimpleNamespace(load_config=load_config),
        raw_config={
            "source": {"type": "mssql"},
            "sink": {
                "type": "clickhouse",
                "strategy": {"mode": "full_refresh"},
                "options": options,
            },
        },
    )
    manifest = LoadedManifest(
        path=Path(__file__),
        kind="dpone.batch.v1",
        raw={},
        processes=(spec,),
    )

    issues = validate_manifest(manifest)

    assert [issue.code for issue in issues] == [
        "CLICKHOUSE_CLUSTER_PUBLICATION_ENGINE_MUST_BE_REPLICATED_MERGE_TREE",
        "CLICKHOUSE_CLUSTER_PUBLICATION_STAGING_DATABASE_MUST_MATCH_TARGET",
        "CLICKHOUSE_CLUSTER_PUBLICATION_ACCESS_TABLE_UNSUPPORTED",
        "CLICKHOUSE_CLUSTER_PUBLICATION_MAX_SOURCE_BYTES_REQUIRED",
        "CLICKHOUSE_CLUSTER_PUBLICATION_DDL_SCOPE_MUST_BE_CLUSTER",
    ]


def test_dpone_check_exposes_cluster_blockers_as_structured_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    relative = Path("pipelines/cluster_snapshot/pipeline.yaml")
    source = tmp_path / relative
    source.parent.mkdir(parents=True)
    source.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.flow.v1",
                "authoring": {"mode": "flow", "source": relative.as_posix()},
                "metadata": {"id": "cluster_snapshot", "domain": "analytics", "airflow": False},
                "processes": [
                    {
                        "name": "cluster_snapshot",
                        "source": {
                            "type": "mssql",
                            "connection_ref": "source",
                            "table": {"schema": "dbo", "name": "source_table"},
                        },
                        "sink": {
                            "type": "clickhouse",
                            "connection_ref": "target",
                            "table": {"schema": "analytics", "name": "target_table"},
                            "staging": {"schema": "technical"},
                            "strategy": {"mode": "full_refresh"},
                            "options": {
                                "physical_design": {
                                    "storage": {
                                        "clickhouse": {
                                            "engine": "MergeTree",
                                            "cluster": {
                                                "name": "analytics_cluster",
                                                "ddl_scope": "local",
                                            },
                                        }
                                    }
                                }
                            },
                        },
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as raised:
        cli_main.main(["check", "pipelines/cluster_snapshot", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert int(raised.value.code) == 1
    codes = {error["code"] for error in payload["errors"]}
    assert {
        "CLICKHOUSE_CLUSTER_PUBLICATION_ENGINE_MUST_BE_REPLICATED_MERGE_TREE",
        "CLICKHOUSE_CLUSTER_PUBLICATION_STAGING_DATABASE_MUST_MATCH_TARGET",
        "CLICKHOUSE_CLUSTER_PUBLICATION_MAX_SOURCE_BYTES_REQUIRED",
        "CLICKHOUSE_CLUSTER_PUBLICATION_DDL_SCOPE_MUST_BE_CLUSTER",
    } <= codes

    fixed = yaml.safe_load(source.read_text(encoding="utf-8"))
    fixed_sink = fixed["processes"][0]["sink"]
    fixed_sink["staging"]["schema"] = "analytics"
    fixed_sink["strategy"]["max_source_bytes"] = 1024
    clickhouse = fixed_sink["options"]["physical_design"]["storage"]["clickhouse"]
    clickhouse["engine"] = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
    clickhouse["cluster"]["ddl_scope"] = "cluster"
    source.write_text(yaml.safe_dump(fixed, sort_keys=False), encoding="utf-8")

    with pytest.raises(SystemExit) as passed_exit:
        cli_main.main(["check", "pipelines/cluster_snapshot", "--format", "json"])

    passed = json.loads(capsys.readouterr().out)
    assert int(passed_exit.value.code) == 0
    assert passed["passed"] is True


def test_dbt_compiler_uses_shared_cluster_admission_after_resolution() -> None:
    baseline = (
        build_dbt_dpone_compiler()
        .build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
        .models[0]
    )
    profile = replace(
        baseline.profile,
        sink_type="clickhouse",
        target_schema="analytics",
        staging_schema=None,
        physical_design={
            "clickhouse": {
                "engine": "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
                "cluster": {"name": "analytics_cluster", "ddl_scope": "cluster"},
            }
        },
    )
    intent = replace(baseline.intent, strategy_mode="full_refresh")
    policy = DbtPublishStrategyPolicy(
        allowed_strategies=("full_refresh",),
        full_refresh_authorized=True,
        full_refresh_max_source_bytes=1024,
    )

    compiled = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()).compile(
        baseline.model,
        intent,
        profile,
        policy,
    )

    assert not [issue for issue in compiled.warnings if issue.code.startswith("CLICKHOUSE_CLUSTER_PUBLICATION_")]

    invalid_profile = replace(
        profile,
        staging_schema="technical",
        physical_design={
            "clickhouse": {
                "engine": "MergeTree",
                "cluster": {"name": "analytics_cluster", "ddl_scope": "local"},
                "access_table": {"name": "target_all"},
            }
        },
    )
    invalid = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()).compile(
        baseline.model,
        intent,
        invalid_profile,
        policy,
    )
    assert {issue.code for issue in invalid.warnings if issue.code.startswith("CLICKHOUSE_CLUSTER_PUBLICATION_")} == {
        "CLICKHOUSE_CLUSTER_PUBLICATION_ENGINE_MUST_BE_REPLICATED_MERGE_TREE",
        "CLICKHOUSE_CLUSTER_PUBLICATION_STAGING_DATABASE_MUST_MATCH_TARGET",
        "CLICKHOUSE_CLUSTER_PUBLICATION_ACCESS_TABLE_UNSUPPORTED",
        "CLICKHOUSE_CLUSTER_PUBLICATION_DDL_SCOPE_MUST_BE_CLUSTER",
    }


def test_plan_explains_cluster_publication_admission_and_no_fallback(tmp_path: Path) -> None:
    manifest = tmp_path / "cluster.yaml"
    manifest.write_text(
        """
name: cluster_snapshot
source:
  type: mssql
  connection_id: source
  table: {schema: dbo, name: source_table}
sink:
  type: clickhouse
  connection_id: target
  table: {schema: analytics, name: target_table}
  strategy:
    mode: full_refresh
    max_source_bytes: 1024
  options:
    physical_design:
      storage:
        clickhouse:
          engine: "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
          cluster: {name: analytics_cluster, ddl_scope: cluster}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    publication = ExecutionPlanService().plan_manifest(manifest)["publication"]

    assert publication == {
        "requested": True,
        "selected": True,
        "mode": "cluster",
        "runtime_admission_required": True,
        "blockers": [],
        "no_fallback": True,
        "replication_mode": "internal",
    }


def test_successful_cluster_receipt_is_in_load_result_reconciliation_metrics() -> None:
    receipt = SimpleNamespace(
        schema_version=CLUSTER_RECEIPT_VERSION,
        marker=SimpleNamespace(operation_id="operation-1"),
        to_dict=lambda: {"schema_version": CLUSTER_RECEIPT_VERSION, "operation": "operation-1"},
    )
    sink = SimpleNamespace(
        _swap_table_into_target=lambda _config, _candidate: receipt,
        _count=lambda _config: 2,
    )
    handle = StagedLoadHandle(
        staging_config=SimpleNamespace(target_table="candidate"),
        payload_schema=(),
        staged_rows=2,
        metadata={},
    )

    result = finalize_full_refresh(sink, _runtime_config(), handle)

    assert result.reconciliation_metrics == {"clickhouse_cluster_full_refresh": receipt.to_dict()}
