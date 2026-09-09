from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.governance.hooks import (
    HookExecutionContext,
    HookGraph,
    HookGraphRunner,
    HookValidationError,
    InMemoryLoadStepAuditStorage,
)


def test_hook_graph_parses_dbt_style_pre_and_post_hooks() -> None:
    graph = HookGraph.from_config(
        {
            "pre_hook": [
                {
                    "id": "refresh_base",
                    "kind": "source_refresh",
                    "type": "sql",
                    "connector": "source",
                    "sql": "EXEC [schema].[p_refresh_base]",
                    "mutates_source": True,
                    "depends_on": [],
                    "lineage": {"outputs": ["analytics_reporting.reporting.base"]},
                    "execution": {"cli": "inline", "airflow": "separate_task"},
                },
                {
                    "id": "refresh_final",
                    "kind": "source_refresh",
                    "type": "sql",
                    "connector": "source",
                    "sql": "EXEC [schema].[p_refresh_final]",
                    "mutates_source": True,
                    "depends_on": ["refresh_base"],
                },
            ],
            "post_hook": [],
        }
    )

    assert [action.id for action in graph.phase("pre_hook")] == ["refresh_base", "refresh_final"]
    assert graph.phase("pre_hook")[0].retry_policy == "none"
    assert graph.phase("pre_hook")[0].execution.airflow == "separate_task"
    assert graph.phase("pre_hook")[0].lineage.outputs == ("analytics_reporting.reporting.base",)


def test_hook_graph_rejects_pre_run_and_mutating_sql_without_explicit_guard() -> None:
    with pytest.raises(HookValidationError, match="pre_hook"):
        HookGraph.from_config({"pre_run": []})

    with pytest.raises(HookValidationError, match="mutates_source"):
        HookGraph.from_config(
            {
                "pre_hook": [
                    {
                        "id": "refresh_base",
                        "kind": "source_refresh",
                        "type": "sql",
                        "connector": "source",
                        "sql": "EXEC [schema].[p_refresh_base]",
                    }
                ]
            }
        )


def test_hook_graph_resolves_sql_file_before_mutating_sql_guard(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "workloads" / "pricing" / "manifests"
    sql_dir = tmp_path / "workloads" / "pricing" / "sql"
    manifest_dir.mkdir(parents=True)
    sql_dir.mkdir(parents=True)
    sql_file = sql_dir / "refresh.sql"
    sql_file.write_text("CREATE OR REPLACE TABLE DWH_Tech.refresh AS SELECT 1\n", encoding="utf-8")

    hook = {
        "pre_hook": [
            {
                "id": "refresh",
                "kind": "source_refresh",
                "type": "sql",
                "connector": "source",
                "sql_file": "../sql/refresh.sql",
            }
        ]
    }

    with pytest.raises(HookValidationError, match="mutates_source"):
        HookGraph.from_config(hook, manifest_dir=manifest_dir, repo_root=tmp_path)

    hook["pre_hook"][0]["mutates_source"] = True
    graph = HookGraph.from_config(hook, manifest_dir=manifest_dir, repo_root=tmp_path)

    action = graph.pre_hook[0]
    assert action.sql == "CREATE OR REPLACE TABLE DWH_Tech.refresh AS SELECT 1"
    assert action.sql_file == "../sql/refresh.sql"
    assert action.sql_hash.startswith("sha256:")


def test_hook_graph_blocks_sql_file_outside_repo(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "repo" / "manifests"
    manifest_dir.mkdir(parents=True)

    with pytest.raises(HookValidationError, match="sql_file_outside_repo"):
        HookGraph.from_config(
            {
                "pre_hook": [
                    {
                        "id": "refresh",
                        "type": "sql",
                        "connector": "source",
                        "sql_file": "../../outside.sql",
                        "mutates_source": True,
                    }
                ]
            },
            manifest_dir=manifest_dir,
            repo_root=tmp_path / "repo",
        )


def test_hook_graph_rejects_missing_dependencies_and_cycles() -> None:
    with pytest.raises(HookValidationError, match="missing dependency"):
        HookGraph.from_config(
            {"pre_hook": [{"id": "refresh", "type": "sql", "sql": "SELECT 1", "depends_on": ["missing"]}]}
        )

    with pytest.raises(HookValidationError, match="cycle"):
        HookGraph.from_config(
            {
                "pre_hook": [
                    {"id": "a", "type": "sql", "sql": "SELECT 1", "depends_on": ["b"]},
                    {"id": "b", "type": "sql", "sql": "SELECT 1", "depends_on": ["a"]},
                ]
            }
        )


def test_hook_graph_runner_executes_topologically_and_records_step_audit() -> None:
    graph = HookGraph.from_config(
        {
            "pre_hook": [
                {"id": "a", "type": "sql", "sql": "SELECT 1"},
                {"id": "b", "type": "sql", "sql": "SELECT 2", "depends_on": ["a"]},
            ]
        }
    )
    calls: list[str] = []

    class Provider:
        def execute(self, action, context):  # noqa: ANN001
            calls.append(f"{context.phase}:{action.id}:{action.sql}")
            return {"ok": True}

    audit = InMemoryLoadStepAuditStorage()
    runner = HookGraphRunner(providers={"sql": Provider()}, audit_storage=audit)

    evidence = runner.run_phase(
        graph,
        "pre_hook",
        HookExecutionContext(run_id="run_1", load_id="load_1", process_name="orders"),
    )

    assert calls == ["pre_hook:a:SELECT 1", "pre_hook:b:SELECT 2"]
    assert [step.status for step in evidence.steps] == ["succeeded", "succeeded"]
    assert [record.status for record in audit.records] == ["running", "succeeded", "running", "succeeded"]


def test_hook_step_evidence_preserves_explicit_procedure_lineage() -> None:
    graph = HookGraph.from_config(
        {
            "pre_hook": [
                {
                    "id": "refresh_sales",
                    "kind": "source_refresh",
                    "type": "sql",
                    "sql": "EXEC [reporting].[p_sales]",
                    "mutates_source": True,
                    "lineage": {
                        "inputs": ["analytics_staging.clickhouse.sales_base"],
                        "outputs": ["analytics_reporting.reporting.account_sales"],
                    },
                }
            ]
        }
    )

    class Provider:
        def execute(self, action, context):  # noqa: ANN001
            del action, context
            return {"ok": True}

    evidence = HookGraphRunner(providers={"sql": Provider()}).run_phase(
        graph,
        "pre_hook",
        HookExecutionContext(run_id="run_1", load_id="load_1"),
    )

    assert evidence.steps[0].lineage.inputs == ("analytics_staging.clickhouse.sales_base",)
    assert evidence.steps[0].lineage.outputs == ("analytics_reporting.reporting.account_sales",)
