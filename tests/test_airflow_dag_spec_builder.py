"""Builder tests for dag-spec resolution and the three-layer dependency merge.

Layers under test (Phase 1 scope):

- curated wiring from the ``dags:`` block (waves expansion + explicit edges);
- declared ``depends_on``/``task_group`` read from workload manifests through
  the existing manifest/dependency machinery;
- reserved ``inferred`` hook (Phase 4) must exist but produce nothing yet.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from tests.airflow_dag_spec_repo import (
    dag_declaration,
    manifest_ref,
    standard_repo,
    write_batch_manifest,
    write_domain,
    write_flow_manifest,
    write_manifest,
    write_workload_set,
)


def _build(repo_root: Path, workload_set: Path):
    return AirflowDagSpecBuilder(repo_root=repo_root).build(
        workload_set=workload_set.relative_to(repo_root).as_posix(), env="dev"
    )


def test_unexpected_manifest_loader_failure_propagates(tmp_path: Path) -> None:
    workload_set = standard_repo(
        tmp_path,
        dags={
            "DAG__unexpected": dag_declaration(
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["marketing_app"]}}
            )
        },
    )

    class BrokenManifestLoader:
        def load(self, path: Path, *, metadata_only: bool) -> object:
            _ = path, metadata_only
            raise RuntimeError("unexpected manifest-loader failure")

    builder = AirflowDagSpecBuilder(
        repo_root=tmp_path,
        manifest_loader=BrokenManifestLoader(),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="unexpected manifest-loader failure"):
        builder.build(workload_set=workload_set.relative_to(tmp_path).as_posix(), env="dev")


def test_waves_wiring_expands_into_explicit_curated_edges(tmp_path: Path) -> None:
    first = write_manifest(tmp_path, "w1")
    second = write_manifest(tmp_path, "w2")
    third = write_manifest(tmp_path, "w3")
    write_domain(
        tmp_path,
        workloads={"w1": manifest_ref(first), "w2": manifest_ref(second), "w3": manifest_ref(third)},
        dags={
            "DAG__waves": dag_declaration(
                workloads=["w1", "w2", "w3"],
                wiring={"mode": "waves", "max_parallel_workloads": 2},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__waves")
    curated = {(edge.upstream, edge.downstream) for edge in spec.edges if edge.reason == "curated"}
    assert curated == {("w1", "w3"), ("w2", "w3")}
    assert spec.topological_order.index("w1") < spec.topological_order.index("w3")


def test_mapping_outlet_satisfies_schedule_asset_validation(tmp_path: Path) -> None:
    manifest = write_manifest(tmp_path, "producer", outlets=["placeholder://outlet"])
    manifest_path = tmp_path / manifest
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    uri = "object://warehouse/orders"
    payload["gitops"]["airflow"]["execution"]["outlets"] = [{"uri": uri, "provenance": "declared:authoring"}]
    manifest_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(manifest)},
        dags={
            "DAG__asset": dag_declaration(
                workloads=["producer"],
                schedule={"assets": [uri]},
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    assert report.by_dag_id("DAG__asset").dag_id == "DAG__asset"


def test_explicit_wiring_dependencies_become_curated_edges(tmp_path: Path) -> None:
    workload_set = standard_repo(
        tmp_path,
        dags={
            "DAG__explicit": dag_declaration(
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["marketing_app"]}}
            )
        },
    )

    report = _build(tmp_path, workload_set)

    assert report.passed
    spec = report.by_dag_id("DAG__explicit")
    assert [(edge.upstream, edge.downstream, edge.reason) for edge in spec.edges] == [
        ("marketing_app", "marketing_web", "curated")
    ]


def test_single_process_flows_retain_workload_node_identity_and_selector(tmp_path: Path) -> None:
    app = write_flow_manifest(tmp_path, "app")
    web = write_flow_manifest(tmp_path, "web", depends_on=["app.yaml"])
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={
            "DAG__flow": dag_declaration(
                workloads=["marketing_app", "marketing_web"],
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["marketing_app"]}},
            )
        },
    )

    report = _build(tmp_path, write_workload_set(tmp_path))

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__flow")
    assert [(node.node_id, node.selector) for node in spec.nodes] == [
        ("marketing_app", "app"),
        ("marketing_web", "web"),
    ]
    assert [(edge.upstream, edge.downstream, edge.reason) for edge in spec.edges] == [
        ("marketing_app", "marketing_web", "curated")
    ]


def test_manifest_depends_on_produces_declared_edges_and_task_groups(tmp_path: Path) -> None:
    app = write_manifest(tmp_path, "app", task_group="ingest")
    web = write_manifest(tmp_path, "web", depends_on=["app.yaml"], task_group="ingest")
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={
            "DAG__declared": dag_declaration(
                workloads=["marketing_app", "marketing_web"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__declared")
    declared = [edge for edge in spec.edges if edge.reason == "declared"]
    assert [(edge.upstream, edge.downstream) for edge in declared] == [("marketing_app", "marketing_web")]
    assert "depends_on" in declared[0].origin
    assert {node.task_group for node in spec.nodes} == {"ingest"}


def test_batch_nodes_publish_explicit_visibility_selector_and_task_estimate(tmp_path: Path) -> None:
    batch = write_batch_manifest(tmp_path, "visibility")
    batch_path = tmp_path / batch
    payload = yaml.safe_load(batch_path.read_text(encoding="utf-8"))
    tables = payload["schemas"]["public"]["tables"]
    tables[0]["overrides"] = {"execution": {"visibility": "inline"}}
    tables[1]["overrides"] = {"execution": {"visibility": "group"}, "task_group": "loads"}
    batch_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    write_domain(
        tmp_path,
        workloads={"pipeline": manifest_ref(batch)},
        dags={"DAG__visibility": dag_declaration(workloads=["pipeline"])},
    )

    report = _build(tmp_path, write_workload_set(tmp_path))

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    nodes = report.by_dag_id("DAG__visibility").nodes
    assert [(node.selector, node.visibility, node.task_group, node.estimated_visible_tasks) for node in nodes] == [
        ("public.t1", "inline", None, 1),
        ("public.t2", "group", "loads", 2),
    ]
    payload = report.by_dag_id("DAG__visibility").to_jsonable()
    assert payload["visible_task_plan"] == {
        "estimated_total": 3,
        "warn_threshold": 100,
        "max_tasks": 250,
        "status": "within_budget",
    }


def test_visible_task_budget_blocks_before_dag_spec_publication(tmp_path: Path) -> None:
    first = write_manifest(tmp_path, "w1")
    second = write_manifest(tmp_path, "w2")
    write_domain(
        tmp_path,
        workloads={"w1": manifest_ref(first), "w2": manifest_ref(second)},
        dags={
            "DAG__budget": dag_declaration(
                workloads=["w1", "w2"],
                wiring={
                    "mode": "waves",
                    "max_parallel_workloads": 2,
                    "visible_task_budget": {"warn": 1, "max": 1},
                },
            )
        },
    )

    report = _build(tmp_path, write_workload_set(tmp_path))

    assert not report.passed
    assert report.specs == ()
    assert any(blocker.code == "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_EXCEEDED" for blocker in report.blockers)


def test_duplicate_edge_keeps_highest_priority_reason(tmp_path: Path) -> None:
    app = write_manifest(tmp_path, "app")
    web = write_manifest(tmp_path, "web", depends_on=["app.yaml"])
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={
            "DAG__dup": dag_declaration(
                workloads=["marketing_app", "marketing_web"],
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["marketing_app"]}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    spec = report.by_dag_id("DAG__dup")
    edges = [(edge.upstream, edge.downstream, edge.reason) for edge in spec.edges]
    assert edges == [("marketing_app", "marketing_web", "curated")]


def test_cycle_between_layers_is_a_build_error_with_explanation(tmp_path: Path) -> None:
    app = write_manifest(tmp_path, "app", depends_on=["web.yaml"])
    web = write_manifest(tmp_path, "web")
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={
            "DAG__cycle": dag_declaration(
                workloads=["marketing_app", "marketing_web"],
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["marketing_app"]}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert not report.passed
    cycle = [blocker for blocker in report.blockers if blocker.code == "dag_spec_cycle"]
    assert cycle
    assert "marketing_app" in cycle[0].message and "marketing_web" in cycle[0].message


def test_unknown_workload_id_is_a_build_error(tmp_path: Path) -> None:
    workload_set = standard_repo(tmp_path, dags={"DAG__bad": dag_declaration(workloads=["marketing_app", "ghost"])})

    report = _build(tmp_path, workload_set)

    assert not report.passed
    assert any(
        blocker.code == "dag_spec_workload_unknown" and "ghost" in blocker.message for blocker in report.blockers
    )


def test_build_and_write_reports_actual_custom_artifact_paths(tmp_path: Path) -> None:
    workload_set = standard_repo(tmp_path, dags={"DAG__custom": dag_declaration()})
    artifact_dir = ".dpone/fixture/airflow/_dags"

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build_and_write(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
        artifact_dir=artifact_dir,
    )

    expected = f"{artifact_dir}/DAG__custom.dag-spec.json"
    assert report.output_paths == (expected,)
    assert report.to_jsonable()["dag_specs"] == {"DAG__custom": expected}
    assert (tmp_path / expected).is_file()


def test_build_and_write_rejects_symlink_escape(tmp_path: Path) -> None:
    from dpone.gitops.paths import GitOpsPathValidationError

    workload_set = standard_repo(tmp_path, dags={"DAG__confined": dag_declaration()})
    outside = tmp_path.parent / "outside"
    outside.mkdir(exist_ok=True)
    (tmp_path / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(GitOpsPathValidationError, match="below the repository root"):
        AirflowDagSpecBuilder(repo_root=tmp_path).build_and_write(
            workload_set=workload_set.relative_to(tmp_path).as_posix(),
            env="dev",
            artifact_dir="escape/_dags",
        )

    assert not tuple(outside.glob("*.dag-spec.json"))


def test_build_and_write_rejects_symlinked_artifact_file(tmp_path: Path) -> None:
    from dpone.gitops.paths import GitOpsPathValidationError

    workload_set = standard_repo(tmp_path, dags={"DAG__confined": dag_declaration()})
    artifact_dir = tmp_path / ".dpone/fixture/airflow/_dags"
    outside = tmp_path.parent / "outside-spec.json"
    artifact_dir.mkdir(parents=True)
    outside.write_text("untouched", encoding="utf-8")
    (artifact_dir / "DAG__confined.dag-spec.json").symlink_to(outside)

    with pytest.raises(GitOpsPathValidationError, match="below the repository root"):
        AirflowDagSpecBuilder(repo_root=tmp_path).build_and_write(
            workload_set=workload_set.relative_to(tmp_path).as_posix(),
            env="dev",
            artifact_dir=artifact_dir.relative_to(tmp_path),
        )

    assert outside.read_text(encoding="utf-8") == "untouched"


def test_group_reference_resolves_via_workflow_groups(tmp_path: Path) -> None:
    workload_set = standard_repo(tmp_path, dags={"DAG__group": dag_declaration(workloads=None, group="wa")})

    report = _build(tmp_path, workload_set)

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__group")
    assert [node.node_id for node in spec.nodes] == ["marketing_app", "marketing_web"]
    unknown = AirflowDagSpecBuilder(repo_root=tmp_path)
    write_domain(
        tmp_path,
        workloads={},
        dags={"DAG__group": dag_declaration(workloads=None, group="ghost_group")},
    )
    bad = unknown.build(workload_set=workload_set.relative_to(tmp_path).as_posix(), env="dev")
    assert any(blocker.code == "dag_spec_group_unknown" for blocker in bad.blockers)


def test_schedule_assets_must_reference_pack_outlets_or_be_external(tmp_path: Path) -> None:
    workload_set = standard_repo(
        tmp_path,
        dags={
            "DAG__ok": dag_declaration(schedule={"assets": ["postgres://dst/app"]}),
            "DAG__ext": dag_declaration(schedule={"assets": [{"uri": "s3://warehouse/x", "external": True}]}),
            "DAG__bad": dag_declaration(schedule={"assets": ["s3://nowhere/y"]}),
        },
    )

    report = _build(tmp_path, workload_set)

    assert report.by_dag_id("DAG__ok") is not None
    assert report.by_dag_id("DAG__ext") is not None
    assert any(
        blocker.code == "dag_spec_schedule_asset_unknown" and "s3://nowhere/y" in blocker.message
        for blocker in report.blockers
    )


def test_batch_manifest_workload_expands_into_per_process_nodes(tmp_path: Path) -> None:
    batch = write_batch_manifest(tmp_path)
    write_domain(
        tmp_path,
        workloads={"marketing_pipe": manifest_ref(batch)},
        dags={"DAG__batch": dag_declaration(workloads=["marketing_pipe"], wiring={"mode": "explicit"})},
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__batch")
    assert [node.node_id for node in spec.nodes] == [
        "marketing_pipe__public__t1",
        "marketing_pipe__public__t2",
    ]
    assert {node.workload_id for node in spec.nodes} == {"marketing_pipe"}
    assert {node.selector for node in spec.nodes} == {"public.t1", "public.t2"}
    declared = [(edge.upstream, edge.downstream) for edge in spec.edges if edge.reason == "declared"]
    assert declared == [("marketing_pipe__public__t1", "marketing_pipe__public__t2")]


def test_spec_fingerprint_is_stable_across_rebuilds(tmp_path: Path) -> None:
    workload_set = standard_repo(tmp_path, dags={"DAG__fp": dag_declaration()})

    first = _build(tmp_path, workload_set).by_dag_id("DAG__fp").to_jsonable()
    second = _build(tmp_path, workload_set).by_dag_id("DAG__fp").to_jsonable()

    assert first["spec_fingerprint"] == second["spec_fingerprint"]
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_wiring_mode_assets_infers_lineage_edges(tmp_path: Path) -> None:
    app = write_manifest(
        tmp_path,
        "app",
        outlets=["postgres://dst/app"],
        sink_schema="dst",
        sink_table="app",
    )
    web = write_manifest(
        tmp_path,
        "web",
        source_schema="dst",
        source_table="app",
        inlets=["postgres://dst/app"],
    )
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={"DAG__assets": dag_declaration(workloads=["marketing_app", "marketing_web"], wiring={"mode": "assets"})},
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed
    spec = report.by_dag_id("DAG__assets")
    inferred = [(edge.upstream, edge.downstream) for edge in spec.edges if edge.reason == "inferred"]
    assert inferred == [("marketing_app", "marketing_web")]


def test_declared_dependency_outside_dag_membership_is_a_warning(tmp_path: Path) -> None:
    app = write_manifest(tmp_path, "app")
    web = write_manifest(tmp_path, "web", depends_on=["app.yaml"])
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        dags={
            "DAG__solo": dag_declaration(workloads=["marketing_web"], wiring={"mode": "explicit"}),
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert report.passed
    assert report.by_dag_id("DAG__solo").edges == ()
    assert any(warning.code == "dag_spec_dependency_outside_dag" for warning in report.warnings)


def test_ambiguous_asset_producers_are_blockers_for_dag_spec_build(tmp_path: Path) -> None:
    producer_a = write_manifest(
        tmp_path,
        "producer_a",
        sink_schema="public",
        sink_table="shared",
    )
    producer_b = write_manifest(
        tmp_path,
        "producer_b",
        sink_schema="public",
        sink_table="shared",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        source_schema="public",
        source_table="shared",
        sink_schema="public",
        sink_table="downstream",
    )
    write_domain(
        tmp_path,
        domain="analytics",
        workloads={
            "producer_a": manifest_ref(producer_a),
            "producer_b": manifest_ref(producer_b),
            "consumer": manifest_ref(consumer),
        },
        dags={
            "DAG__analytics": dag_declaration(
                workloads=["producer_a", "producer_b", "consumer"],
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert not report.passed
    assert any(blocker.code == "asset_graph_uri_ambiguous" for blocker in report.blockers)


def test_cross_dag_cycles_are_blockers_for_all_involved_dags(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        source_schema="public",
        source_table="consumer",
        sink_schema="public",
        sink_table="producer",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        source_schema="public",
        source_table="producer",
        sink_schema="public",
        sink_table="consumer",
    )
    write_domain(
        tmp_path,
        domain="marketing",
        workloads={"marketing_producer": manifest_ref(producer)},
        dags={"DAG__producer": dag_declaration(workloads=["marketing_producer"], wiring={"mode": "assets"})},
    )
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"sales_consumer": manifest_ref(consumer)},
        dags={"DAG__consumer": dag_declaration(workloads=["sales_consumer"], wiring={"mode": "assets"})},
    )
    workload_set = write_workload_set(tmp_path)

    report = _build(tmp_path, workload_set)

    assert not report.passed
    blockers = [item for item in report.blockers if item.code == "dag_spec_cross_dag_cycle"]
    assert {item.path for item in blockers} == {"DAG__producer", "DAG__consumer"}
    assert all("cross-dag inferred dependency cycle" in item.message.lower() for item in blockers)


def test_build_tolerates_malformed_project_config_for_membership_warnings(tmp_path: Path) -> None:
    workload_set = standard_repo(
        tmp_path,
        dags={"DAG__marketing": dag_declaration(workloads=["marketing_web"])},
    )
    (tmp_path / "dpone.yaml").write_text("not: valid: project:\n", encoding="utf-8")

    report = _build(tmp_path, workload_set)

    assert isinstance(report.warnings, tuple)
    assert isinstance(report.blockers, tuple)
