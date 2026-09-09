"""Tests for dag-spec edge explainability."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.gitops.airflow_dag_spec_explain import (
    explain_dag_spec_direct_edge,
    resolve_dag_spec_context,
    to_dag_edge_explanation,
)
from tests.airflow_dag_spec_repo import (
    dag_declaration,
    manifest_ref,
    write_domain,
    write_manifest,
    write_workload_set,
)


def _repo_with_explicit_edge(tmp_path: Path) -> tuple[Path, str]:
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
    return tmp_path, workload_set.relative_to(tmp_path).as_posix()


def test_explain_dag_spec_reports_curated_winning_reason(tmp_path: Path) -> None:
    repo_root, workload_set = _repo_with_explicit_edge(tmp_path)

    context, blockers = resolve_dag_spec_context(
        repo_root=repo_root,
        workload_set=workload_set,
        dag_id="DAG__dup",
    )
    assert blockers == ()
    assert context is not None

    explanation = explain_dag_spec_direct_edge(
        context=context,
        upstream_id="marketing_app",
        downstream_id="marketing_web",
    )

    assert explanation.direct_edge is True
    assert explanation.winning_edge is not None
    assert explanation.winning_edge.reason == "curated"
    assert explanation.winning_edge.origin == "wiring.dependencies"
    assert any(item.layer == "declared" and item.present for item in explanation.layer_contributions)
    assert explanation.overridden_edges
    assert explanation.overridden_edges[0].reason == "declared"


def test_explain_dag_spec_cycle_still_reports_edges(tmp_path: Path) -> None:
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
    workload_set = write_workload_set(tmp_path).relative_to(tmp_path).as_posix()

    context, blockers = resolve_dag_spec_context(
        repo_root=tmp_path,
        workload_set=workload_set,
        dag_id="DAG__cycle",
    )
    assert context is not None
    assert any(blocker.code == "dag_spec_cycle" for blocker in context.merge.blockers)

    explanation = explain_dag_spec_direct_edge(
        context=context,
        upstream_id="marketing_app",
        downstream_id="marketing_web",
    )
    assert explanation.direct_edge is True
    assert explanation.merge_blockers


def test_explain_dag_spec_shortest_path_when_no_direct_edge(tmp_path: Path) -> None:
    first = write_manifest(tmp_path, "w1")
    second = write_manifest(tmp_path, "w2", depends_on=["w1.yaml"])
    third = write_manifest(tmp_path, "w3", depends_on=["w2.yaml"])
    write_domain(
        tmp_path,
        workloads={
            "w1": manifest_ref(first),
            "w2": manifest_ref(second),
            "w3": manifest_ref(third),
        },
        dags={
            "DAG__chain": dag_declaration(
                workloads=["w1", "w2", "w3"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path).relative_to(tmp_path).as_posix()

    context, _blockers = resolve_dag_spec_context(
        repo_root=tmp_path,
        workload_set=workload_set,
        dag_id="DAG__chain",
    )
    assert context is not None

    explanation = explain_dag_spec_direct_edge(
        context=context,
        upstream_id="w1",
        downstream_id="w3",
        include_path=True,
    )

    assert explanation.direct_edge is False
    assert explanation.path == ("w1", "w2", "w3")


def test_to_dag_edge_explanation_adapts_for_renderer() -> None:
    from dpone.gitops.airflow_dag_spec import DagSpecEdge, DagSpecNode

    node_a = DagSpecNode(node_id="a", workload_id="a")
    node_b = DagSpecNode(node_id="b", workload_id="b")
    winning = DagSpecEdge(upstream="a", downstream="b", reason="curated", origin="wiring.dependencies")
    explanation = type(
        "Exp",
        (),
        {
            "upstream": node_a,
            "downstream": node_b,
            "direct_edge": True,
            "winning_edge": winning,
            "layer_contributions": (),
            "overridden_edges": (),
            "warnings": (),
            "merge_blockers": (),
            "path": None,
        },
    )()

    legacy = to_dag_edge_explanation(explanation)  # type: ignore[arg-type]
    assert legacy.direct_edge is True
    assert legacy.reasons[0].kind == "curated"


def test_cmd_dag_explain_dag_spec_edge_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.commands.dag.explain_edge_cmd import cmd_dag_explain_edge

    repo_root, workload_set = _repo_with_explicit_edge(tmp_path)
    ctx = type("Ctx", (), {"settings": type("Settings", (), {"repo_root": repo_root})()})()
    args = type(
        "Args",
        (),
        {
            "dag_spec": True,
            "dag_id": "DAG__dup",
            "root": workload_set,
            "upstream": "marketing_app",
            "downstream": "marketing_web",
            "env": "dev",
            "path": False,
            "explain_path": False,
            "path_view": "edges",
            "path_output": "compact",
            "format": "json",
        },
    )()
    captured: dict[str, object] = {}

    def _write_json(payload: dict[str, object]) -> None:
        captured.update(payload)

    monkeypatch.setattr("dpone.commands.dag.explain_edge_cmd.write_json", _write_json)

    exit_code = cmd_dag_explain_edge(args, ctx=ctx, logger=__import__("logging").getLogger("test"))
    assert exit_code == 0
    assert captured["kind"] == "dag.explain_edge_dag_spec"
    assert captured["direct_edge"] is True
    assert captured["winning_edge"]["reason"] == "curated"
