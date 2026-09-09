from __future__ import annotations

import hashlib
import json
import logging
from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from dpone.manifest.selection import (
    NamedSelection,
    SelectionEngine,
    SelectionError,
    SelectionGraph,
    SelectionNode,
    SelectionRequest,
    SelectionState,
    SelectionStateNode,
)
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry


def _node(
    node_id: str,
    *,
    domain: str = "sales",
    owner: str = "data-platform",
    tags: tuple[str, ...] = (),
    sources: tuple[str, ...] = ("mssql",),
    sinks: tuple[str, ...] = ("clickhouse",),
    groups: tuple[str, ...] = (),
    semantic: str | None = None,
) -> SelectionNode:
    return SelectionNode(
        node_id=node_id,
        source_path=f"pipelines/{node_id}/pipeline.yaml",
        semantic_fingerprint=semantic or "sha256:" + hashlib.sha256(node_id.encode("utf-8")).hexdigest(),
        domain=domain,
        owner=owner,
        tags=tags,
        sources=sources,
        sinks=sinks,
        groups=groups,
    )


def _graph() -> SelectionGraph:
    return SelectionGraph.build(
        nodes=(
            _node("raw_orders", tags=("finance", "daily"), groups=("daily",)),
            _node("orders", tags=("finance",), groups=("daily",)),
            _node("orders_quality", tags=("quality",), sources=("clickhouse",)),
            _node("marketing", domain="marketing", owner="growth", tags=("deprecated",)),
        ),
        edges=(("raw_orders", "orders"), ("orders", "orders_quality")),
    )


def test_selector_engine_matches_exact_metadata_and_explains_graph_expansion() -> None:
    report = SelectionEngine().select(
        _graph(),
        SelectionRequest(select=("tag:finance+",), exclude=("tag:deprecated",)),
    )

    assert [item.node.node_id for item in report.selected] == ["orders", "orders_quality", "raw_orders"]
    quality = next(item for item in report.selected if item.node.node_id == "orders_quality")
    assert quality.reasons[0].kind == "descendant"
    assert quality.reasons[0].path in {
        ("orders", "orders_quality"),
        ("raw_orders", "orders", "orders_quality"),
    }
    assert report.selection_fingerprint.startswith("sha256:")
    assert report.catalog_fingerprint == _graph().catalog_fingerprint


def test_selector_prefix_plus_includes_all_ancestors_with_shortest_paths() -> None:
    report = SelectionEngine().select(_graph(), SelectionRequest(select=("+id:orders_quality",)))

    assert [item.node.node_id for item in report.selected] == ["orders", "orders_quality", "raw_orders"]
    raw = next(item for item in report.selected if item.node.node_id == "raw_orders")
    assert raw.reasons[0].kind == "ancestor"
    assert raw.reasons[0].path == ("orders_quality", "orders", "raw_orders")


def test_graph_expansion_uses_one_deterministic_shortest_path() -> None:
    graph = SelectionGraph.build(
        nodes=tuple(_node(node_id, tags=("root",) if node_id == "a" else ()) for node_id in ("a", "b", "c", "d")),
        edges=(("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")),
    )

    report = SelectionEngine().select(graph, SelectionRequest(select=("tag:root+",)))

    leaf = next(item for item in report.selected if item.node.node_id == "d")
    assert leaf.reasons[0].path == ("a", "b", "d")


def test_graph_expansion_preserves_one_shortest_reason_per_direct_root() -> None:
    graph = SelectionGraph.build(
        nodes=(
            _node("a", tags=("root",)),
            _node("b", tags=("root",)),
            _node("c"),
        ),
        edges=(("a", "c"), ("b", "c")),
    )

    report = SelectionEngine().select(graph, SelectionRequest(select=("tag:root+",)))

    leaf = next(item for item in report.selected if item.node.node_id == "c")
    graph_reasons = [reason for reason in leaf.reasons if reason.kind == "descendant"]
    assert [(reason.root, reason.path) for reason in graph_reasons] == [
        ("a", ("a", "c")),
        ("b", ("b", "c")),
    ]


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("domain:marketing", ["marketing"]),
        ("owner:growth", ["marketing"]),
        ("source:clickhouse", ["orders_quality"]),
        ("sink:clickhouse", ["marketing", "orders", "orders_quality", "raw_orders"]),
        ("group:daily", ["orders", "raw_orders"]),
    ],
)
def test_selector_methods_are_exact(expression: str, expected: list[str]) -> None:
    report = SelectionEngine().select(_graph(), SelectionRequest(select=(expression,)))

    assert [item.node.node_id for item in report.selected] == expected


def test_repeated_select_is_union_and_exclude_is_applied_after_expansion() -> None:
    report = SelectionEngine().select(
        _graph(),
        SelectionRequest(
            select=("domain:sales", "id:marketing"),
            exclude=("id:orders+",),
        ),
    )

    assert [item.node.node_id for item in report.selected] == ["marketing", "raw_orders"]
    assert [item.node.node_id for item in report.excluded] == ["orders", "orders_quality"]


def test_named_selector_composes_selection_and_detects_cycles() -> None:
    named = {
        "sales_active": NamedSelection(
            name="sales_active",
            select=("domain:sales",),
            exclude=("tag:deprecated",),
        ),
        "changed": NamedSelection(name="changed", select=("selector:sales_active",)),
    }
    report = SelectionEngine().select(
        _graph(),
        SelectionRequest(select=("selector:changed",), named=named),
    )
    assert [item.node.node_id for item in report.selected] == ["orders", "orders_quality", "raw_orders"]

    cyclic = {
        "a": NamedSelection(name="a", select=("selector:b",)),
        "b": NamedSelection(name="b", select=("selector:a",)),
    }
    with pytest.raises(SelectionError, match="cycle") as error:
        SelectionEngine().select(_graph(), SelectionRequest(select=("selector:a",), named=cyclic))
    assert error.value.code == "DPONE_SELECTION_NAMED_CYCLE"


def test_state_selection_uses_semantic_and_graph_fingerprints() -> None:
    graph = _graph()
    baseline = SelectionState.build(
        SelectionStateNode(
            node_id=node.node_id,
            semantic_fingerprint=("sha256:" + "f" * 64 if node.node_id == "orders" else node.semantic_fingerprint),
            graph_fingerprint=node.graph_fingerprint,
        )
        for node in graph.nodes
        if node.node_id != "marketing"
    )

    modified = SelectionEngine().select(
        graph,
        SelectionRequest(select=("state:modified",), state=baseline),
    )
    assert [item.node.node_id for item in modified.selected] == ["orders"]

    new = SelectionEngine().select(graph, SelectionRequest(select=("state:new",), state=baseline))
    assert [item.node.node_id for item in new.selected] == ["marketing"]


def test_graph_fingerprint_changes_only_for_the_direct_downstream_dependency() -> None:
    nodes = (_node("raw_orders"), _node("orders"))
    without_edge = SelectionGraph.build(nodes=nodes, edges=())
    with_edge = SelectionGraph.build(nodes=nodes, edges=(("raw_orders", "orders"),))
    baseline = SelectionState.build(
        SelectionStateNode(node.node_id, node.semantic_fingerprint, node.graph_fingerprint)
        for node in without_edge.nodes
    )

    report = SelectionEngine().select(
        with_edge,
        SelectionRequest(select=("state:modified",), state=baseline),
    )

    assert [item.node.node_id for item in report.selected] == ["orders"]


def test_state_selector_requires_baseline_and_empty_selection_fails_closed() -> None:
    with pytest.raises(SelectionError) as missing:
        SelectionEngine().select(_graph(), SelectionRequest(select=("state:modified",)))
    assert missing.value.code == "DPONE_SELECTION_STATE_REQUIRED"

    with pytest.raises(SelectionError) as empty:
        SelectionEngine().select(_graph(), SelectionRequest(select=("tag:missing",)))
    assert empty.value.code == "DPONE_SELECTION_EMPTY"


@pytest.mark.parametrize(
    "expression",
    ["", "finance", "unknown:x", "tag:", "tag:a b", "tag:{{x}}", "tag:../x", "tag:https://example"],
)
def test_invalid_selector_expressions_are_rejected(expression: str) -> None:
    with pytest.raises(SelectionError) as error:
        SelectionEngine().select(_graph(), SelectionRequest(select=(expression,)))
    assert error.value.code == "DPONE_SELECTION_EXPRESSION_INVALID"


def test_graph_rejects_duplicate_nodes_unknown_edges_and_cycles() -> None:
    with pytest.raises(SelectionError) as duplicate:
        SelectionGraph.build(nodes=(_node("a"), _node("a")), edges=())
    assert duplicate.value.code == "DPONE_SELECTION_DUPLICATE_WORKLOAD"

    with pytest.raises(SelectionError) as unknown:
        SelectionGraph.build(nodes=(_node("a"),), edges=(("a", "missing"),))
    assert unknown.value.code == "DPONE_SELECTION_EDGE_UNKNOWN"

    with pytest.raises(SelectionError) as cycle:
        SelectionGraph.build(nodes=(_node("a"), _node("b")), edges=(("a", "b"), ("b", "a")))
    assert cycle.value.code == "DPONE_SELECTION_GRAPH_CYCLE"


def test_selection_result_budget_is_hard_limit() -> None:
    with pytest.raises(SelectionError) as error:
        SelectionEngine().select(_graph(), SelectionRequest(max_selected=2))
    assert error.value.code == "DPONE_SELECTION_LIMIT_EXCEEDED"


def _write(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _pipeline(pipeline_id: str, *, domain: str, owner: str, tags: list[str], source: str, sink: str) -> dict:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": f"pipelines/{pipeline_id}/pipeline.yaml"},
        "metadata": {"id": pipeline_id, "domain": domain, "owner": owner, "tags": tags},
        "processes": [
            {
                "name": pipeline_id,
                "source": {
                    "type": source,
                    "connection_ref": "source_dev",
                    "table": {"schema": "dbo", "name": pipeline_id},
                },
                "sink": {
                    "type": sink,
                    "connection_ref": "sink_dev",
                    "table": {"schema": "analytics", "name": pipeline_id},
                    "strategy": {"mode": "full_refresh"},
                },
            }
        ],
    }


def _project(tmp_path: Path) -> None:
    _write(
        tmp_path / "dpone.yaml",
        {
            "schema": "dpone.project.v1",
            "authoring": {"primary_source_policy": "one_per_pipeline"},
        },
    )
    _write(
        tmp_path / "domains" / "sales.yaml",
        {
            "schema": "dpone.domain-catalog.v1",
            "domain": "sales",
            "workflow_groups": {"daily": {"workloads": ["raw_orders", "orders"]}},
            "workloads": {
                "raw_orders": {"authoring_source": "pipelines/raw_orders/pipeline.yaml"},
                "orders": {
                    "authoring_source": "pipelines/orders/pipeline.yaml",
                    "depends_on": ["raw_orders"],
                },
            },
            "dags": {
                "orders": {
                    "workloads": ["raw_orders", "orders"],
                    "schedule": None,
                    "start_date": "2026-01-01",
                    "catchup": False,
                }
            },
        },
    )
    _write(
        tmp_path / "pipelines" / "raw_orders" / "pipeline.yaml",
        _pipeline(
            "raw_orders",
            domain="sales",
            owner="sales-platform",
            tags=["finance"],
            source="mssql",
            sink="clickhouse",
        ),
    )
    _write(
        tmp_path / "pipelines" / "orders" / "pipeline.yaml",
        _pipeline(
            "orders",
            domain="sales",
            owner="sales-platform",
            tags=["finance", "daily"],
            source="clickhouse",
            sink="clickhouse",
        ),
    )
    write_mssql_connection_registry(tmp_path, include=("source_dev",))


def test_project_selection_service_builds_graph_from_explicit_sources(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    outcome = ProjectSelectionService(root=tmp_path).select(
        target=".",
        select=("source:mssql+",),
    )

    assert [item.node.node_id for item in outcome.report.selected] == ["orders", "raw_orders"]
    assert outcome.checked_sources["orders"].compilation is not None
    assert str(tmp_path) not in json.dumps(outcome.report.to_jsonable())
    assert "dpone.yaml" in outcome.consumed_files


def test_project_selection_tracks_compiler_dependencies_and_rejects_drift(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    source_path = tmp_path / "pipelines" / "raw_orders" / "pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source["processes"][0]["source"]["query"] = {"sql_file": "orders.sql"}
    _write(source_path, source)
    sql_path = source_path.parent / "orders.sql"
    sql_path.write_text("SELECT 1\n", encoding="utf-8")
    service = ProjectSelectionService(root=tmp_path)

    outcome = service.select(target=".", select=("id:raw_orders",))
    assert "pipelines/raw_orders/orders.sql" in outcome.consumed_files

    sql_path.write_text("SELECT 2\n", encoding="utf-8")
    with pytest.raises(SelectionError) as error:
        service.verify_consumed_files(dict(outcome.consumed_files))
    assert error.value.code == "DPONE_SELECTION_STATE_CHANGED"


def test_project_selection_rejects_more_than_1000_workloads_before_compilation(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _write(tmp_path / "dpone.yaml", {"schema": "dpone.project.v1"})
    _write(
        tmp_path / "domains" / "oversized.yaml",
        {
            "schema": "dpone.domain-catalog.v1",
            "domain": "oversized",
            "workloads": {
                f"workload_{index:04d}": {"authoring_source": "pipelines/missing/pipeline.yaml"}
                for index in range(1001)
            },
        },
    )

    with pytest.raises(SelectionError) as error:
        ProjectSelectionService(root=tmp_path).select(target=".")
    assert error.value.code == "DPONE_SELECTION_LIMIT_EXCEEDED"


def test_project_selection_rejects_nested_or_missing_project_targets(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    nested = tmp_path / "nested"
    _write(nested / "dpone.yaml", {"schema": "dpone.project.v1"})
    service = ProjectSelectionService(root=tmp_path)

    for target in ("nested", "missing"):
        with pytest.raises(SelectionError) as error:
            service.select(target=target)
        assert error.value.code == "DPONE_SELECTION_CATALOG_INVALID"


def test_project_selection_service_loads_named_selectors_and_state(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    _write(
        tmp_path / "selectors.yaml",
        {
            "schema": "dpone.selectors.v1",
            "selectors": {
                "finance": {"select": ["tag:finance"], "exclude": ["tag:deprecated"]},
            },
        },
    )
    first = ProjectSelectionService(root=tmp_path).select(target=".", select=("selector:finance",))
    state_path = tmp_path / "selection-state.json"
    state_path.write_text(json.dumps(first.state.to_jsonable()), encoding="utf-8")

    second = ProjectSelectionService(root=tmp_path).select(
        target=".",
        select=("state:unmodified",),
        state_path="selection-state.json",
    )

    assert [item.node.node_id for item in second.report.selected] == ["orders", "raw_orders"]


def test_project_loader_rejects_symlink_escape_and_metadata_conflict(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    outside = _write(
        tmp_path.parent / "outside-pipeline.yaml",
        _pipeline("raw_orders", domain="sales", owner="sales-platform", tags=[], source="mssql", sink="clickhouse"),
    )
    source = tmp_path / "pipelines" / "raw_orders" / "pipeline.yaml"
    source.unlink()
    source.symlink_to(outside)

    with pytest.raises(SelectionError) as error:
        ProjectSelectionService(root=tmp_path).select(target=".")
    assert error.value.code == "DPONE_SELECTION_CATALOG_INVALID"


def test_explicit_selection_state_symlink_is_rejected(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    initial = ProjectSelectionService(root=tmp_path).select(target=".")
    real_state = tmp_path / "real-state.json"
    real_state.write_text(json.dumps(initial.state.to_jsonable()), encoding="utf-8")
    (tmp_path / "selection-state.json").symlink_to(real_state)

    with pytest.raises(SelectionError) as error:
        ProjectSelectionService(root=tmp_path).select(
            target=".",
            select=("state:unmodified",),
            state_path="selection-state.json",
        )
    assert error.value.code == "DPONE_SELECTION_STATE_INVALID"


def test_check_command_uses_project_selector_and_emits_explanations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.airflow_self_service_cmd import cmd_check

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code = cmd_check(
        Namespace(
            target=".",
            connections=False,
            live=False,
            environment="dev",
            format="json",
            select=["source:mssql+"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=1000,
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert [item["id"] for item in payload["selection"]["selected"]] == ["orders", "raw_orders"]
    assert payload["selection"]["selected"][0]["reasons"]


def test_check_command_preserves_structured_selection_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.airflow_self_service_cmd import cmd_check

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code = cmd_check(
        Namespace(
            target=".",
            connections=False,
            live=False,
            environment="dev",
            format="json",
            select=["tag:missing"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=1000,
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["errors"][0]["code"] == "DPONE_SELECTION_EMPTY"


def test_selected_static_check_and_preview_reuse_authoring_validation(tmp_path: Path) -> None:
    from dpone.readiness.airflow_authoring_check_service import ProjectSelectionCheckService
    from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService

    _project(tmp_path)
    source_path = tmp_path / "pipelines" / "raw_orders" / "pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source_config = source["processes"][0]["source"]
    source_config.pop("connection_ref")
    source_config["connection_type"] = "vault"
    source_config["vault_path"] = "secret/data/raw-orders"
    _write(source_path, source)

    checked = ProjectSelectionCheckService(root=tmp_path).check(
        target=".",
        select=("id:raw_orders",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=1000,
        mode="static",
        environment="dev",
    )
    previewed = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=".",
        select=("id:raw_orders",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )

    assert checked.passed is False
    assert previewed.passed is False
    assert checked.errors[0]["code"] == "DPONE_LEGACY_CONNECTION_CONFIG_FOUND"
    assert previewed.errors[0]["code"] == "DPONE_LEGACY_CONNECTION_CONFIG_FOUND"


def test_airflow_preview_materializes_one_static_selected_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.airflow_self_service_cmd import cmd_airflow_preview

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code = cmd_airflow_preview(
        Namespace(
            pipeline=".",
            format="json",
            select=["source:mssql+"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=500,
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["selection"]["selection_fingerprint"].startswith("sha256:")
    current = tmp_path / ".dpone-cache" / "current"
    index = json.loads((current / "airflow-index.json").read_text(encoding="utf-8"))
    assert [item["id"] for item in index["workload_packs"]] == ["orders", "raw_orders"]
    assert [item["id"] for item in index["dag_specs"]] == ["orders"]
    assert all(item["bytes"] > 0 for item in (*index["dag_specs"], *index["workload_packs"]))
    from dpone_airflow_pack.deployment_index import load_airflow_deployment_index

    loaded_index = load_airflow_deployment_index(current / "airflow-index.json")
    assert [artifact.id for artifact in loaded_index.dag_specs] == ["orders"]
    assert [artifact.id for artifact in loaded_index.workload_packs] == ["orders", "raw_orders"]
    state = json.loads((current / "selection-state.json").read_text(encoding="utf-8"))
    assert state["schema"] == "dpone.selection-state.v1"

    dag_ref = index["dag_specs"][0]["artifact_ref"]
    release_dir = dag_ref.removeprefix("cache://releases/").split("/", 1)[0]
    dag_spec = json.loads(
        (tmp_path / ".dpone-cache" / "releases" / release_dir / "dags" / "orders.dag-spec.json").read_text(
            encoding="utf-8"
        )
    )
    assert dag_spec["edges"] == [
        {"downstream": "orders", "origin": "selection_graph", "reason": "declared", "upstream": "raw_orders"}
    ]


def test_airflow_preview_prunes_unselected_boundary_edges_without_dangling_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.airflow_self_service_cmd import cmd_airflow_preview

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code = cmd_airflow_preview(
        Namespace(
            pipeline=".",
            format="json",
            select=["id:orders"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=500,
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["pruned_boundary_edges"] == [{"downstream": "orders", "upstream": "raw_orders"}]


def test_airflow_preview_records_selected_upstream_boundary_edge(
    tmp_path: Path,
) -> None:
    from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService

    _project(tmp_path)
    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=".",
        select=("id:raw_orders",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )

    assert result.passed is True
    assert result.details["pruned_boundary_edges"] == [{"downstream": "orders", "upstream": "raw_orders"}]


def test_airflow_preview_fails_when_selected_dependency_cannot_be_represented_in_one_dag(
    tmp_path: Path,
) -> None:
    from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService

    _project(tmp_path)
    domain_path = tmp_path / "domains" / "sales.yaml"
    domain = yaml.safe_load(domain_path.read_text(encoding="utf-8"))
    domain["dags"] = {
        "raw_orders": {
            "workloads": ["raw_orders"],
            "schedule": None,
            "start_date": "2026-01-01",
            "catchup": False,
        },
        "orders": {
            "workloads": ["orders"],
            "schedule": None,
            "start_date": "2026-01-01",
            "catchup": False,
        },
    }
    _write(domain_path, domain)

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=".",
        select=("domain:sales",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_SELECTION_CATALOG_INVALID"


def test_selected_preview_is_content_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.airflow_self_service_cmd import cmd_airflow_preview

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    args = Namespace(
        pipeline=".",
        format="json",
        select=["domain:sales"],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=500,
    )
    assert cmd_airflow_preview(args, ctx=object(), logger=logging.getLogger("test")) == 0
    first = json.loads(capsys.readouterr().out)
    assert cmd_airflow_preview(args, ctx=object(), logger=logging.getLogger("test")) == 0
    second = json.loads(capsys.readouterr().out)

    assert first["release"]["release_id"] == second["release"]["release_id"]
    assert first["deployment"]["deployment_id"] == second["deployment"]["deployment_id"]
    assert first["selection"]["selection_fingerprint"] == second["selection"]["selection_fingerprint"]


def test_equivalent_selected_artifacts_do_not_collide_in_immutable_release_storage(tmp_path: Path) -> None:
    from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService

    _project(tmp_path)
    service = ProjectSelectionPreviewService(root=tmp_path)
    first = service.preview(
        target=".",
        select=("source:mssql+",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )
    second = service.preview(
        target=".",
        select=("id:raw_orders+",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )

    assert first.passed is True
    assert second.passed is True
    assert first.details["release"]["release_id"] != second.details["release"]["release_id"]
    assert first.details["release"]["selection_fingerprint"] != second.details["release"]["selection_fingerprint"]


def test_selected_run_requires_bounded_temporary_sample(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands.run_cmd import cmd_run

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code = cmd_run(
        Namespace(
            path=".",
            selector=None,
            select=["domain:sales"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=10,
            sample=None,
            target=None,
            run_id=None,
            environment="development",
            format="json",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 4
    assert payload["errors"][0]["code"] == "DPONE_SELECTION_RUN_REQUIRES_SAFE_SAMPLE"


def test_selected_safe_sample_runs_in_id_order_and_stops_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands import run_cmd
    from dpone.commands.run_cmd import cmd_run
    from dpone.commands.run_safe_sample_cmd import SafeSampleCommandResult

    _project(tmp_path)
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[str, str]] = []

    def run_one(args: Namespace) -> SafeSampleCommandResult:
        calls.append((str(args.path), str(args.run_id)))
        payload: dict[str, object] = {
            "run_id": args.run_id,
            "passed": False,
            "result": {
                "status": "error",
                "errors": [{"code": "DPONE_TEST_FAILURE", "message": "bounded failure"}],
            },
            "safe_sample": {},
        }
        return SafeSampleCommandResult(1, payload, "failed\n", "# failed\n")

    monkeypatch.setattr(run_cmd, "build_safe_sample_result", run_one)
    code = cmd_run(
        Namespace(
            path=".",
            selector=None,
            select=["domain:sales"],
            exclude=[],
            state=None,
            selectors="selectors.yaml",
            max_selected=10,
            sample=1000,
            target="temporary",
            run_id="selected-test",
            environment="development",
            format="json",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert calls == [("pipelines/orders/pipeline.yaml", "selected-test-orders")]
    assert [item["workload_id"] for item in payload["results"]] == ["orders"]
    assert payload["unscheduled"] == ["raw_orders"]
    assert payload["selection"]["selection_fingerprint"].startswith("sha256:")


def test_safe_sample_process_selector_selects_the_requested_process(tmp_path: Path) -> None:
    from dpone.services.safe_sample_policy import SafeSamplePlanError, build_temporary_target_plan_from_file

    pipeline = _pipeline(
        "multi",
        domain="sales",
        owner="sales-platform",
        tags=[],
        source="mssql",
        sink="clickhouse",
    )
    second = json.loads(json.dumps(pipeline["processes"][0]))
    second["name"] = "second"
    second["sink"]["connection_ref"] = "second_sink"
    pipeline["processes"].append(second)
    path = _write(tmp_path / "pipelines" / "multi" / "pipeline.yaml", pipeline)

    with pytest.raises(SafeSamplePlanError) as ambiguous:
        build_temporary_target_plan_from_file(path, environment="development", run_id="run")
    assert ambiguous.value.code == "DPONE_PIPELINE_PROCESS_AMBIGUOUS"

    plan = build_temporary_target_plan_from_file(
        path,
        environment="development",
        run_id="run",
        process_selector="second",
    )
    assert plan.process == "second"
    assert plan.connection_ref == "second_sink"


def test_selection_public_schemas_match_runtime_contracts(tmp_path: Path) -> None:
    import jsonschema

    from dpone.gitops.schema_airflow_authoring_contracts import selection_schema_contracts

    contracts = {contract.kind: contract for contract in selection_schema_contracts()}
    graph = _graph()
    report = SelectionEngine().select(graph, SelectionRequest(select=("tag:finance+",)))
    state = SelectionState.build(
        SelectionStateNode(node.node_id, node.semantic_fingerprint, node.graph_fingerprint) for node in graph.nodes
    )
    payloads = {
        "dpone.selection-report.v1": report.to_jsonable(),
        "dpone.selection-state.v1": state.to_jsonable(),
    }
    schema_paths = {
        "dpone.selection-report.v1": Path("docs/schemas/gitops/selection-report.schema.json"),
        "dpone.selection-state.v1": Path("docs/schemas/gitops/selection-state.schema.json"),
    }
    for kind, payload in payloads.items():
        documented = json.loads(schema_paths[kind].read_text(encoding="utf-8"))
        assert documented == contracts[kind].schema
        jsonschema.Draft202012Validator(documented).validate(payload)

    selectors = {
        "schema": "dpone.selectors.v1",
        "selectors": {
            "sales_changed": {
                "description": "Changed sales workloads and downstream dependants.",
                "select": ["domain:sales", "state:modified+"],
                "exclude": ["tag:deprecated"],
            }
        },
    }
    selector_schema = json.loads(Path("src/dpone/schema/selectors.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(selector_schema).validate(selectors)


def test_selection_state_digest_mismatch_fails_closed(tmp_path: Path) -> None:
    from dpone.readiness.project_selection import ProjectSelectionService

    _project(tmp_path)
    state_path = tmp_path / "selection-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema": "dpone.selection-state.v1",
                "state_fingerprint": "sha256:" + "0" * 64,
                "nodes": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SelectionError) as error:
        ProjectSelectionService(root=tmp_path).select(
            target=".",
            select=("state:modified",),
            state_path="selection-state.json",
        )
    assert error.value.code == "DPONE_SELECTION_STATE_INVALID"


def test_selection_state_rejects_non_sha256_node_fingerprints() -> None:
    from dpone.manifest.selection_models import parse_selection_state

    state = SelectionState.build((SelectionStateNode("orders", "not-a-digest", "sha256:" + "0" * 64),))
    with pytest.raises(SelectionError) as error:
        parse_selection_state(state.to_jsonable())
    assert error.value.code == "DPONE_SELECTION_STATE_INVALID"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update({"unknown": True}),
        lambda payload: payload["nodes"][0].update({"unknown": True}),
        lambda payload: payload.update({"nodes": payload["nodes"] * 1001}),
    ],
)
def test_selection_state_parser_rejects_unknown_fields_and_node_budget(mutate) -> None:
    from dpone.manifest.selection_models import parse_selection_state

    state = SelectionState.build(
        (SelectionStateNode("orders", "sha256:" + "1" * 64, "sha256:" + "2" * 64),)
    ).to_jsonable()
    mutate(state)

    with pytest.raises(SelectionError) as error:
        parse_selection_state(state)
    assert error.value.code == "DPONE_SELECTION_STATE_INVALID"


def test_airflow_provider_source_has_no_selector_or_authoring_dependency() -> None:
    provider_root = Path("packages/dpone-airflow-pack/src")
    forbidden = ("dpone.manifest.selection", "project_selection", "selectors.yaml", "selection-state.json")
    for source in provider_root.rglob("*.py"):
        text = source.read_text(encoding="utf-8")
        assert not any(value in text for value in forbidden), source
