from __future__ import annotations

from pathlib import Path

from dpone.dag.edge_explain import build_edge_context, explain_direct_edge
from dpone.dag.edge_resolver import build_adjacency, build_dependency_indexes, resolve_declared_dependency
from dpone.dag.graph import DependencyGraph
from dpone.dag.process_config_parser import ETLProcessConfigParser
from dpone.dag.resolver import DependencyResolver
from dpone.dag.yaml_types import ProcessNode

PARSER = ETLProcessConfigParser()


def _node(
    tmp_path: Path,
    *,
    name: str,
    selector: str,
    task_group: str | None = None,
    depends_on: list[dict[str, str]] | None = None,
) -> ProcessNode:
    cfg = {
        "name": name,
        "source": {
            "type": "postgres",
            "connection_id": "src",
            "table": {"schema": "public", "name": selector.split(".")[-1]},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "sink",
            "table": {"schema": "landing__demo__db", "name": selector.replace(".", "__")},
            "strategy": {"mode": "full_refresh"},
        },
    }
    if depends_on:
        cfg["depends_on"] = depends_on

    process = PARSER.parse(cfg, base_path=tmp_path, metadata_only=True)
    process.task_group = task_group
    return ProcessNode(
        config=process,
        config_path=tmp_path / "demo.batch.yaml",
        selector=selector,
        dependencies=process.dependencies.copy(),
        task_group=task_group,
    )


def test_group_dependency_expands_to_whole_downstream_group(tmp_path: Path) -> None:
    up_a = _node(tmp_path, name="up_a", selector="public.up_a", task_group="up_group")
    up_b = _node(tmp_path, name="up_b", selector="public.up_b", task_group="up_group")
    down_trigger = _node(
        tmp_path,
        name="down_trigger",
        selector="public.down_trigger",
        task_group="down_group",
        depends_on=[{"group": "up_group"}],
    )
    down_peer = _node(tmp_path, name="down_peer", selector="public.down_peer", task_group="down_group")

    nodes = [up_a, up_b, down_trigger, down_peer]

    adjacency = build_adjacency(nodes)
    assert adjacency["up_a"] == {"down_trigger", "down_peer"}
    assert adjacency["up_b"] == {"down_trigger", "down_peer"}

    graph = DependencyGraph(resolver=DependencyResolver(tmp_path))
    for node in nodes:
        graph.add_node(node)
    graph.build_relationships()

    assert down_trigger in up_a.dependents
    assert down_peer in up_a.dependents
    assert down_trigger in up_b.dependents
    assert down_peer in up_b.dependents

    ctx = build_edge_context(nodes)
    exp = explain_direct_edge(ctx, upstream_name="up_a", downstream_name="down_peer")
    assert exp.direct_edge is True
    assert {reason.kind for reason in exp.reasons} == {"group_to_group"}


def test_group_string_path_only_applies_to_declaring_task(tmp_path: Path) -> None:
    up_a = _node(tmp_path, name="up_a", selector="public.up_a", task_group="up_group")
    up_b = _node(tmp_path, name="up_b", selector="public.up_b", task_group="up_group")
    down_declaring = _node(
        tmp_path,
        name="down_declaring",
        selector="public.down_declaring",
        task_group="down_group",
        depends_on=[{"path": "up_group"}],
    )
    down_peer = _node(tmp_path, name="down_peer", selector="public.down_peer", task_group="down_group")

    nodes = [up_a, up_b, down_declaring, down_peer]

    adjacency = build_adjacency(nodes)
    assert adjacency["up_a"] == {"down_declaring"}
    assert adjacency["up_b"] == {"down_declaring"}
    assert "down_peer" not in adjacency["up_a"]
    assert "down_peer" not in adjacency["up_b"]

    ctx = build_edge_context(nodes)
    exp = explain_direct_edge(ctx, upstream_name="up_a", downstream_name="down_declaring")
    assert exp.direct_edge is True
    assert {reason.kind for reason in exp.reasons} == {"depends_on_group_string"}


def test_resolve_declared_dependency_uses_same_modes_as_explain(tmp_path: Path) -> None:
    upstream = _node(tmp_path, name="users_task", selector="public.users")
    downstream = _node(
        tmp_path,
        name="orders_task",
        selector="public.orders",
        depends_on=[{"path": "#public.users"}],
    )
    nodes = [upstream, downstream]
    indexes = build_dependency_indexes(nodes)

    resolution = resolve_declared_dependency(
        downstream,
        downstream.dependencies[0],
        dep_index=0,
        indexes=indexes,
    )

    assert resolution.kind == "depends_on_selector"
    assert resolution.upstream_names == ("users_task",)
    assert resolution.downstream_names == ("orders_task",)
    assert resolution.detail["selector"] == "public.users"
