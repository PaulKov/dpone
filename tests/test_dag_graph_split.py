from __future__ import annotations

from pathlib import Path

from dpone.dag.graph import DependencyGraph
from dpone.dag.loader import ConfigLoader
from dpone.dag.manager import DependencyManager
from dpone.dag.node_registry import NodeRegistry
from dpone.dag.process_config_parser import ETLProcessConfigParser
from dpone.dag.resolver import DependencyResolver
from dpone.dag.task_group_index import TaskGroupFileIndex
from dpone.dag.yaml_types import ProcessNode
from dpone.manifest.loader import ManifestLoaderRouter


def _node(tmp_path: Path, *, name: str, selector: str, task_group: str | None = None) -> ProcessNode:
    cfg = {
        "name": name,
        "source": {
            "type": "postgres",
            "connection_id": "src",
            "table": {"schema": "public", "name": name},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "sink",
            "table": {"schema": "landing__demo__db", "name": f"public__{name}"},
            "strategy": {"mode": "full_refresh"},
        },
    }
    process = ETLProcessConfigParser().parse(cfg, base_path=tmp_path, metadata_only=True)
    process.task_group = task_group
    return ProcessNode(
        config=process,
        config_path=tmp_path / "demo.yaml",
        selector=selector,
        dependencies=process.dependencies.copy(),
        task_group=task_group,
    )


def test_node_registry_indexes_by_name_path_ref_and_group(tmp_path: Path) -> None:
    registry = NodeRegistry()
    users = _node(tmp_path, name="users", selector="public.users", task_group="core")
    registry.add(users)

    assert registry.get("users") is users
    assert registry.get_by_path(tmp_path / "demo.yaml") == [users]
    assert registry.get_by_ref(tmp_path / "demo.yaml", "public.users") is users
    assert registry.find_by_selector(tmp_path / "demo.yaml", "public.users") is users
    assert registry.nodes_by_group["core"] == [users]


def test_dependency_graph_builds_selector_and_group_edges(tmp_path: Path) -> None:
    graph = DependencyGraph(resolver=DependencyResolver(tmp_path))

    upstream_cfg = ETLProcessConfigParser().parse(
        {
            "name": "public_users",
            "source": {"type": "postgres", "connection_id": "src", "table": {"schema": "public", "name": "users"}},
            "sink": {
                "type": "bigquery",
                "connection_id": "sink",
                "table": {"schema": "landing__demo__db", "name": "public__users"},
                "strategy": {"mode": "full_refresh"},
            },
        },
        base_path=tmp_path,
        metadata_only=True,
    )
    downstream_cfg = ETLProcessConfigParser().parse(
        {
            "name": "public_orders",
            "source": {"type": "postgres", "connection_id": "src", "table": {"schema": "public", "name": "orders"}},
            "sink": {
                "type": "bigquery",
                "connection_id": "sink",
                "table": {"schema": "landing__demo__db", "name": "public__orders"},
                "strategy": {"mode": "full_refresh"},
            },
            "depends_on": [{"path": "#public.users"}, {"group": "up_group"}],
        },
        base_path=tmp_path,
        metadata_only=True,
    )

    upstream = ProcessNode(
        config=upstream_cfg,
        config_path=tmp_path / "batch.yaml",
        selector="public.users",
        dependencies=upstream_cfg.dependencies.copy(),
        task_group="up_group",
    )
    downstream = ProcessNode(
        config=downstream_cfg,
        config_path=tmp_path / "batch.yaml",
        selector="public.orders",
        dependencies=downstream_cfg.dependencies.copy(),
    )

    graph.add_node(upstream)
    graph.add_node(downstream)
    graph.build_relationships()

    assert downstream in upstream.dependents
    assert downstream.upstream is upstream


def test_dependency_manager_uses_task_group_index_and_chain_loader(tmp_path: Path) -> None:
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    (manifests / "root.batch.yaml").write_text(
        """
kind: dpone.batch.v1
vars:
  layer: landing
  src_system: demo
  src_database: db
naming:
  sink_dataset: "{{ layer }}__{{ src_system }}__{{ src_database }}"
  sink_table: "{{ src_schema }}__{{ src_table }}"
  process_name: "{{ src_schema }}_{{ src_table }}"
  task_group: "demo__db"
defaults:
  source:
    type: postgres
    connection_id: src
  sink:
    type: bigquery
    connection_id: sink
    strategy:
      mode: full_refresh
schemas:
  public:
    tables:
      - users
      - table: orders
        depends_on:
          - path: "#public.users"
""".strip(),
        encoding="utf-8",
    )

    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manager = DependencyManager(manifests, loader=loader)
    root = manager.load_process_chain(manifests / "root.batch.yaml")

    assert root.name == "public_users"
    assert any(node.name == "public_orders" for node in manager.get_execution_plan())
    assert manager.processed_paths == {manifests / "root.batch.yaml"}
    assert isinstance(manager.task_group_index, TaskGroupFileIndex)
